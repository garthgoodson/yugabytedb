(ns jepsen.yugabyte.nemesis
  "Fault injection"
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]
                    [random :as rand]
                    [role :as role]
                    [util :as util]]
            [jepsen.control [util :as cu]]
            [jepsen.nemesis [combined :as nc]
                            [file :as nf]]
            [jepsen.yugabyte.db.core :refer [data-dir]]))

(defrecord PackageGen [; The time period, in nanos, we run a given set of
                       ; packages
                       ^long period
                       ; The deadline when we'll rotate to new active packages
                       ^long deadline
                       ; A vector of collections of fs, one for each package
                       fs
                       ; A vector of integers, each an active package index
                       active
                       ; A vector of generators, one for each package
                       gens
                       ; A vector of final generators, one for each package
                       finals]
  gen/Generator
  (op [this test ctx]
    ; Lazily initialize deadline
    (let [deadline (if (= -1 deadline)
                     (+ (:time ctx) period)
                     deadline)]
      (if (< deadline (:time ctx))
        ; Pick new active packages
        (let [active' (if (rand/bool 1/4)
                        []
                        (vec (util/random-nonempty-subset
                               (range (count gens)))))]
          (gen/op
            [; Run final generators for all active packages
             (map finals active)
             {:type :log, :value (str "Switching to new mix of nemeses: "
                                      (pr-str (mapv fs active')))}
             (PackageGen. period (+ deadline period)
                          fs active' gens finals)]
            test ctx))
        ; Fan out to each active package, ask each for an op, and pick the
        ; soonest
        (when-let [{:keys [op gen' i]}
                   (->> active
                        (map (fn [i]
                               (when-let [[op gen'] (gen/op (gens i) test ctx)]
                                 {:op   op
                                  :gen' gen'
                                  :i    i})))
                        (reduce gen/soonest-op-map nil))]
          [op
           (PackageGen. period deadline fs active
                        (assoc gens i gen')
                        finals)]))))

  (update [this test ctx op]
    ; Propagate update to all generators.
    (PackageGen. period deadline fs active
                 (mapv (fn [gen]
                         (gen/update gen test ctx op))
                       gens)
                 finals)))

(defn package-gen
  "For long-running tests, it's nice to be able to have periods of no faults,
  periods with lots of faults, just one kind of fault, etc. This takes a time
  period in seconds, which is how long to emit nemesis operations for a
  particular subset of packages. Takes a collection of packages. Constructs a
  nemesis generator which emits faults for a shifting collection of packages
  over time."
  [period packages]
  (PackageGen.
    (util/secs->nanos period)
    -1
    (mapv (comp n/fs :nemesis) packages)
    (vec (util/random-nonempty-subset (range (count packages))))
    (mapv :generator packages)
    (mapv :final-generator packages)))

;; File corruption

(def file-types
  "A map of file types (e.g. :wal) to regexes that identify those files. I'm
  just guessing at a few of these that might be useful, for now. YB has a large
  assortment of disk files, and also mixes config and log files in, so I'm not
  sure what exactly we should be corrupting."
  {:all             #""
   :db              #"\.db$"
   :wal             #"/wal-\d+$"
   :index           #"/index\.\d+$"
   :table           #"/table-[a-z0-9]+$"
   :tablet          #"/tablet-[a-z0-9]+$"
   :rocks-log       #"/rocksdb/.+\.log$"
   :rocks-manifest  #"/rocksdb/.+/MANIFEST-\d+$"
   :rocks-tablet    #"/rocksdb/.+/tablet-"
   :rocks-snapshot  #"/rocksdb/.+-snapshots$"})

(defn log-file?
  "Is a file a log file? YB mixes its logs in with the data dirs."
  [file]
  (or (re-find #"/tserver/logs/" file)
      (re-find #"/master/logs/" file)))

(defn rand-data-file
  "Picks a random data file on the given node. Uses (:corrupt-file-types test)
  to pick only some types of files."
  [test node]
  (c/with-node test node
    (c/su
      (->> (cu/ls data-dir
                  {:recursive? true
                   :full-path? true
                   :types [:file]})
           (remove log-file?)
           (filter (if-let [t (:corrupt-file-type test)]
                     (partial re-find (file-types t))
                     (constantly true)))
           vec
           rand/nth-empty))))

(defn corrupt-file-expand-corruption
  "Fills in the parameters for a single file corruption operation's corruption
  map."
  [test f {:keys [node] :as corruption}]
  (when-let [file (rand-data-file test node)]
    (when-let [size (try+ (-> (c/with-node test node
                                (c/su (c/exec :stat "-c" "%s" file)))
                                (parse-long))
                          (catch [:type :jepsen.control/nonzero-exit] _))]
      (when (pos? size)
        (let [corruption (assoc corruption :file file)]
          (case f
            :bitflip-file-chunks
            (assoc corruption
                   ; Roughly two errors per file
                   :probability (/ 2 8 size))

            :copy-file-chunks
            (assoc corruption
                   ; Let's say, I don't know, a sixteenth of the 4k chunks get
                   ; shuffled
                   :probability 1/16)

            :snapshot-file-chunks
            (assoc corruption :probability 0.5)

            :restore-file-chunks
            (assoc corruption :probability 0.5)

            ; We want a blend of truncations--most just clipping a few bytes
            ; off the end, but occasionally taking out large chunks of the
            ; file.
            :truncate-file
            (assoc corruption :size (- (rand/zipf size)))))))))

(defn corrupt-file-expand-value
  "Fills in parameters for a file corruption operation."
  [{:keys [f value] :as op} test ctx]
  (update op :value
          (partial keep (partial corrupt-file-expand-corruption test f))))

(defn corrupt-file-package
  "Nemesis package for file corruption, based on (:corrupt-file-types test) and
  :faults containing some of :bitflip-file-chunks, :snapshot-file-chunks, and
  :truncate-file."
  [{:keys [faults interval] :as opts}]
  (info :faults faults :interval interval)
  (let [; What faults can we inject?
        faults (set/intersection faults
                                 #{:bitflip-file-chunks
                                   :copy-file-chunks
                                   :snapshot-file-chunks
                                   :truncate-file})
        needed? (seq faults)
        ; Generator of faults (just :f, no values; we fill those in later)
        gen (gen/mix
              (mapv {:bitflip-file-chunks
                     (gen/repeat {:type :info, :f :bitflip-file-chunks})

                     :copy-file-chunks
                     (gen/repeat {:type :info, :f :copy-file-chunks})

                     :snapshot-file-chunks
                     (gen/flip-flop
                       (gen/repeat {:type :info, :f :snapshot-file-chunks})
                       (gen/repeat {:type :info, :f :restore-file-chunks}))

                     :truncate-file
                     (gen/repeat {:type :info, :f :truncate-file})}
                    faults))
        ; Target a minority of nodes.
        gen (nf/nodes-gen (fn [test] (util/minority (count (:nodes test))))
                          gen)
        ; Expand values into specific files and probabilities etc
        gen (gen/map corrupt-file-expand-value gen)
        ; And slow down
        gen (gen/stagger interval gen)]
    {:nemesis (nf/corrupt-file-nemesis
                {; Let's guess 4K chunks for now
                 :chunk-size (* 1024 4)})
     :generator (when needed? gen)
     :perf #{{:name "corrupt-file"
              :fs #{:bitflip-file-chunks
                    :copy-file-chunks
                    :snapshot-file-chunks
                    :restore-file-chunks
                    :truncate-file}
              :start  #{}
              :stop   #{}
              :color  "#D2E9A0"}}}))

;; Roles

(defn role-faults
  "Takes a set of faults like #{:pause-master} and a role name like :master.
  Returns a set of faults for that specific role, like #{:pause}.

  General faults, like :pause or :kill, apply to every role."
  [faults role]
  (->> faults
       (keep (fn [fault]
               (if-let [[m fault role']
                        (re-find #"^(\w+)-(master|tserver)$"
                                 (name fault))]
                 ; This is a role-specific fault
                 (when (= (name role) role')
                   (keyword fault))
                 ; A general fault always applies
                 fault)))
       set))

(defn update-role-faults
  "Takes CLI options and updates :faults to be specific to the given role."
  [role opts]
  (update opts :faults role-faults role))

(defn nemesis-packages
  "Takes a nemesis options maps and returns a seq of nemesis packages"
  [opts]
  (concat (nc/nemesis-packages opts)
          [(corrupt-file-package opts)]))

(defn package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        roles (:roles opts)
        _ (assert (map? roles))
        packages
        (->> (concat
               ; Unfurl each role into a collection of nemesis packages for
               ; that particular role.
               (mapcat (fn [[role nodes]]
                         (->> opts
                              (update-role-faults role)
                              nemesis-packages
                              (map (partial role/restrict-nemesis-package role))))
                       roles)
               ; Custom packages across all roles
               [])
             (filter :generator))
        nsp (:stable-period opts)]
    ;(info :packages (map (comp n/fs :nemesis) packages))
    (cond-> (nc/compose-packages packages)
      nsp (assoc :generator (package-gen nsp packages)))))
