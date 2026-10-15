(ns jepsen.yugabyte.core
  "Integrates workloads, nemeses, and automation to construct test maps."
  (:require [clojure.tools.logging :refer :all]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [jepsen [checker   :as checker]
                    [generator :as gen]
                    [random    :as random]
                    [role      :as role]
                    [sql       :as sql]
                    [tests     :as tests]
                    [util      :refer [map-vals]]]
            [jepsen.os.debian :as debian]
            [jepsen.os.centos :as centos]
            [jepsen.yugabyte [append          :as append]
                             [bank            :as bank]
                             [bank-improved   :as bank-improved]
                             [counter         :as counter]
                             [db              :as db]
                             [g2              :as g2]
                             [jsql            :as jsql]
                             [long-fork       :as long-fork]
                             [monotonic       :as monotonic]
                             [multi-key-acid  :as multi-key-acid]
                             [nemesis         :as nemesis]
                             [recovery        :as recovery]
                             [set             :as set]
                             [single-key-acid :as single-key-acid]
                             [types           :as types]
                             [rw              :as rw]]
            [jepsen.yugabyte.ycql [bank            :as ycql.bank]
                                  [counter         :as ycql.counter]
                                  [long-fork       :as ycql.long-fork]
                                  [monotonic       :as ycql.monotonic]
                                  [multi-key-acid  :as ycql.multi-key-acid]
                                  [set             :as ycql.set]
                                  [single-key-acid :as ycql.single-key-acid]
                                  [types           :as ycql.types]]
            [jepsen.yugabyte.ysql [append          :as ysql.append]
                                  [append-table    :as ysql.append-table]
                                  [bank            :as ysql.bank]
                                  [bank-improved   :as ysql.bank-improved]
                                  [client          :as ysql.client]
                                  [counter         :as ysql.counter]
                                  [g2              :as ysql.g2]
                                  [long-fork       :as ysql.long-fork]
                                  [monotonic       :as ysql.monotonic]
                                  [multi-key-acid  :as ysql.multi-key-acid]
                                  [set             :as ysql.set]
                                  [single-key-acid :as ysql.single-key-acid]
                                  [types           :as ysql.types]
                                  [rw              :as ysql.rw]])
  (:import (jepsen.client Client)))

(def version-regex #"(?<=yugabyte\-)(\d+\.\d+(\.\d+){0,2}(-b\d+)?)")

(defmacro with-client
  [workload client-ctor]
  "Wraps a workload function to add :client entry to the result.
  Made as macro to re-evaluate client on every invocation."
  `(fn [~'opts] (assoc (~workload ~'opts) :client ~client-ctor)))

(defn workloads-builder
  "Many of our tests have a shared core workload which can be run with multiple
  clients. This takes a group name (e.g. :ysql) then a flat vector of
  [workload-name, workload-fn, client-fn] triples, and returns a map in which
  each name maps to a workload function which calls the given workload
  function, then associates (client-fn)."
  [group-name triples]
  (assert (zero? (mod (count triples) 3)))
  (reduce (fn [workload-map [workload-name workload-fn client-fn]]
            (assoc workload-map
                   (keyword (name group-name) (name workload-name))
                   (fn add-client [opts]
                     (assoc (workload-fn opts) :client (client-fn)))))
          {}
          (partition 3 triples)))

(def workloads-ycql
  "A map of workload names to functions that can take option maps and construct workloads."
  (workloads-builder
    :ycql
    [; YCQL can't do reads or conditional writes in transactions, so we have to
     ; allow negative balances.
     :bank            bank/workload-allow-neg  ycql.bank/->Client
     ; YCQL doesn't do reads in transactions, which we would need in order to
     ; delete an account
     :bank-improved   bank-improved/workload-sans-deletes
                      ycql.bank/->Client
     :counter         counter/workload         ycql.counter/->Client
     :long-fork       long-fork/workload       ycql.long-fork/->Client
     :monotonic       monotonic/workload       ycql.monotonic/->Client
     :multi-key-acid  multi-key-acid/workload  ycql.multi-key-acid/->Client
     :set             set/workload             ycql.set/->Client
     :set-index       set/workload             ycql.set/->IndexClient
     :single-key-acid single-key-acid/workload ycql.single-key-acid/->Client
     :types           types/workload           ycql.types/->Client]))

(def workloads-ysql
  "A map of workload names to functions that can take option maps and construct workloads."
  (workloads-builder
    :ysql
    [:append          append/workload          ysql.append/->Client
     :append-table    append/workload          ysql.append-table/->Client
     :bank            bank/workload-allow-neg  (partial ysql.bank/->Client true)
     :bank-multitable bank/workload-allow-neg  (partial ysql.bank/->MultiClient true)
     :bank-improved   bank-improved/workload   ysql.bank-improved/->Client
     :counter         counter/workload         ysql.counter/->Client
     :g2              g2/workload              ysql.g2/->Client
     :long-fork       long-fork/workload       ysql.long-fork/->Client
     :monotonic       monotonic/workload       ysql.monotonic/->Client
     :multi-key-acid  multi-key-acid/workload  ysql.multi-key-acid/->Client
     :set             set/workload             (partial ysql.set/->Client false)
     :set-index       set/workload             (partial ysql.set/->Client true)
     :single-key-acid single-key-acid/workload ysql.single-key-acid/->Client
     :types           types/workload           ysql.types/->Client
     :rw              rw/workload              ysql.rw/->Client]))

(def all-workloads
  "All workloads: a map of keywords to workload-constructing functions."
  (merge workloads-ycql
         workloads-ysql
         jsql/workloads))

(defn combos
  "Takes a map of options to collections of values for that option. Computes a
  collection of maps with the combinatorial expansion of every possible option
  value."
  ([opts]
   (combos {} opts))
  ([m opts]
   (if (seq opts)
     (let [[k vs] (first opts)]
       (mapcat (fn [v]
                 (combos (assoc m k v) (next opts)))
               vs))
     (list m))))

(def suggested-opts
  "A map of workload names to a collection of suggested options for that
  workload."
  (let [; While Read Uncommitted is present in YB, it maps to Read Committed,
        ; so we don't bother testing it separately.
        ru+ {:isolation [:read-uncommitted
                         :read-committed
                         :repeatable-read
                         :serializable]}
        rc+ {:isolation [:read-committed :repeatable-read :serializable]}
        si+ {:isolation [:repeatable-read :serializable]}
        s   {:isolation [:serializable]}]
    ; I'm writing (combos ...) explicitly here because it seems likely we're
    ; going to add some options that you *don't* want to take the cartesian
    ; product of, for specific workloads
    {:jsql/append          (combos rc+)
     :jsql/default-value   (combos (merge rc+
                                          {:locking-table [false true]}))
     :jsql/internal        (combos si+)
     :jsql/internal-sim    (combos si+)
     :jsql/rw              (combos rc+)
     :ysql/append          (combos
                             (merge rc+
                                    {:geo-partition [false true]
                                     :locking [:optimistic :pessimistic]}))
     :ysql/append-table    (combos (merge rc+
                                          {:locking-table [false true]}))
     :ysql/bank            (combos si+)
     :ysql/bank-improved   (combos si+)
     :ysql/bank-multitable (combos si+)
     :ysql/counter         (combos rc+)
     :ysql/g2              (combos s)
     :ysql/long-fork       (combos si+)
     :ysql/monotonic       (combos rc+)
     :ysql/multi-key-acid  (combos rc+)
     :ysql/set             (combos rc+)
     :ysql/set-index       (combos rc+)
     :ysql/single-key-acid (combos rc+)
     :ysql/types           (combos s)
     :ysql/rw              (combos rc+)}))

(def nemesis-specs
  "These are the types of failures that the nemesis can perform."
  #{:partition
    :partition-master
    :partition-tserver
    :kill
    :kill-master
    :kill-tserver
    :pause
    :pause-master
    :pause-tserver
    :clock
    :clock-master
    :clock-tserver
    :packet
    :packet-master
    :packet-tserver
    :bitflip-file-chunks
    :bitflip-file-chunks-master
    :bitflip-file-chunks-tserver
    :copy-file-chunks
    :copy-file-chunks-master
    :copy-file-chunks-tserver
    :truncate-file
    :truncate-file-master
    :truncate-file-tserver
    :snapshot-file-chunks
    :snapshot-file-chunks-master
    :snapshot-file-chunks-tserver
    })

(def all-nemeses
  "All nemesis specs to run as a part of a complete test suite."
  [#{}               ; No faults
   #{:kill-tserver}  ; Just tserver
   #{:kill-master}   ; Just master
   #{:pause-tserver} ; Just pause tserver
   #{:pause-master}  ; Just pause master
   #{:clock-skew}    ; Just clocks
   #{:partition}     ; Just partitions
   #{:kill-tserver
     :kill-master
     :pause-tserver
     :pause-master
     :clock
     :partition
     :packet}
   ; Plus file corruption
   #{:kill-tserver
     :kill-master
     :pause-tserver
     :pause-master
     :clock
     :partition
     :packet
     :copy-file-chunks
     :bitflip-file-chunks
     :truncate-file
     :snapshot-file-chunks}])

(defn yugabyte-ssh-defaults
  "A partial test map with SSH options for a test running in Yugabyte's
  internal testing environment."
  []
  {:ssh {:port                     54422
         :strict-host-key-checking false
         :username                 "yugabyte"
         :private-key-path (str (System/getenv "HOME")
                                "/.yugabyte/yugabyte-dev-aws-keypair.pem")}})

(def trace-logging
  "Logging configuration for the test which sets up traces for queries."
  {:logging {:overrides {;"com.datastax"                            :trace
                         ;"com.yugabyte"                            :trace
                         "com.datastax.driver.core.RequestHandler" :trace
                         ;"com.datastax.driver.core.CodecRegistry"  :info
                         }}})

(defn roles
  "Computes a roles map for CLI options. The last replication-factor nodes are
  masters; the others are tservers."
  [opts]
  (let [nodes (:nodes opts)
        rf (:replication-factor opts)
        _ (assert (< rf (count nodes))
                  (str "We need at least " (* 2 rf)
                       " nodes for " rf " masters and " rf
                       " tservers, but test only has " (count nodes)
                       " nodes: " (pr-str nodes)))
        ; Why reverse? This is a deeply silly hack: the jsql workloads assume
        ; that the primary node of the test is the writable one. Ideally I'd go
        ; make that pluggable, but I have no time. :(
        [masters tservers] (split-at rf (reverse nodes))]
    {:master  (vec (sort masters))
     :tserver (vec (sort tservers))}))

(defn test-1
  "Initial test construction from a map of CLI options. Establishes the test
  name, OS, DB."
  [opts]
  (let [api (case (namespace (:workload opts))
              "ysql" :ysql
              "ycql" :ycql
              ; The jepsen.sql tests use the ysql API
              "jsql" :ysql)
        url-version (when-let [url (:url opts)]
                      (first (re-find version-regex url)))]
    (when (and (= :ycql api) (:connection-manager opts))
      (warn "Connection manager is a YSQL-only feature; disabling it for YCQL workload"
            (:workload opts)))
    (assoc opts
      :api api
      ; Connection manager only applies to YSQL.
      :connection-manager (and (not= :ycql api) (:connection-manager opts))
      :db (db/db)

      :expected-consistency-model
      (or (:expected-consistency-model opts)
          (case (:isolation opts)
            ; The existing tests add realtime edges to the graph, so we'll
            ; expect the strong variants of each isolation level. YB
            ; "Repeatable Read" is actually SI, and I suspect Strong SI. The
            ; docs don't claim Strong variants directly, but we verified this
            ; in previous Jepsen tests and it seems to pass here!
            :read-uncommitted :strong-read-committed
            :read-committed   :strong-read-committed
            :repeatable-read  :strong-snapshot-isolation
            :serializable     :strong-serializable))

      :name (str (-> (or (:url opts) (:version opts))
                     (str/split #"/")
                     (last))
                 " " (namespace (:workload opts))
                 " " (name (:workload opts))
                 ; Isolation doesn't apply to ycql
                 (when (not= :ycql api)
                   (str " " (name (:isolation opts))))
                 (when (:geo-partition opts) " geo")
                 (when (:table-locks opts) " table-locks")
                 (when (seq (:nemesis opts))
                   (str " nemesis "
                        (->> (:nemesis opts)
                             sort
                             (map name)
                             (str/join ","))))
                 (when-let [cft (:corrupt-file-type opts)]
                   (str " cft=" (name cft))))
      :os (case (:os opts)
            :centos centos/os
            :debian debian/os)

      :version (or url-version (:version opts))
      :roles (roles opts)
      )))

(defn test-2
  "Second phase of test construction. Builds the workload and nemesis, and
  finalizes the test."
  [opts]
  (let [workload ((get all-workloads (:workload opts)) opts)
        nemesis (nemesis/package
                  {:db       (:db opts)
                   :roles    (:roles opts)
                   :nodes    (:nodes opts)
                   :interval (:nemesis-interval opts)
                   :stable-period (:nemesis-stable-period opts)
                   :faults   (:nemesis opts)
                   :partition {:targets [:one :majority :majorities-ring]}
                   :packet {:targets [:one :minority :all]
                            :behaviors [{:delay {:time "100ms"
                                                 :jitter "50ms"}}]}
                   :pause    {:targets [:one :minority :majority]}
                   :kill     {:targets [:one :minority :majority :all]}})
        gen (:generator workload)
        gen (if-let [r (:rate opts)]
              (gen/stagger (/ r) gen)
              gen)
        gen (->> gen
                 gen/relaxed-reconnect
                 (gen/nemesis
                   (gen/phases
                     ; Give ourselves a little bit before killing processes
                     (gen/sleep 5)
                     (:generator nemesis)))
                 (gen/time-limit (:time-limit opts)))
        gen (if-let [final (:final-generator workload)]
              (gen/phases gen
                          (gen/log "Healing cluster")
                          (gen/nemesis (:final-generator nemesis))
                          (recovery/generator opts)
                          (gen/clients final))
              gen)
        gen (if-let [wrap (:wrap-generator workload)]
              (wrap gen)
              gen)
        checker (checker/compose {:perf                 (checker/perf)
                                  :stats                (checker/stats)
                                  :unhandled-exceptions (checker/unhandled-exceptions)
                                  :clock                (checker/clock-plot)
                                  :workload             (:checker workload)})]
    (merge tests/noop-test
           opts
           (dissoc workload
                   :generator
                   :final-generator
                   :wrap-generator
                   :checker)
           (when (:yugabyte-ssh opts) (yugabyte-ssh-defaults))
           (when (:trace-cql opts) (trace-logging))
           {:client          ; Only tservers run the client-facing API
                             (role/restrict-client
                               :tserver (:client workload))
            :concurrency     (or (:concurrency opts)
                                 (:concurrency workload)
                                 (* 2 (count (:tserver (:roles opts)))))
            :nemesis         (recovery/nemesis
                               (:nemesis nemesis))
            :plot            {:nemeses (:perf nemesis)}
            :generator       gen
            :checker         checker})))

(defn test-3
  "Final phase where we define global cluster configuration parameters"
  [opts]
  (let [; TODO: <sigh> not sure why they're doing random choices here; I assume
        ; these should be CLI parameters.
        packed-columns-enabled (random/bool)
        colocated (and (not (:geo-partition opts)) (random/bool))]
    (assoc opts
           :yb-packed-columns-enabled packed-columns-enabled
           :yb-colocated colocated)))

(defn yb-test
  "Constructs a yugabyte test from CLI options."
  [opts]
  (-> opts test-1 test-2 test-3))
