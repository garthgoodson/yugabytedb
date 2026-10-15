(ns jepsen.yugabyte.db.core
  "Core functions used by both master and tablet server."
    (:require [clojure [pprint :refer [pprint]]
              [string :as str]]
             [clojure.tools.logging :refer :all]
             [clojure.data.json :as json]
             [jepsen.control :as c]
             [jepsen.util :as util]
             [jepsen.random :as random]
             [jepsen.control.net :as cn]
             [jepsen.control.util :as cu]
             [jepsen.os.debian :as debian]
             [jepsen.yugabyte [util :refer [parse-version]]]
             [version-clj.core :as v]
             [jepsen.yugabyte.ycql.client :as ycql.client]
             [jepsen.yugabyte.ysql.client :as ysql.client :refer [ysql-port]]
             [slingshot.slingshot :refer [try+ throw+]]))

(def dir
  "Where we unpack the Yugabyte package"
  "/home/yugabyte")

(def data-dir (str dir "/data"))

(def tmp-dir "/tmp/jepsen/yugabyte")

(def installed-url-file
  "This file caches our version string, so we can skip re-downloading it each
  time."
  (str tmp-dir "/installed-url"))

(def share-tarball
  "This is... very silly.. YB's tarball is chonky, and unpacking it takes a
  while, so we don't want to untar it every single test. However, wiping the
  data/ directory is *not* enough to get a clean state! Why? Because on
  startup, YB *hardlinks* some SSTable files from share/ to data/. When we
  corrupt files in data/, we can wind up modifying the other link in share/ as
  well, which then leaks into later test runs.

  To work around this, we tar up just the share/ directory, which is about a
  fifteenth of the total directory volume, and cache it here. We then untar
  this every test run, to ensure we always start with a clean copy of
  share/."
  (str tmp-dir "/share.tar.gz"))

; Versions where things changed
(def minimal-packed-version "2.16.4.0-b1")
(def minimal-skip-prefix-locks-version
  "skip_prefix_locks gflag was introduced in 2026.1; older clusters fail to
  start when it is set."
  "2026.1.0.0-b0")

(defmacro suppress-interrupted-exception
  "When there's an error encountered on one of the node, the whole cluster
  worker thread group is interrupted (see dom-top.core/real-pmap-helper). This
  is likely to interrupt a bunch of waits and sleeps in SSH connection helpers,
  which would cause a lot of noise in the log. Same happens when the Ctrl+C is
  pressed.

  Since interruption only happens on error, we can safely suppress those
  InterruptedExceptions - execution as a whole will error out anyway."
  [& body]
  `(try+
     (do ~@body)
     (catch InterruptedException e#
       (info "Interrupted, probably an error happened on another node"))))

(defn master-addresses
  "Given a test, returns a list of master addresses, like \"10.0.0.1:7100,10.0.0.2:7100,...\""
  [test]
  (assert (coll? (:nodes test)))
  (->> (:master (:roles test))
       (take (:replication-factor test))
       (map #(str (cn/ip %) ":7100"))
       (str/join ",")))

(defn yb-admin
  "Runs a yb-admin command on a node. Args are passed to yb-admin."
  [test & args]
  (apply c/exec (str dir "/bin/yb-admin")
         :--master_addresses (master-addresses test)
         args))

(defn ysqlsh
  "Runs a ysqlsh command on a node. Args are passed to ysqlsh."
  [test node & args]
  (let [args (concat args
                     [:-h (cn/ip node)
                      :--port (if (:connection-manager test)
                                5431
                                5433)])]
    ;(info "/bin/ysqlsh" args)
    (apply c/exec (str dir "/bin/ysqlsh")
           args)))

(defn await-ysqlsh
  "Blocks until we can execute commands using ysqlsh."
  [test node]
  (util/await-fn (fn [] (ysqlsh test node :-c (str "SELECT TRUE;")))
                 {:retry-interval 500
                  :log-interval 10000
                  :log-message "Waiting for YSQL"
                  :timeout 120000}))

(defn await-api
  "Blocks until the API is ready on this node."
  [test node]
  (case (:api test)
    :ycql
    (ycql.client/await-setup node)

    :ysql
    (cu/await-tcp-port (cn/ip node) (ysql-port test) {})))

(defn version
  "Returns a map of version information by calling `bin/yb-master --version`,
  including:

  :version
  :build
  :revision
  :build-type
  :timestamp"
  []
  (try
    (-> #"version (.+?) build (.+?) revision (.+?) build_type (.+?) built at (.+)"
        (re-find (c/exec (str dir "/bin/yb-master") :--version))
        next
        (->> (zipmap [:version :build :revision :build-type :timestamp])))
    (catch RuntimeException e
      ; Probably not installed
      )))

(defn get-installed-url
  "Returns URL from which YugaByte was installed on node"
  []
  (try
    (c/exec :cat installed-url-file)
    (catch RuntimeException e
      ; Probably not installed
      )))

(defn download-url
  "Returns URL to tarball for a test."
  [test]
  (or (:url test)
      (let [{:keys [short full]} (parse-version (:version test))]
        (str "https://software.yugabyte.com/releases/" short
             "/yugabyte-" full "-linux-x86_64.tar.gz"))))

(defn log-files-without-symlinks
  "Takes a directory, and returns a map of logfiles in that directory to short
  names, skipping the symlinks which end in .INFO, .WARNING, etc."
  [^String dir]
  (->> (try (cu/ls dir {:full-path? true})
            (catch RuntimeException e nil))
       (remove (partial re-find #"\.(INFO|WARNING|ERROR)$"))
       ; Drop dir prefix
       (map (fn [file]
              [file (subs file (.length dir))]))
       (into {})))

;; Shared configuration flags

(defn basic-flags
  "Shared configuration flags for both master and tserver"
  [node]
  [; Data files!
   :--fs_data_dirs data-dir
   ; Limit memory to 2GB
   :--memory_limit_hard_bytes 2147483648
   ; Fewer shards to improve perf
   :--yb_num_shards_per_tserver 4
   ; YB can do weird things with loopback interfaces, so... bind explicitly
   :--rpc_bind_addresses (cn/ip node)
   ; Seconds before declaring an unavailable node dead and initiating a raft
   ; membership change
   ;:--follower_unavailable_considered_failed_sec 10
   ; Clock skew threshold
   ; :--max_clock_skew_usec 1
   ; Disable YugaByte call-home analytics
   :--callhome_enabled=false])

(defn packed-columns-flags
  "TODO: what is this?"
  [test]
  (if (and (v/newer-or-equal? (:version test) minimal-packed-version)
           (:yb-packed-columns-enabled test))
    [:--ysql_enable_packed_row]
    []))

(defn get-random-node-skew
  [max_skew node_ip]
  (random/long max_skew))

(def get-node-skew
  (memoize get-random-node-skew))

(defn random-clock-skew-flags
  "Enable random clock skew

  max-skew parameter is less than (490 / (tservers + master))
  as a result we should avoid random -500 skews in all masters e.g.

  half-skew is needed to generate negative skews"
  [test node]
  (if (:clock-skew-flags test)
    (let [max-skew (int (/ 490 (count (:nodes test))))
          host-skew (if (:extreme-skew test)
                      (get-random-node-skew max-skew (cn/ip node))
                      (get-node-skew max-skew (cn/ip node)))
          half-skew (int (/ max-skew 2))]
      [:--time_source (format "skewed,%s" (- host-skew half-skew))])
    []))

(defn geo-partitioning-flags
  "Geo partitioning specific mapping flags
  Each node will be mapped to id in [1 2] and then used in each node."
  [test node nodes]
  (if (:geo-partition test)
    (let [geo-ids (map #(+ 1 (mod % 2)) (range (count nodes)))
          geo-node-map (zipmap nodes geo-ids)
          node-id-int (get geo-node-map node)]
      (info node [:--placement_cloud :ybc
                  :--placement_region (str "jepsen-" node-id-int)
                  :--placement_zone (str "jepsen-" node-id-int "a")])
      [:--placement_cloud :ybc
       :--placement_region (str "jepsen-" node-id-int)
       :--placement_zone (str "jepsen-" node-id-int "a")])
    []))

(defn experimental-tuning-flags
  "Speed up recovery from partitions and crashes. Right now it looks like
  these actually make the cluster slower to, or unable to, recover."
  [test]
  (if (:experimental-tuning-flags test)
    [:--client_read_write_timeout_ms 2000
     :--leader_failure_max_missed_heartbeat_periods 2
     :--leader_failure_exp_backoff_max_delta_ms 5000
     :--rpc_default_keepalive_time_ms 5000
     :--rpc_connection_timeout_ms 1500]
    []))

(defn perf-flags
  "Common performance-related flags. YB basically falls over if you have any
  kind of contended workload, and most of these workloads exist specifically to
  test contention. I'm seeing routine 30-second latencies for small
  transactions and throughput of ~1 txn/sec. It's rooough."
  [test]
  [; Poll txn coordinators more often for the status of blocking
   ; transactions. I think this helps reduce latencies a bit?
   :--wait_queue_poll_interval_ms 10])

(defn stress-flags
  "Shared stress-test flags for master and tserver.
  Disabled flags are commented with the reason — re-enable after verifying startup."
  [test]
  (if (:stress-tuning test)
    [; WAL: 512KB segments — may be too small for catalog bootstrap
     ; :--log_segment_size_bytes 524288
     ;     :--consensus_max_batch_size_bytes 65536        ; 64KB — smaller replication batches
     :--bg_superblock_flush_interval_secs 5]
    []))


(defn parse-gflag
  "Parse a gflag spec 'flag_name=value' into [flag-name value], or
  'flag_name' into [flag-name nil] for boolean flags."
  [spec]
  (let [idx (.indexOf ^String spec (int \=))]
    (if (pos? idx)
      [(subs spec 0 idx) (subs spec (inc idx))]
      [spec nil])))

(defn pg-conf-flag?
  "Returns true if flag-name is a pg_conf-style flag whose values should be
  merged rather than overwritten."
  [flag-name]
  (str/includes? flag-name "ysql_pg_conf_csv"))

(defn merge-pg-conf-csv
  "Merge two pg_conf_csv value strings. Settings in `override` take precedence
  over those in `base`. Each string is a CSV of key=value pairs."
  [base override]
  (let [parse (fn [s]
                (when (seq s)
                  (into {}
                        (map (fn [pair]
                               (let [[k v] (str/split pair #"=" 2)]
                                 [k v]))
                             (str/split s #",")))))
        merged (merge (parse base) (parse override))]
    (str/join "," (map (fn [[k v]] (str k "=" v)) merged))))

(defn preview-flags-csv-flag?
  "Returns true if flag-name is (aphyr, 2026-08-13: Uh... that's not what this
  does) allowed_preview_flags_csv, whose value is (aphyr: what???) a plain CSV
  list of flag names that must be unioned rather than overwritten: gflags is
  last-wins, so two occurrences would silently drop earlier entries (e.g.
  enable_ysql_conn_mgr) and make YB reject the preview flag at startup."
  [flag-name]
  (str/includes? flag-name "allowed_preview_flags_csv"))

(defn merge-csv-list
  "Merge two plain CSV list strings into a deduplicated union, preserving the
  order of first appearance."
  [base override]
  (->> (concat (str/split (or base "") #",")
               (str/split (or override "") #","))
       (map str/trim)
       (remove empty?)
       distinct
       (str/join ",")))

(defn merge-fn-for
  "Returns the CSV merge function for a flag name (without the leading --), or
  nil for a regular flag. pg_conf merges key=value settings; preview-flag lists
  are unioned."
  [flag-name]
  (cond
    (pg-conf-flag? flag-name)           merge-pg-conf-csv
    (preview-flags-csv-flag? flag-name) merge-csv-list
    :else                               nil))

(defn collapse-csv-flags
  "Collapse repeated CSV flags (pg_conf, allowed_preview_flags) in a flattened
  flag vector into a single merged occurrence at the position of the first one,
  preserving the order of every other flag. Different parts of the flag list
  (e.g. connection manager and append-table) each emit their own
  allowed_preview_flags_csv; gflags is last-wins, so leaving the duplicates
  would silently drop entries and make YB reject the preview flag at startup."
  [flat]
  (loop [items (seq flat), out [], seen {}]
    (if-not items
      out
      (let [x        (first items)
            merge-fn (when (keyword? x) (merge-fn-for (subs (name x) 2)))]
        (if (and merge-fn (next items))
          (let [v (str (second items))]
            (if-let [i (get seen x)]
              (recur (nnext items)
                     (update out (inc i) #(merge-fn (str %) v))
                     seen)
              (recur (nnext items)
                     (conj out x v)
                     (assoc seen x (count out)))))
          (recur (next items) (conj out x) seen))))))

(defn apply-extra-gflags
  "Apply extra gflags to a flag vector built by start-master!/start-tserver!.
  The flag vector is first flattened and its CSV flags collapsed (so multiple
  features can each emit allowed_preview_flags_csv safely). Regular extra flags
  are then appended (YugaByteDB uses last-wins); extra CSV flags are merged into
  the existing occurrence instead of producing a duplicate."
  [flag-vec extra-specs]
  (let [flat (collapse-csv-flags (vec (flatten flag-vec)))]
    (reduce
      (fn [acc [flag-name value]]
        (let [kw       (keyword (str "--" flag-name))
              merge-fn (merge-fn-for flag-name)]
          (if (and value merge-fn)
            ;; CSV flag — find existing and merge, or append
            (let [idx (.indexOf acc kw)]
              (if (and (>= idx 0) (< (inc idx) (count acc)))
                (assoc acc (inc idx) (merge-fn (str (get acc (inc idx))) value))
                (conj acc kw value)))
            ;; Regular flag — append (last-wins). Render as a single
            ;; --flag=value token: `--flag value` is invalid for boolean
            ;; gflags (the value is left as a stray positional and YB aborts
            ;; with "Error parsing command-line flags"). Matches the
            ;; --skip_prefix_locks=false style used elsewhere.
            (if value
              (conj acc (keyword (str "--" flag-name "=" value)))
              (conj acc kw)))))
      flat
      (map parse-gflag extra-specs))))

(def limits-conf
  "Ulimits, in the format for /etc/security/limits.conf."
  "
  * hard nofile 1048576
  * soft nofile 1048576")

(defn install!
  "Installs YugabyteDB on a node."
  [test]
  (c/su
    (c/exec :mkdir :-p tmp-dir)
    (c/cd dir
          (let [url           (download-url test)
                installed-url (get-installed-url)]
            ; Installing deps and unpacking the full tarball takes a little
            ; bit, so we try to re-use an existing installation if possible.
            (if (= url installed-url)
              ; Fast path: we only need to replace share/, to make sure it
              ; doesn't have any corrupted hardlinks
              (do (info "Refreshing share/")
                  (c/exec :rm :-rf "share")
                  (c/exec :tar :xf share-tarball))
              ; Slow path: install the distribution from scratch
              (do (info "Replacing version" installed-url "with" url)
                  (debian/install ["python3"])
                  (assert (re-find #"Python 3"
                                   (c/exec :python3 :--version (c/lit "2>&1"))))

                  (info "Installing tarball into" dir)
                  (cu/install-archive! url dir)
                  (let [post-install-script-path "./bin/post_install.sh"]
                    (assert (cu/exists? post-install-script-path)
                            "Post-install script does not exist!")
                    (info "Post-install script")
                    (c/exec post-install-script-path)

                    (info "Saving share/")
                    (c/exec :tar :czf share-tarball "share/")

                    (c/exec :echo url :>> installed-url-file)
                    (info "Done with setup"))))))))

(defn configure!
  "Configures a node after installation."
  []
  ; YB will explode after creating just a handful of tables if we don't raise
  ; ulimits. This is sort of a hack; it won't take effect for the current
  ; session, but will on the second and subsequent runs. We can't run
  ; `ulimit` directly because the shell context doesn't carry over to
  ; subsequent commands. Should write a subshell exec thing to handle this at
  ; some point.
  (c/su (c/exec :echo limits-conf :> "/etc/security/limits.d/jepsen.conf")))

(defn create-geo-tablespace!
  [node tablespace-name replica-placement]
  (info "Creating tablespace" tablespace-name)
  (ysqlsh test node
          :-c (str "CREATE TABLESPACE " tablespace-name " "
                   "WITH (replica_placement='"
                   (json/write-str replica-placement) "');")))

(defn setup-geo-partition!
  [node tablespace-name]
  (create-geo-tablespace!
    node
    (str tablespace-name "_1a")
    {:num_replicas     2
     :placement_blocks [{:cloud             :ybc
                         :region            :jepsen-1
                         :zone              :jepsen-1a
                         :min_num_replicas  1
                         :leader_preference 1}]})
  (create-geo-tablespace!
    node
    (str tablespace-name "_2a")
    {:num_replicas     2
     :placement_blocks [{:cloud             :ybc
                         :region            :jepsen-2
                         :zone              :jepsen-2a
                         :min_num_replicas  1
                         :leader_preference 1}]}))

(defn setup-db!
  "Creates the YugaByte database and sets up geo-partitioning if necessary."
  [test node]
  (ysqlsh test node :-c (str "DROP DATABASE IF EXISTS "
                             ysql.client/dbname ";"))
  (ysqlsh test node :-c (str "DROP USER IF EXISTS " ysql.client/user "; "
                             "CREATE USER " ysql.client/user " createdb; "))
  (ysqlsh test node :-U ysql.client/user
          :-c (str "CREATE DATABASE " ysql.client/dbname
                   (when (:yb-colocated test)
                     " WITH colocated = true")
                   ";"))
  (when (:geo-partition test)
    (info "Setup optional geo partitioning")
    (setup-geo-partition! node ysql.client/tablespace-name)
    (ysqlsh test node :-c (str "GRANT CREATE ON TABLESPACE "
                               ysql.client/tablespace-name
                               "_1a TO " ysql.client/user ";"))
    (ysqlsh test node :-c (str "GRANT CREATE ON TABLESPACE "
                               ysql.client/tablespace-name
                               "_2a TO " ysql.client/user ";"))))
