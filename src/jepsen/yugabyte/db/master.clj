(ns jepsen.yugabyte.db.master
  "Installs and runs the Yugabyte master process."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer :all]
            [clj-http.client :as http]
            [dom-top.core :as dt]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.util :as util :refer [meh]]
            [jepsen.random :as random]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.yugabyte [util :refer [parse-version]]]
            [jepsen.yugabyte.db.core :as core]
            [slingshot.slingshot :refer [try+ throw+]]))

(def bin (str core/dir "/bin/yb-master"))
(def log-dir (str core/data-dir "/yb-data/master/logs"))
(def log-file (str log-dir "/stdout"))
(def pid-file (str core/dir "/master.pid"))

(defn list-all-masters
  "Asks a node to list all the masters it knows about."
  [test]
  (->> (core/yb-admin test :list_all_masters)
       (str/split-lines)
       rest
       (map (fn [line]
              (->> line
                   (re-find #"(\w+)\s+([^\s]+)\s+(\w+)\s+(\w+)")
                   next
                   (zipmap [:uuid :address :state :role]))))))

(defn voter?
  "True if a `list-all-masters` entry is a committed voting member of the master
  Raft group: it is ALIVE and currently acting as LEADER or FOLLOWER. A master
  that is still being added to the config shows up as a LEARNER (Raft PRE_VOTER)
  and is not yet a dependable quorum member."
  [master]
  (and (= "ALIVE" (:state master))
       (contains? #{"LEADER" "FOLLOWER"} (:role master))))

(defn converged?
  "True when the master Raft group has converged for this test: every expected
  master is a voting member and exactly one leader has been elected."
  [test]
  (let [masters (list-all-masters test)]
    ;(info :masters (with-out-str (pprint masters)))
    (and (= (count (:master (:roles test)))
            (count (filter voter? masters)))
         (= 1 (count (filter (comp #{"LEADER"} :role) masters))))))

(defn await-converged
  "Blocks until the master Raft group has converged: every expected master is an
  ALIVE voting member and a single leader has been elected. Retries through the
  transient errors that occur while masters are still starting up and electing a
  leader."
  [test]
  (dt/with-retry [tries 60]
    (when (zero? tries)
      (throw (RuntimeException. "Giving up waiting for masters to converge.")))

    (Thread/sleep 1000)

    (if (converged? test)
      :ready
      (do (info "Waiting for masters to converge...")
          (retry (dec tries))))

    (catch RuntimeException e
      (if (some #(re-find % (.getMessage e))
                [#"Could not locate the leader master"
                 #"Timed out"
                 #"Leader not yet ready to serve requests"
                 #"Leader not yet replicated NoOp"
                 #"Not the leader"
                 #"This leader has not yet acquired a lease"])
        (do (info "Waiting for masters:" (.getMessage e))
            (retry (dec tries)))
        (throw e)))))

(defn api-flags
  "API-specific options for master"
  [api node]
  (case api
    :ysql []
    :ycql []))

(defn table-lock-flags
  "Object locking is coordinated through the master, so the table-lock flag and
  its preview allow-list entry must be present on the master as well."
  [test]
  (if (:table-locks test)
    [:--allowed_preview_flags_csv "enable_object_locking_for_table_locks,ysql_yb_ddl_transaction_block_enabled"
     :--ysql_yb_ddl_transaction_block_enabled
     :--enable_object_locking_for_table_locks]
    []))

(defn perf-flags
  "Master performance-related flags."
  [test]
  (into (core/perf-flags test)
        []))

(defn stress-flags
  "Stress-test flags for master: tablet splitting.
  Disabled — tiny thresholds cause split storms during bootstrap."
  [test]
  (into (core/stress-flags test)
        (if (:stress-tuning test)
          []; :--enable_automatic_tablet_splitting true
          ;      :--tablet_split_low_phase_size_threshold_bytes 1024
          ;      :--tablet_split_high_phase_size_threshold_bytes 4096
          ;      :--tablet_force_split_threshold_bytes 8192
          [])))

(defn wipe!
  "Wipes all data files for the node."
  []
  (c/su (c/exec :rm :-rf core/data-dir))
  (info "Wiped" core/data-dir))

(defrecord DB []
  db/DB
  (setup! [db test node]
    (core/suppress-interrupted-exception
      (c/su
        (core/install! test)
        (core/configure!)
        (db/start! db test node)
        (await-converged test))))

  (teardown! [db test node]
    (c/su
      (db/kill! db test node)
      (wipe!)))

  db/Process
  (kill! [this test node]
    (c/su
      (cu/kill-bin! bin)))

  (start! [this test node]
    (c/su (c/exec :mkdir :-p log-dir)
          (apply cu/start-daemon!
                 {:logfile log-file
                  :pidfile pid-file
                  :chdir   core/dir}
                 bin
                 (core/apply-extra-gflags
                   [:--master_addresses (core/master-addresses test)
                    :--replication_factor (:replication-factor test)
                    (core/basic-flags node)
                    ; Should there be clock-skew flags here too, or just on the
                    ; tserver?
                    (core/experimental-tuning-flags test)
                    (core/geo-partitioning-flags test node (:nodes test))
                    (api-flags (:api test) node)
                    (perf-flags test)
                    (stress-flags test)
                    (core/packed-columns-flags test)
                    (table-lock-flags test)]
                   (:master-flags test)))))

  db/Pause
  (pause! [this test node]
    (c/su
      (cu/kill-bin! :STOP false bin)))

  (resume! [this test node]
    (c/su
      (cu/kill-bin! :CONT false bin)))

  db/Primary
  (setup-primary! [this test node])

  (primaries [this test]
    ; TODO: detect likely primaries from the API
    [])

  db/LogFiles
  (log-files [_ _ _]
    (merge (core/log-files-without-symlinks log-dir)
           {log-file "daemon.log"})))

(defn running-masters
  "Returns a list of nodes where a master process is running."
  [nodes]
  (->> nodes
       (pmap (fn [node]
               (try
                 (let [is-running
                       (-> (str "http://" node ":7000/jsonmetricz")
                           (http/get)
                           :status
                           (= 200))]
                   [node is-running])
                 (catch Exception e [node false]))))
       (filter second)
       (map first)))
