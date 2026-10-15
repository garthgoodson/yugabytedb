(ns jepsen.yugabyte.db.tserver
  "Installs and runs the Yugabyte tablet server process."
  (:require [clojure [pprint :refer [pprint]]
             [string :as str]]
            [clojure.tools.logging :refer :all]
            [dom-top.core :as dt]
            [jepsen [control :as c]
             [core :as jepsen]
             [db :as db]
             [random :as random]
             [util :as util]]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.yugabyte [util :refer [parse-version]]]
            [version-clj.core :as v]
            [jepsen.yugabyte.db.core :as core]
            [slingshot.slingshot :refer [try+ throw+]]))

(def bin (str core/dir "/bin/yb-tserver"))
(def log-dir (str core/data-dir "/yb-data/tserver/logs"))
(def log-file (str log-dir "/stdout"))
(def pid-file (str core/dir "/tserver.pid"))

(defn api-flags
  "API-specific options for tserver"
  [test node]
  (if (:connection-manager test)
    [:--start_pgsql_proxy
     :--pgsql_proxy_bind_address (str (cn/ip node))
     :--ysql_conn_mgr_port 5431
     ]
    [:--start_pgsql_proxy
     :--pgsql_proxy_bind_address (cn/ip node)
     ]))

(defn read-committed-flags
  "Read committed specific flags"
  [test]
  (if (= :read-committed (:isolation test))
    [:--yb_enable_read_committed_isolation]
    []))

(defn serializable-flags
  "Serializable-isolation specific flags. skip_prefix_locks only exists in
  2026.1+; setting it on older versions makes the cluster fail to start."
  [test]
  (if (and (= :serializable (:isolation test))
           (v/newer-or-equal? (:version test) core/minimal-skip-prefix-locks-version))
    [:--skip_prefix_locks=false]
    []))

(defn table-lock-flags
  "append-table workload flags: transactional DDL, table-level object locking,
  and concurrent DDL. All three are preview-gated, so they're added to
  allowed_preview_flags_csv (apply-extra-gflags collapses this into a single
  flag alongside any other preview flags, e.g. connection manager)."
  [test]
  ; aphyr, 2026-08-14: is the implication here that this test does not pass
  ; unless you specify these options? I suspect the answer is yes:
  ; https://docs.yugabyte.com/stable/explore/transactions/explicit-locking/
  ; suggests that DML and DDL could run concurrently and maybe that messes
  ; things up?
  (if (:table-locks test)
    [:--allowed_preview_flags_csv "enable_object_locking_for_table_locks,ysql_yb_ddl_transaction_block_enabled,ysql_enable_concurrent_ddl"
     :--ysql_yb_ddl_transaction_block_enabled
     :--enable_object_locking_for_table_locks
     :--ysql_enable_concurrent_ddl]
    []))

(defn connection-manager-preview-flags
  "Preview flags for connection manager feature"
  [test]
  (if (:connection-manager test)
    [:--allowed_preview_flags_csv "enable_ysql_conn_mgr"
     :--enable_ysql_conn_mgr]
    []))

(defn heartbeat-flags
  "Heartbeat tracing flags"
  [test]
  (if (:heartbeat-flags test)
    [:--heartbeat_interval_ms 100
     :--heartbeat_rpc_timeout_ms 1500
     :--retryable_rpc_single_call_timeout_ms 2000
     :--rpc_connection_timeout_ms 1500
     :--leader_failure_exp_backoff_max_delta_ms 1000
     :--leader_failure_max_missed_heartbeat_period 3
     :--consensus_rpc_timeout_ms 300
     :--client_read_write_timeout_ms 6000]
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
  "TServer performance-related flags we use to try and improve YB latencies."
  [test]
  (into (core/perf-flags test)
        [; Reduce retries to try and get a handle on ridiculous latencies
         :--ysql_pg_conf_csv "yb_max_query_layer_retries=5"]))

(defn stress-flags
  "Stress-test flags for tserver — DocDB, RocksDB, MVCC, intent cleanup."
  [test]
  (into (core/stress-flags test)
        (if (:stress-tuning test)
          [:--txn_max_apply_batch_records 5
           ;      :--db_write_buffer_size 524288
           :--db_block_cache_size_bytes 8388608
           ;     :--aborted_intent_cleanup_ms 1000
           :--timestamp_history_retention_interval_sec 5
           ;     :--transaction_deadlock_detection_interval_usec 1000000
           :--backfill_index_write_batch_size 10
           ; :--cdc_stream_records_threshold_size_bytes 1024
           ]
          [])))


(defn list-all-tservers
  "Asks a node to list all the tservers it knows about. Columns are
  `UUID  RPC-Host/Port  Heartbeat-delay  Status ...`, so we capture the status
  as :state (ALIVE/DEAD) - without it the readiness filter in await-tservers has
  nothing to match on and never counts a tserver as up."
  [test]
  (->> (core/yb-admin test :list_all_tablet_servers)
       (str/split-lines)
       rest
       (map (fn [line]
              (->> line
                   (re-find #"(\w+)\s+([^\s]+)\s+([^\s]+)\s+(\w+)")
                   next
                   (zipmap [:uuid :address :heartbeat :state]))))))

(defn wipe!
  "Wipes all data files for the node."
  []
  (c/su (c/exec :rm :-rf core/data-dir))
  (info "Wiped" core/data-dir))

(defn await-tservers
  "Waits until all tservers for a test are online, according to this node."
  [test]
  (dt/with-retry [tries 60]
    (when (zero? tries)
      (throw (RuntimeException. "Giving up waiting for tservers.")))

    (Thread/sleep 1000)

    (if (= (count (:nodes test))
           (->> (list-all-tservers test)
                (filter (comp #{"ALIVE"} :state))
                count))
      :ready
      (do (info "Waiting for tservers")
          (retry (dec tries))))

    (catch RuntimeException e
      (condp re-find (.getMessage e)
        #"Leader not yet ready to serve requests" (retry (dec tries))
        #"This leader has not yet acquired a lease" (retry (dec tries))
        #"Could not locate the leader master" (retry (dec tries))
        #"Leader not yet replicated NoOp" (retry (dec tries))
        #"Not the leader" (retry (dec tries))
        (throw e)))))

(defrecord DB []
  db/DB
  (setup! [db test node]
    (core/suppress-interrupted-exception
      (c/su
        (core/install! test)
        (core/configure!)
        (db/start! db test node)
        (await-tservers test)
        (core/await-ysqlsh test node)
        (core/await-api test node)
        (jepsen/synchronize test)
        (when (= node (first (:tserver (:roles test))))
          (core/setup-db! test node)))))

  (teardown! [db test node]
    (c/su
      (db/kill! db test node)
      (wipe!)))

  db/Process
  (kill! [this test node]
    (c/su
      (cu/kill-bin! bin)
      (c/su (cu/grepkill! "postgres"))
      (c/su (cu/grepkill! "odyssey"))))

  (start! [this test node]
    (c/su (c/exec :mkdir :-p log-dir)
          (apply cu/start-daemon!
                 {:logfile log-file
                  :pidfile pid-file
                  :chdir   core/dir}
                 bin
                 (core/apply-extra-gflags
                   [:--tserver_master_addrs (core/master-addresses test)
                    ; Tracing
                    :--enable_tracing
                    :--rpc_slow_query_threshold_ms 1000
                    (core/basic-flags node)
                    (core/experimental-tuning-flags test)
                    (core/geo-partitioning-flags test node (:nodes test))
                    (core/packed-columns-flags test)
                    (core/random-clock-skew-flags test node)
                    (api-flags test node)
                    (connection-manager-preview-flags test)
                    (heartbeat-flags test)
                    (perf-flags test)
                    (read-committed-flags test)
                    (serializable-flags test)
                    (stress-flags test)
                    (table-lock-flags test)]
                   (:tserver-flags test)))))

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
