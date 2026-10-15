(ns jepsen.yugabyte.cli
  "Runs YugaByteDB tests."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.cli :as cli]
            [jepsen.random :as random]
            [jepsen.store :as store]
            [jepsen.sql]
            [jepsen.yugabyte [core :as core]
                             [nemesis :as nemesis]]))

(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis faults, and turns it into a set
  of keywords."
  [s]
  (if (= s "none")
    #{}
    (->> (str/split s #",")
         (map keyword)
         set)))

(defn one-of
  "Like jepsen.cli/one-of but doesn't 'eat' namespaces"
  [coll]
  (let [stringify      (fn [s] (if (qualified-keyword? s)
                                 (str (namespace s) "/" (name s))
                                 (name s)))
        coll-keys      (if (map? coll) (keys coll) coll)
        coll-key-names (sort (map stringify coll-keys))]
    (str "Must be one of " (str/join ", " coll-key-names))))

;
; Options
;
; For the options format, see clojure.tools.cli/parse-opts
;

(def cli-opts
  "Options for single or multiple tests."
  (cli/merge-opt-specs
    ; These options give us things like --isolation,
    ; --expected-consistency-model, --key-count, etc.
    jepsen.sql/cli-opts

    [[nil "--corrupt-file-type TYPES" "A single type of file to restrict our file corruptions to. See nemesis/file-types."
      :parse-fn keyword
      :default :all
      :validate [nemesis/file-types (cli/one-of nemesis/file-types)]]

     ; Because many of our workloads have specific concurrency requirements, we
     ; let the workloads themselves fill in defaults.
     [nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
      :validate [(partial re-find #"^\d+n?$")
                 "Must be an integer, optionally followed by n."]]

     [nil "--connection-manager" "Enable connection manager flags on YB since 2024.2 version"
      :default false]

     [nil "--clock-skew-flags" "Enable soft clock skew flags on YB"
      :default true]

     [nil "--extreme-skew" "Enable extreme clock skew flags: master and tserver process can have different skew on one node"
      :default false]

     [nil "--experimental-tuning-flags" "Enable some experimental tuning flags which are supposed to help YB recover faster"
      :default false]

     [nil "--heartbeat-flags" "Enable heartbeat tserver tracing flags on YB"
      :default false]

     [nil "--final-recovery-time SECONDS" "How long, at most, to wait for the cluster to stabilize at the end of a test"
      :default  600
      :parse-fn parse-long
      :validate [(complement neg?) "Must be a non-negative number"]]

     [nil "--geo-partition" "For workloads like `append`, partitions tables with geo_partition"]

     [nil "--[no-]linearizable-keys" "If set, assumes keys are linearizable for the wr workload."
      :id :linearizable-keys?
      :default true]

     [nil "--locking MODE" "Locking mode for append workloads: mixed (default), optimistic, or pessimistic"
      :default :mixed
      :parse-fn keyword
      :validate [#{:mixed :optimistic :pessimistic} "Must be one of: mixed, optimistic, pessimistic"]]

     [nil "--master-flags FLAG" "Extra gflag for master (repeatable): flag_name or flag_name=value. pg_conf flags are merged."
      :default []
      :assoc-fn (fn [m _ v] (update m :master-flags conj v))]

     [nil "--nemesis SPEC" "A comma-separated list of nemesis fault types"
      :parse-fn parse-nemesis-spec
      :validate [(partial every? core/nemesis-specs)
                 (str "Should be a comma-separated list of faults. A failure "
                      (.toLowerCase (cli/one-of core/nemesis-specs))
                      ". Or, you can use 'none' to indicate no failures.")]]

     [nil "--nemesis-interval SECS"
      "Roughly how long to wait between nemesis operations. Default: 10s."
      :default 10
      :parse-fn parse-long
      :validate [(complement neg?) "should be a non-negative number"]]

     ["-o" "--os NAME" "Operating system: either centos or debian."
      :default :debian
      :parse-fn keyword
      :validate [#{:centos :debian} "One of `centos` or `debian`"]]

     [nil "--rate HZ" "Maximum request rate, in reqs/sec."
      :default  1000
      :parse-fn read-string
      :validate [#(and (number? %) (pos? %)) "must be positive"]]

     ["-r" "--replication-factor INT" "Number of nodes in each Raft cluster."
      :default 3
      :parse-fn #(Long/parseLong %)
      :validate [pos? "Must be a positive integer"]]

     [nil "--nemesis-stable-period SECS" "If given, rotates the mixture of nemesis faults over time with roughly this period."
      :default  30
      :parse-fn parse-long
      :validate [pos? "Must be a positive number."]]

     [nil "--stress-tuning" "Enable stress-test flags that use tiny thresholds for internal subsystems (batching, compaction, WAL, cache, splitting, etc.) to trigger edge cases more frequently"
      :default true]

     [nil "--table-count INT" "Number of tables to spread rows across."
      :default 5]

     [nil "--table-locks" "If set, enables table-level locks: an experimental feature. See https://docs.yugabyte.com/stable/explore/transactions/explicit-locking/#table-level-locks for details."]

     [nil "--time-limit SECONDS"
      "Excluding setup and teardown, how long should a test run for, in seconds?"
      :default  300
      :parse-fn parse-long
      :validate [pos? "Must be positive"]]

     [nil "--trace-cql" "If provided, logs CQL queries"
      :default false]

     [nil "--tserver-flags FLAG" "Extra gflag for tserver (repeatable): flag_name or flag_name=value. pg_conf flags are merged."
      :default []
      :assoc-fn (fn [m _ v] (update m :tserver-flags conj v))]

     [nil "--url URL" "URL to Yugabyte tarball to install, has precedence over --version"
      :default nil]

     [nil "--version VERSION" "What version of Yugabyte to install"
      :default "2026.1.1.1-b2"]

     [nil "--yugabyte-ssh" "Override SSH options with hardcoded defaults for Yugabyte's internal testing environment"
      :default false]

     ]))

(def test-all-opts
  "CLI options for testing everything."
  [[nil "--only-workloads-expected-to-pass" "If present, skips tests which are not expected to pass"
    :default false]

   [nil "--test-number I" "For test-all, runs only the `I`th test out of all the tests that would otherwise be run."
    :parse-fn parse-long
    :validate [(complement neg?) "Must be non-negative"]]

   ["-w" "--workload NAME"
    "Test workload to run. If omitted, runs all workloads"
    :parse-fn keyword
    :validate [core/all-workloads (one-of core/all-workloads)]]])

(def single-test-opts
  "Command line options for single tests"
  [["-w" "--workload NAME" "Test workload to run"
    :default  :jsql/append
    :parse-fn keyword
    :missing (str "--workload " (one-of core/all-workloads))
    :validate [core/all-workloads (one-of core/all-workloads)]]])

;; Subcommands

(defn all-tests
  "Takes CLI options and constructs a lazy sequence of test maps."
  [opts]
  (let [workloads     (if-let [w (:workload opts)]
                        [w]
                        (keys core/all-workloads))
        nemeses       (if-let [n (:nemesis opts)]
                        [n]
                        core/all-nemeses)
        counts        (range (:test-count opts))
        tests (for [i               counts
                    nemesis         nemeses
                    workload        workloads
                    suggested-opts  (get core/suggested-opts workload [{}])]
                (-> opts
                    (merge {:workload workload, :nemesis nemesis})
                    (merge suggested-opts)
                    (core/yb-test)))]
    (if-let [i (:test-number opts)]
      (try [(nth tests i)]
           (catch IndexOutOfBoundsException e
             (println "End of tests")
             (System/exit 250)))
      tests)))

(defn -main
  "Handles CLI arguments"
  [& args]
  (cli/run! (merge
              (cli/serve-cmd)
              (cli/test-all-cmd
                {:opt-spec (cli/merge-opt-specs cli-opts test-all-opts)
                 :tests-fn all-tests})
              (cli/single-test-cmd
                {:opt-spec (cli/merge-opt-specs cli-opts single-test-opts)
                 :test-fn core/yb-test}))
            args))
