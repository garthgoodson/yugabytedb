(defproject io.jepsen/yugabyte "0.1.2"
  :description "Jepsen testing for YugaByteDB"
  :url "http://yugabyte.com/"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [clj-http "3.13.1" :exclusions [commons-io
                                                 commons-logging
                                                 potemkin]]
                 [jepsen "0.3.14"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]
                 [io.jepsen/sql "0.1.1"]
                 [com.yugabyte/cassandra-driver-core "3.10.3-yb-3"]
                 [org.slf4j/slf4j-api "2.0.19"]
                 [org.clojure/data.json "2.5.2"]
                 [com.yugabyte/jdbc-yugabytedb "42.3.5-yb-3"]
                 [version-clj "2.0.3"]
                 [clj-wallhack "1.0.1"]]
  :main jepsen.yugabyte.cli
  :jvm-opts ["-Djava.awt.headless=true"
             "-Djava.net.preferIPv4Stack=true"
             "-Xms4g"
             "-Xmx24g"
             "-XX:-OmitStackTraceInFastThrow"
             ])
