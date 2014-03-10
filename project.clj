(defproject hbase_mr_test "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
		 [clojure-hbase "0.92.3"]
		 [cascalog/cascalog-core "2.0.0"]
		 [cascalog "2.0.0"]
	         [hbase.cascalog "0.2.1-SNAPSHOT"]
		 [org.slf4j/slf4j-api "1.7.2"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]]}}
  :repositories [["conjars.org" "http://conjars.org/repo"]]
  :jvm-opt ["-Xms768m" "-Xmx768m"]
  :main hbase-mr-test.core  
 )





