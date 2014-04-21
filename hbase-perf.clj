;(ns hbase-poc.core)


;;;;;;;;;;;;; PROD CODE ;;;;;;;;;;;;;;;;;;;;;;;;
;; https://github.com/clj-time/clj-time

(require ['clojure-hbase.core :as 'hb])
(import '(org.apache.hadoop.hbase.util Bytes))
(import '(org.apache.hadoop.hbase HBaseConfiguration))
(require '[clojure.string :as s])
(require '[clj-time.core :as t]) ;;clj datetime lib
(require '[clj-time.coerce :as c]) ;; clj datetime format for coercing joda time
(require '[clj-time.format :as f])
;(require '[clj-time.local :as l]) ;; lib not loading? lein deps?


;ids prod
(hb/set-config  
    (hb/make-config
     {"hbase.zookeeper.quorum" "prodmserver11c.ids.enernoc.net"
      "hbase.zookeeper.property.clientPort" "2181"}))

;function to make reverse hash from pid
(defn rowkey-pid [pid] 
  (Long/parseLong (s/reverse (format "%015d" pid))))

;function to restore rowkey - have to handle adding 0's back
(defn add0s [s] (str s "0"))

(defn add-0s [str cnt] ; pass string and num of 0's to add
  (loop [s str
         n cnt]
    (if (< n 1)
      (Long/parseLong s)
      (recur (add0s s) (dec n)))))


;pass row from get or scanner. Returns raw row byte array
(defn raw-row [hbase-row] 
  (.getRow hbase-row))

;pass whole row byte array- get first piece and return reverse hash
(defn get-row-revhash [raw-row] 
  (Bytes/toLong (byte-array (first (partition 8 raw-row)))))

;pass whole row byte array- get second piece and return date
(defn get-row-int-dt [hbase-row] 
  (c/to-date-time (- Long/MAX_VALUE (Bytes/toLong (byte-array (second (partition 8 (raw-row hbase-row))))))))


;restore rowkey to original pid from hashed pid
(defn restore-rk [hashed-pid]
  (let [pid (s/reverse (Long/toString hashed-pid))]
    (let [grow-num (- 15 (count pid))]
     (add-0s pid grow-num))))


;combines all the above functions. Pass hbase row to return actual pid. 
(defn split-row [hb-row] 
  (->> hb-row
       (raw-row)
       (get-row-revhash)
       (restore-rk)))


;tuple of pid and int date returned
(defn split-row-pd [hb-row] 
  (let [pid (->> hb-row
       (raw-row)
       (get-row-revhash)
       (restore-rk))
        int-dt (get-row-int-dt hb-row)]
    [pid int-dt]))


;compenents to make row key
;long builder
(defn rev-maxlong [long]  
  (- Long/MAX_VALUE long))

;make rowkey when passed 3 part key
(defn make-rowkey [pid int-date batch-id] 
  (let [rk (rowkey-pid pid)
        int-dt (rev-maxlong (c/to-long int-date))
        b-id (rev-maxlong batch-id)]      
    (->> [rk int-dt b-id]
         (map #(Bytes/toBytes %))
         (into-array)
         (mapcat seq)
         (byte-array))))


;;hbase scanner stuff
;get single column
(defn fscan-map  
([table cf col]
  (hb/with-scanner [scan-intervals (hb/table table)]
   (hb/scan scan-intervals :column [cf col])))
([table cf col start stop]
  (hb/with-scanner [scan-intervals (hb/table table)]
   (hb/scan scan-intervals :column [cf col]
           :start-row start
           :stop-row stop
           :max-versions 1))))

;get all columns not one. Pass vec of cols
(defn fscan-map-cols  
([table cf cols]
  (hb/with-scanner [scan-intervals (hb/table table)]
   (hb/scan scan-intervals :columns [cf cols])))
([table cf cols start stop]
  (hb/with-scanner [scan-intervals (hb/table table)]
   (hb/scan scan-intervals :columns [cf cols]
           :start-row start
           :stop-row stop
           :max-versions 1))))


;function for hb/as-map
;single value
(defn map-row [row]
  (hb/as-map row
           :map-family #(keyword (Bytes/toString %))
           :map-qualifier #(keyword (Bytes/toString %))
           :map-timestamp #(java.util.Date. %)
           :map-value #(Bytes/toFloat %) str))

;multiple col values
(defn map-row-cols [row]
  (hb/as-map row
           :map-family #(keyword (Bytes/toString %))
           :map-qualifier #(keyword (Bytes/toString %))
           :map-timestamp #(java.util.Date. %)
           ;:map-value #(Bytes/toFloat %) 
           str))
 
;create tuple - now for just one col- reading. 
(defn get-reading [col-val cf col] (get-in col-val [(keyword cf) (keyword col)]))

(defn split-row-datetup-reading [hb-row cf col] 
  (let [pid (->> hb-row
                 (raw-row)
                 (get-row-revhash)
                 (restore-rk))
        int-dt (get-row-int-dt hb-row)
        reading-val (second (first (get-reading (map-row hb-row) cf col)))] 
    [pid int-dt (t/year int-dt) (t/month int-dt) (t/day int-dt) (t/hour int-dt) reading-val]))










;; testing date componenets for grouping: 
(def test-date (c/to-date-time "2013-04-14T13:08:00.000Z"))
(def row-tup (split-row-pd rk-scan-test))

(t/year (second row-tup))
(t/month (second row-tup))
(t/day (second row-tup))
(t/hour (second row-tup))

(t/year test-date)
(t/month test-date)
(t/day test-date)
(t/hour test-date)



;; DATA TESTING
;test pid 16355656
(def start-key (make-rowkey 16355656 "2013-02-19T00:05:00.000Z" 1))
(def end-key (make-rowkey 16355656 "2013-04-19T00:20:00.000Z" 1))
(def scan-test-1 (iterator-seq (.iterator (fscan-map "fivemin_2" "Intervals" "V" end-key start-key))))
(def tup-test-1 (into [] (for [row scan-test-1] (split-row-datetup-reading row "Intervals" "V"))))
(count tup-test-1)
(reduce + (for [row tup-test-1] (last row))) ;sum readings for period


;another 17571399, 487472, 17919268 (one-min), 16350267 (one min)
(def start-key (make-rowkey 16350267 "2014-04-02T20:20:00.000Z" 1))
(def end-key (make-rowkey 16350267 "2014-04-02T21:00:00.000Z" 1))
(iterator-seq (.iterator (fscan-map "onemin" "Intervals" "V" end-key start-key)))
(def scan-test-5 (iterator-seq (.iterator (fscan-map "onemin" "Intervals" "V" end-key start-key))))
(count scan-test-5)
(def row-test-5 (nth scan-test-5 1))
(type row-test-5)

(split-row-datetup-reading row-test-5 "Intervals" "V")
(for [row scan-test-5] (split-row-datetup-reading row "Intervals" "V"))
(into [] (for [row scan-test-5] (split-row-datetup-reading row "Intervals" "V"))) ;works nicely 
(def tup-test-5 (into [] (for [row scan-test-5] (split-row-datetup-reading row "Intervals" "V"))))

tup-test-5
(reduce + (for [row tup-test-5] (last row)))







