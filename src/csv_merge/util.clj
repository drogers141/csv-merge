(ns csv-merge.util
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.string :as str]))

(defn small-csv [in out cols rows]
  "Take csv file in and make smaller csv file out.
   cols, rows - size of out - using first rows and cols"
  (let [incsv (-> in io/file io/reader csv/read-csv)]
    (with-open [outwr (io/writer out)]
      (doall 
        (for [r (take rows incsv)]
          (csv/write-csv outwr [(subvec r 0 cols)]))))))

(defn small-csv-preserve-quotes [infile outfile cols rows]
  (with-open [in (io/reader infile)
              out (io/writer outfile)]
    (doall
      (for [r (take rows (line-seq in))]
        (doseq []
          (.write out (str/join \, (subvec (str/split r #",") 0 cols)))
          (.newLine out))))))
