(ns csv-merge.core
  "Takes as input more than one csv file that share a key.
   The shared key, or column, may have different names among the
   files.
   This program merges the input files into a single output file
   with all of the columns combined.  It is assumed that all column
   names other than the shared column are unique across all files.
   The output will have the first column as the shared key with column
   name taken from the first input file.
   The output file merges the input files by merging rows where the
   shared key has the same value, so it is assumed that the shared key
   has unique values within a single csv input file.
   The input files may be large - e.g. up to 2 million lines long and
   255 columns wide.
   Program args - file paths and 0-based indices of the shared key's
       location in each file.
   Run with no args to see usage.
   "
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(defn reorder [row sk-index]
  "Takes vector created from row of data and index of shared-key
   and returns row reordered with shared key at index 0"
  (into [(row sk-index)]
        (concat (subvec row 0 sk-index) (subvec row (inc sk-index)))))

(defn reorder-sort [rows sk-index]
  "Reorder all rows to put sk-index at 0 and sort body by sk-index.
   Assumes first row is header with names of columns.
   rows - header and all rows from csv file as vectors
   sk-index - index of shared key column in input file"
  (let [reordered (vec (map #(reorder % sk-index) rows))
        header (reordered 0)
        body (subvec reordered 1)]
    (into [header]
          (sort-by first body))))

(defn read-csv [infile]
  "Returns 2d vector of complete csv file. (Not lazy)."
  (with-open [in (io/reader infile)]
     (vec
       (csv/read-csv in))))

(defn write-csv [rows outfile]
  "Writes 2d vector of data rows to outfile"
  (with-open [out (io/writer outfile)]
    (csv/write-csv out rows)))

(defn reorder-sort-file [infile sk-index outfile]
  "Reads csv file infile, reorders with column at sk-index as first
   column, sorts by that column and writes to outfile.
   Note csv parser stringifies numeric values."
  (let [rows (read-csv infile)]
    (write-csv (reorder-sort rows sk-index) outfile)))

;;** Assumes shared key values are unique within a single input file
(defn mins [indices-to-rows]
  "Returns map with indices and rows of one or more rows whose first
   element (the shared key) has the lowest value.

   indices-to-rows - map with integer keys and vector values
      key is index of filename starting point in output header
      value is the output row for that file"
  (let [minval (-> (map first (vals indices-to-rows)) sort first)
        get-min-entry #(= minval (first (second %)))]
    (into {} (filter get-min-entry indices-to-rows))))

;;** Doesn't care if column names clash among different files
(defn output-row [indices-to-rows headersz]
  "Merges one or more rows of data from intermediate files
   to the output row, adding shared key, ordering and padding with
   nils as necessary.  See tests for examples.
   Returns vector of single output row.

   indices-to-rows - map with integer keys and vector values
      key is index of filename starting point in output header
      value is the output row for that file
   headersz - number of columns in output header

   TODO: works, but maybe could be simplified"
  (let [sk-val (first (first (vals indices-to-rows)))
        nils #(take % (repeat nil))]
    (loop [row [sk-val] ks (sort (keys indices-to-rows))]
      (let [rowsz (count row) k (first ks)]
        (cond
           (>= rowsz headersz)  (vec row)
           (= rowsz k) (recur (concat row (rest (indices-to-rows k))) (rest ks))
           :else
           (if (seq ks)
             (recur (concat row (nils (- k rowsz))) ks)
             (recur (concat row (nils (- headersz rowsz))) ks)))))))

(defn read-header [file]
  "Returns csv file header as vector."
  (with-open [in (io/reader file)]
    (-> (csv/read-csv in) first vec)))

(defn output-header-and-indices [infiles]
  "Returns map with
       :header - vector of final header of column names
       :indices - indices of starting column in the output header for
           each infile in order received
           Note that 0 is reserved for the shared key.
   infiles - vector (any seq) of intermediate files in order desired"
  (loop [header (read-header (first infiles))
         files (rest infiles)
         indices [1]]
    (if (seq files)
      (let [next-part (-> (first files) read-header rest)]
        (recur (concat header next-part)
               (rest files)
               (conj indices (count header))))
      {:header (vec header) :indices indices})))

(defn csv-seq [file]
  "Returns map with file reader in :rdr, lazy csv reader in :csv
   and row index :row. :row starts with 1 - skips header.
   Call (.close (varname :rdr)) when done."
  (let [in (io/reader file)
        c (csv/read-csv in)]
    {:rdr in :csv c :row 1}))

(defn incr-seqs [seqs indices-to-seqs min-rows]
  "Helper returns csv-seqs with :row incremented if their
   index is in min rows.
   TODO - kludgy"
  (let [inc-indices (keys min-rows)
        to-inc (vals (select-keys indices-to-seqs inc-indices))
        bools (vec (map #(> (.indexOf to-inc %) -1) seqs))
        bool-inc-pairs (map vector seqs bools)
        do-inc #(assoc %1 :row (inc (%1 :row)))]
    (for [p bool-inc-pairs]
      (if (second p) (do-inc (first p))
        (first p)))))

;;** Assumes header with column names on all files
(defn merge-to-outfile-orig [infiles outfile]
 "Creates output csv file merged from intermediate csv files.
   infiles - vector of intermediate file paths
       order will determine output column order
   outfile - final product"
 (let [peek-next #(nth (% :csv) (% :row) nil)
       {:keys [header indices]} (output-header-and-indices infiles)]
   (loop [output [header]
          csv-seqs (vec (map csv-seq infiles))
          indices indices]
     (if (seq csv-seqs)
       (let [candidate-rows (vec (map peek-next csv-seqs))
             candidates (zipmap indices candidate-rows)
             viable (into {} (filter second candidates))
             minrows (mins viable)]
         (if
           (pos? (count minrows))
           ;; create output row and add to output
           ;; remove any csv-seqs and indices that return nil
           ;; advance index of csv-seqs that we used
           (let [new-row (output-row minrows (count header))
                 prune #(into [] (map second (filter first (map vector candidate-rows %))))
                 pruned-seqs (prune csv-seqs)
                 indices-to-seqs (zipmap indices csv-seqs)]
             (recur (conj output new-row)
                    (incr-seqs pruned-seqs indices-to-seqs minrows)
                    (prune indices)))
           (write-csv output outfile)))))))

;;** Assumes header with column names on all files
(defn merge-to-outfile [infiles outwr]
 "Creates output csv file merged from intermediate csv files.
  Lazily reads from all infiles and outputs one line at a time to output.
  infiles - vector of intermediate file paths
      order will determine output column order
  outwr - output writer to opened file"
 (let [peek-next #(nth (% :csv) (% :row) nil)
       {:keys [header indices]} (output-header-and-indices infiles)]
   (csv/write-csv outwr [header])
   (loop [csv-seqs (vec (map csv-seq infiles))
          indices indices]
     (when (seq csv-seqs)
       (let [candidate-rows (vec (map peek-next csv-seqs))
             candidates (zipmap indices candidate-rows)
             viable (into {} (filter second candidates))
             minrows (mins viable)]
         (when (pos? (count minrows))
           ;; create output row and write to outwr
           ;; remove any csv-seqs and indices that return nil
           ;; advance index of csv-seqs that we used
           (let [new-row (output-row minrows (count header))
                 prune #(into [] (map second (filter first (map vector candidate-rows %))))
                 pruned-seqs (prune csv-seqs)
                 indices-to-seqs (zipmap indices csv-seqs)]
             (csv/write-csv outwr [new-row])
             (recur (incr-seqs pruned-seqs indices-to-seqs minrows)
                    (prune indices)))))))))

(defn read-all-into-memory [infiles]
  "For testing - read all the csv input files into memory together.
   Returns line count of each file in order."
  (let [inmem (map read-csv infiles)]
    (vec (map count inmem))))

(def cli-options
  [["-h" "--help" "Print this help"
        :flag true]
   ["-o" "--output-file OUTFILE" "Path to create output csv.
    If not provided defaults to output.csv."]
   ["-t" "--test" "Run test for memory characteristics.
    Pass filenames to be read in as params to main"
        :flag true]])

(defn usage [options-summary]
  (->> ["Merge csv files that share a key - see module docs or readme."
        ""
        "Usage: csv-merge [options] input-file-args"
        ""
        "Options:"
        options-summary
        ""
        "Input-file-args"
        "infile index infile index [..]"
        "Pairs of input file paths followed by the zero-based index"
        "of the shared key column in that file."
        "For example:"
        "file1.csv 0 file2.csv 3 file3.csv 0"
        ""
        "Creates intermediate files from the input files that are sorted"
        "and reordered if needed. These are unceremoniously left in same"
        "dir as input file."]
       (str/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 (usage summary))
      (:test options) (do (println "arguments: " arguments)
                        (let [line-counts (read-all-into-memory arguments)]
                          (println "line counts: " line-counts))
                        (exit 0 "ok"))
      (zero? (count arguments)) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))
    (let [outfile (or (options :output-file) "output.csv")
          file-arg-pairs (partition 2 arguments)
          infiles (vec (map first file-arg-pairs))
          inter-files (map #(str (first (str/split % #".csv")) "-tmp.csv") infiles)
          indices (vec (map #(Integer/parseInt %) (map second file-arg-pairs)))
          reorder-sort-args (map vector infiles indices inter-files)]
      (do
        (println "creating sorted and reordered files ..")
        (doall
          (for [tuple reorder-sort-args]
            (apply reorder-sort-file tuple)))
        (println "creating output file: " outfile)
        (with-open [outwr (io/writer outfile)]
          (merge-to-outfile inter-files outwr))))))
