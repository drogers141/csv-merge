(ns csv-merge.core-test
  (:require [clojure.test :refer :all]
            [csv-merge.core :refer :all]))

(deftest reorder-test
  (let [expected ["id" "ident" "type" "name" "latitude_deg"]]
    (are [row sk-index] (= (reorder row sk-index) expected)
         ["id" "ident" "type" "name" "latitude_deg"]  0
         ["ident" "id" "type" "name" "latitude_deg"]  1
         ["ident" "type" "id" "name" "latitude_deg"]  2
         ["ident" "type" "name" "id" "latitude_deg"]  3
         ["ident" "type" "name" "latitude_deg" "id"]  4)))

(deftest reorder-sort-test
  (let [rows [["type" "id" "ident" "name" "latitude_deg"]
              ["small_airport" "6525" "00AL" "Epps Airpark" "34.8647994995117"]
              ["small_airport" "6524" "00AK" "Lowell Field" "59.94919968"]
              ["heliport"
               "6526"
               "00AR"
               "Newport Hospital & Clinic Heliport"
               "35.608699798584"]
              ["heliport" "6523" "00A" "Total Rf Heliport" "40.07080078125"]]
        expected [["id" "type" "ident" "name" "latitude_deg"]
                   ["6523" "heliport" "00A" "Total Rf Heliport" "40.07080078125"]
                   ["6524" "small_airport" "00AK" "Lowell Field" "59.94919968"]
                   ["6525" "small_airport" "00AL" "Epps Airpark" "34.8647994995117"]
                   ["6526"
                    "heliport"
                    "00AR"
                    "Newport Hospital & Clinic Heliport"
                    "35.608699798584"]]]
    (is (= (reorder-sort rows 1) expected))))

(deftest mins-test
  (are [rows minrows] (= minrows (mins rows))
       {1 ["rossty01" "2014" "SDN" "NL" "1980000"]
        2 ["zimmejo02" "2014" "WAS" "NL" "7500000"]
        3 ["ramirar01" "2014" "MIL" "NL" "16000000"]}
       
       {3 ["ramirar01" "2014" "MIL" "NL" "16000000"]}
       ;;;;
       {1 ["rossty01" "2014" "SDN" "NL" "1980000"]
        2 ["ramirar01" "Some" "Other" "Values" 42]
        3 ["zimmejo02" "2014" "WAS" "NL" "7500000"]
        4 ["ramirar01" "2014" "MIL" "NL" "16000000"]}
       
       {2 ["ramirar01" "Some" "Other" "Values" 42]
        4 ["ramirar01" "2014" "MIL" "NL" "16000000"]}
       ;;;;
       {1 [23 0 0 0]
        2 [100 0 0 0]
        3 [23 1 2 3]
        4 [2145 12 12 12]
        5 [23 1 1 1]}
       
       {1 [23 0 0 0]
        3 [23 1 2 3]
        5 [23 1 1 1]}
       ;;;;
       {1 ["rossty01" "2014" "SDN" "NL" "1980000"]
        2 ["rossty01" "2014" "SDN" "NL" "1980000"]
        3 ["rossty01" "2014" "SDN" "NL" "1980000"]}
       
       {1 ["rossty01" "2014" "SDN" "NL" "1980000"]
        2 ["rossty01" "2014" "SDN" "NL" "1980000"]
        3 ["rossty01" "2014" "SDN" "NL" "1980000"]}))

(deftest output-row-test
  (are [irmap headersz output] (= output (output-row irmap headersz))
       {1 [1 "f1col1" "f1col2" "f1col3"]
        8 [1 "f2col1" "f2col2"]}
        10
        [1 "f1col1" "f1col2" "f1col3" nil nil nil nil "f2col1" "f2col2"]
        ;;;;
        {4 ["id42" "f1col1" "f1col2"]
        6 ["id42" "f2col1" "f2col2"]}
        8
        ["id42" nil nil nil "f1col1" "f1col2" "f2col1" "f2col2"]
        ;;;;
        {5 ["id" 5 6 7]}
        8
        ["id" nil nil nil nil 5 6 7]
        ;;;;
        {1 ["id" 1 2 3]}
        8
        ["id" 1 2 3 nil nil nil nil]
        ;;;;
        {1 ["id" 1 2 3]
         4 ["id" 4 5 6]
         7 ["id" 7 8 9]}
        10
        ["id" 1 2 3 4 5 6 7 8 9]))

(deftest output-header-and-indices-test
  (let [infiles ["resources/allstar-out.csv" 
                 "resources/batting-out.csv"
                 "resources/salaries-out.csv"]
        expected {:header 
                  ["playerID" "gameID" "yearID" "gameNum" "teamID" "lgID" 
                   "GP" "startingPos" "yearID" "stint" "teamID" "lgID" "G" 
                   "AB" "R" "H" "2B" "3B" "HR" "RBI" "SB" "CS" "BB" "SO" 
                   "IBB" "HBP" "SH" "SF" "GIDP" "yearID" "teamID" "lgID" 
                   "salary"]
                  :indices [1 8 29]}
                  ]
    (is (= (output-header-and-indices infiles)
           expected))))

;[seqs indices-to-seqs min-rows]
(deftest incr-seqs-test 
  (let [s1 { :csv 1 :row 1}
        s2 {:csv 2 :row 1}
        s3 {:csv 3 :row 2}
        seqs [s1 s2 s3]
        indices-to-seqs {1 s1 2 s2 3 s3}
        min-rows {1 :row1  3 :row3}
        expected [{:csv 1 :row 2} s2 {:csv 3 :row 3}]]
    (is (= (incr-seqs seqs indices-to-seqs min-rows)
           expected))))
        
    