# csv-merge

Merge csv files..

Takes as input more than one csv file that share a key.
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

It is assumed that a single input file cat fit entirely in memory
but not the final product which is built lazily from sorted and ordered
intermediate csv files taken from each input file.

Program args - file paths and 0-based indices of the shared key's 
   location in each file.
   Run with no args to see usage.

This is the root of leiningen project directory.  From here, the source
is in 
src/
tests are in
test
Examples of input/intermediate and output files are in
resources/
See readme there for details.

## Usage

Run jar file with no options to see usage:
drogers@drogers-mbp:~/Desktop/working
$ java -jar csv-merge.jar
Merge csv files that share a key - see module docs or readme.

Usage: csv-merge [options] input-file-args

Options:
  -h, --help                 Print this help
  -o, --output-file OUTFILE  Path to create output csv.
    If not provided defaults to output.csv.

Input-file-args
infile index infile index [..]
Pairs of input file paths followed by the zero-based index
of the shared key column in that file.
For example:
file1.csv 0 file2.csv 3 file3.csv 0

Creates intermediate files from the input files that are sorted
and reordered if needed. These are unceremoniously left in same
dir as input files.

## Usage Example

drogers@drogers-mbp:~/Desktop/working
$ ls
allstar-partial.csv  batting-partial.csv  csv-merge.jar        salaries-partial.csv
drogers@drogers-mbp:~/Desktop/working
$ wc -l *.csv
       6 allstar-partial.csv
       6 batting-partial.csv
       6 salaries-partial.csv
      18 total
drogers@drogers-mbp:~/Desktop/working
$ head *-partial.csv
==> allstar-partial.csv <==
gameID,yearID,gameNum,playerID,teamID,lgID,GP,startingPos
ALS201407150,2014,0,zimmejo02,WAS,NL,0,
ALS201407150,2014,0,streehu01,SDN,NL,0,
ALS201407150,2014,0,rossty01,SDN,NL,0,
ALS201407150,2014,0,wainwad01,SLN,NL,1,
ALS201407150,2014,0,rizzoan01,CHN,NL,1,

==> batting-partial.csv <==
playerID,yearID,stint,teamID,lgID,G,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP
wainwad01,2014,1,SLN,NL,32,72,4,13,3,0,0,6,0,0,3,22,0,0,7,0,0
rossty01,2014,1,SDN,NL,32,56,6,10,1,0,0,3,0,0,3,22,0,1,2,1,0
watsoto01,2014,1,PIT,NL,78,3,0,1,0,0,0,0,0,0,0,2,0,0,0,0,0
rodrifr03,2014,1,MIL,NL,69,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
streehu01,2014,1,SDN,NL,33,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0

==> salaries-partial.csv <==
yearID,teamID,lgID,playerID,salary
2014,MIL,NL,ramirar01,16000000
2014,SDN,NL,streehu01,7000000
2014,SLN,NL,wainwad01,19500000
2014,PIT,NL,watsoto01,518500
2014,WAS,NL,zimmejo02,7500000

drogers@drogers-mbp:~/Desktop/working
$ java -jar csv-merge.jar -o my-output.csv allstar-partial.csv 3 batting-partial.csv 0 salaries-partial.csv 3
creating sorted and reordered files ..
creating output file:  my-output.csv
drogers@drogers-mbp:~/Desktop/working
$ ls
allstar-partial-tmp.csv  batting-partial-tmp.csv  csv-merge.jar            salaries-partial-tmp.csv
allstar-partial.csv      batting-partial.csv      my-output.csv            salaries-partial.csv
drogers@drogers-mbp:~/Desktop/working
$ cat my-output.csv
gameID,yearID,gameNum,playerID,teamID,lgID,GP,startingPos,yearID,stint,teamID,lgID,G,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP,teamID,lgID,playerID,salary
2014,,,,,,,,,,,,,,,,,,,,,,,,,,,,,MIL,NL,ramirar01,16000000
2014,,,,,,,,,,,,,,,,,,,,,,,,,,,,,SDN,NL,streehu01,7000000
2014,,,,,,,,,,,,,,,,,,,,,,,,,,,,,SLN,NL,wainwad01,19500000
2014,,,,,,,,,,,,,,,,,,,,,,,,,,,,,PIT,NL,watsoto01,518500
2014,,,,,,,,,,,,,,,,,,,,,,,,,,,,,WAS,NL,zimmejo02,7500000
ALS201407150,2014,0,zimmejo02,WAS,NL,0, ,,,,,,,,,,,,,,,,,,,,,,,,,
ALS201407150,2014,0,streehu01,SDN,NL,0,,,,,,,,,,,,,,,,,,,,,,,,,,
ALS201407150,2014,0,rossty01,SDN,NL,0,,,,,,,,,,,,,,,,,,,,,,,,,,
ALS201407150,2014,0,wainwad01,SLN,NL,1,,,,,,,,,,,,,,,,,,,,,,,,,,
ALS201407150,2014,0,rizzoan01,CHN,NL,1,,,,,,,,,,,,,,,,,,,,,,,,,,
wainwad01,,,,,,,,2014,1,SLN,NL,32,72,4,13,3,0,0,6,0,0,3,22,0,0,7,0,0,,,,
rossty01,,,,,,,,2014,1,SDN,NL,32,56,6,10,1,0,0,3,0,0,3,22,0,1,2,1,0,,,,
watsoto01,,,,,,,,2014,1,PIT,NL,78,3,0,1,0,0,0,0,0,0,0,2,0,0,0,0,0,,,,
rodrifr03,,,,,,,,2014,1,MIL,NL,69,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,,,,
streehu01,,,,,,,,2014,1,SDN,NL,33,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,,,,


## License

Copyright © 2015 

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
