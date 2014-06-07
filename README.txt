
Note: Description for data output can be found in results directory

--------------------------- BASIC USAGE ----------------------------

NewFosterbtreeg_URA.c:

    Compile:
    gcc -pthread utils.c NewFosterbtreeg_URA.c -o NewFosterbtreeg_URA

    Run:
     ./NewFosterbtreeg_URA <verbose> w <optimistic> [<dataset_file1> <dataset_file2>]
        - executable is program to run
        - verbose = 1 displays detailed, readable output. verbose = 0 displays CSV output
        - optimistic = 1 uses optimistic searching. optimistic = 0 does not use optimistic searching
        - Dataset_files are source of keys

    Example:
    ./NewFosterbtreeg_URA 0 w 1 smallset/skew04/part01 smallset/skew04/part02 smallset/skew04/part03 smallset/skew04/part04

    The above example builds a tree using four threads and optimistic searching. 
    The output will be formatted for a CSV file. 
    More details about CSV output can be found in results directory.

fileSplitter.c:
    Just a simple program to split a file into two new files
    Used in splitandrename.sh as described below

    Compile:
        gcc fileSplitter.c -o fileSplitter

    Run:
        ./fileSplitter <filename>
    
    Output:
        Two files named <filename>a and <filename>b
        Each will contain half of <filename>

fosterbtreeg_URA.c:
    This is the implementation after removing disk I/O and before any modifications to actual algorithms.
    Just here for reference. No need to use it or anything.

utils.c:
    Dependency for b-tree implementation. Just a toolset to measure time in nanoseconds.
    Not actually super accurate, but good enough for this.

-------------------------- SCRIPT USAGE ----------------------------
compileandtest.sh
    - A simple shell script to compile code
    - Tests against some basic dataset
    - Usage:
        sh compileandtest.sh

fulltest.sh
    - A simple shell script to run all tests
    - Does not automatically update resulting file names
    - Usage:
        nohup sh fulltest.sh > out.txt 2> err.txt &

generagekeys.py
    - Generates a dataset by permuting lowercase characters

    - Usage:
        python ./generatekeys.py <filename>

    - Options:
        --length
            Number of characters that will be permuted. 5 by default
        --letters
            Number of letters in alphabet to use. 26 by default
        --repeats
            Number of times to repeat each key
        --scramble
            Randomize keys. Not yet implemented

    - By default, this generates 26^5 keys with 5 characters each
    - Total number of keys is given by ((num letters)^(string-length))*(repeats + 1)

runtests.py
    - Runs all tests for a given dataset by building and running a bunch of commands
    - Should end up with a csv file of results
    
    - Usage:
        python ./runtests.py <filename>

    - Options:
        -s, --start
            The starting number of threads in test. 1 by default
        -e, --end
            The ending number of threads in test. 32 by default
        --no-sorted
            Ignore sorted versions of dataset
        --no-optimistic
            Do not use optimistic search strategy
        -t, --set-type
            Type of dataset to go through

    - Requires a very strict directory structure to function
    - General directory structure described in DATASET section
    - See implementation for more details

sortthem.sh
    - A simple shell script to sort a bunch of files in a directory
    - Used in workflow when generating a new dataset directory
    - Usage:
        sh sortthem.sh

splitandrename.sh
    - A simple shell script that helps in generating a new dataset directory
    - Needs fileSplitter in same directory
    - Usage:
        sh splitandrename.sh

uploadfiles.sh
    - A simple shell script so I don't have to type out scp commands
    - Specific to user
    - Usage:
        sh uploadfiles.sh

---------------------------- DATASETS ------------------------------

Toy Datasets:
These are just some small datasets to help verify small test cases.

Other Datasets:
Datasets can be generated automatically using generatekeys.py. Otherwise, datasets can be downloaded from http://www.naskitis.com/.

Directory Structure:
    - DatasetName
        - <TypeA>01
            - part01
        - <TypeA>02
            - part01
            - part02
        - <TypeA>04
            - part01
            - part02
            - part03
            - part04
        - <TypeA>08
        ...
        - <TypeA>32
            ...
        - <TypeB>01
            - part01
        - <TypeB>02
            - part01
            - part02
        ...

    - Type A may be "rand" or "skew" or something else
    - Type B is sorted. Called "sort" or "sorted" depending on dataset name
    - See naarcini home directory on manta for an example

    * This is kind of messy right now*

New Dataset Workflow:
Initial Steps:
    - Create new dataset directory
    - Create all subdirectories
    - Copy full dataset to <TypeA>01 and rename to part01
    - Copy full dataset to <TypeB>01 and rename to part01
    - Copy sortthem.sh to <TypeB>01
    - Run:
        sh sortthem.sh
    - Copy fileSplitter and splitandrename.sh to <TypeA>02
    - Copy full dataset to <TypeA>02 and rename to part01
    
Main Steps:
    Note: (xx) is current location
          (yy) is next location

    1. Go to <TypeA>xx and run:
        sh splitandrename.sh
    2. Copy contents of <TypeA>xx to <TypeA>yy if available
    3. Delete fileSplitter and splitandrename.sh from <TypeA>xx
    4. Copy contents of <TypeA>xx to <TypeB>xx
    5. Copy sortthem.sh to <TypeB>xx
    6. Run:
        sh sortthem.sh
    7. Delete sortthem.sh from <TypeB>xx
    8. xx is now next location. Go to step 1 until done

All of this is done manually right now. It would be much better if it were automated.
Main thing is that we should be able to use runtests.py.


--------------------------- ORIGINALS ------------------------------

Directory "Originals" contains unmodified versions of foster b-tree and another high-concurrency tree implementation. Both of these use disk I/O. Usage for these files is also a little bit different. The original author is Karl Malbrain.

Complie:
gcc -pthead <file.c> -o <executablename>

Run:
<executable> <filename.db> <mode> <page_bits> <mapped_segments> <seg_bits> <line_numbers> [<dataset_file1> <dataset_file2> ...]
    - executable is program to run
    - Filename is a new or existing file to which all keys will be written/read
    - Mode is Write (w)/Read (r)/Delete (d)/Scan (s)/Find (f)
        - Dataset files used differently in each case
    - Page_bits is size of page in tree in bits. Ex. 14
    - Mapped_segments is number of mmap segments in buffer pool. Ex. 32768
    - Seg_bits is size of individual segments in buffer pool in pages in bits. Ex. 4
    - Line_numbers = 1 to append line numbers to keys
    - Dataset_files are source of keys

For more info, see https://code.google.com/p/high-concurrency-btree/

