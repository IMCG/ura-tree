# An easier way to quickly check if the newly uploaded program is working
# Edit at will
# Usage:
#   sh compileandtest.sh

gcc -pthread utils.c NewFosterbtreeg_URA.c -o NewFosterbtreeg_URA
./NewFosterbtreeg_URA 0 w 1 testfile1
#./NewFosterbtreeg_URA 0 w 1 smallset/skew04/part01 smallset/skew04/part02 smallset/skew04/part03 smallset/skew04/part04
#./NewFosterbtreeg_URA 0 w 1 smallset/skew08/part01 smallset/skew08/part02 smallset/skew08/part03 smallset/skew08/part04 smallset/skew08/part05 smallset/skew08/part06 smallset/skew08/part07 smallset/skew08/part08
./NewFosterbtreeg_URA 0 w 1 testfile2
