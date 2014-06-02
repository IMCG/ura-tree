# An easier way to test a bunch of different cases
# Edit cases at will
# Remember to update the filename so you don't overwrite your old tests
# This will generally take a long time to run
# Usage:
#   nohup sh fulltest.sh > out.txt 2> err.txt &
#   (Runs in background)

python ./runtests.py Optimistic_small_06 -t smallset 
python ./runtests.py Regular_small_06 -t smallset --no-optimistic
python ./runtests.py Optimistic_large_06  --no-sorted
python ./runtests.py Regular_large_06 --no-sorted --no-optimistic
python ./runtests.py Optimistic_distinct_06 -t distinctset
python ./runtests.py Regular_distinct_06 -t distinctset --no-optimistic
mv Optimistic* results/
mv Regular* results/
