# An easier way to type commands to upload files to test
# Edit at will
# Usage:
#   sh uploadfiles.sh

scp -i ~/.ssh/manta NewFosterbtreeg_URA.c naarcini@manta.uwaterloo.ca:~/NewFosterbtreeg_URA.c
scp -i ~/.ssh/manta runtests.py naarcini@manta.uwaterloo.ca:~/runtests.py
