import argparse
import sys
import subprocess
import os.path

# This script is intended to build a set of commands to automatically test a bunch of b-tree cases
# It works specifically with the file structure found on naarcini's home directory on manta
# Currently set up to test from 1 to 32 threads on 3 separate files labelled smallset, largeset, and distinctset

# For example the command to test 2 threaded insertion on a small skew workload using optimistic searching is:
# ./NewFosterbtreeg_URA 0 w 1 smallset/skew02/part01 smallset/skew02/part02

# Output of tests will be put into a csv file in current directory as specified by user

def checkfiles(fileroot, set_type, filetype, threadnum):
    #Check existence of files
    directory = set_type + "/"

    # This set does not actually have a skewed workload. It is more accurate to say random
    if (set_type == 'distinctset'):
        if (filetype == 'skew'):
            directory += "rand"
        else:
            directory += "sort"
    else:
        directory += filetype

    # Ensure to use 2 digitsn for file name
    directory += str(threadnum).zfill(2)
    dirExist = os.path.isdir(os.path.join(fileroot, directory))
    fileExist = True

    if(dirExist):
        for partnum in range(1, threadnum+1):
            if not(os.path.isfile(os.path.join(fileroot, directory, "part" + str(partnum).zfill(2)))):
                fileExist = False
                break
        
    if not(fileExist and dirExist):
        print "Error, structure for directory %(dirname)s invalid. Skipping %(threadnum)d thread %(filetype)s case." % {"dirname": directory, "threadnum": thread, "filetype": filetype}

    return fileExist and dirExist

def buildcommand(noOptimistic, set_type, filetype, threadnum):
    # By default, only output numbers for csv format, not full verbose output
    command = "./NewFosterbtreeg_URA 0 w"

    if(noOptimistic):
        command += " 0"
    else:
        command += " 1"
    
    for partnum in range(1, threadnum+1):
        command += " " + set_type + "/"

        # Poorly designed separation between labels in distinctset and others
        if (set_type == 'distinctset'):
            if (filetype == 'skew'):
                command += "rand"
            else:
                command += "sort"
        else:
            command += filetype

        command += str(threadnum).zfill(2) + "/part" + str(partnum).zfill(2)

    return command


parser = argparse.ArgumentParser(description='Run tests for foster b-tree')
parser.add_argument('outfile', nargs='?', type=str, help='Output file name.')
parser.add_argument('-s', '--start', action='store', default=1, type=int, choices=[1, 2, 4, 8, 16, 32], help='The starting number of threads. Pick 1, 2, 4, 8, 16, or 32')
parser.add_argument('-e', '--end', action='store', default=32, type=int, choices=[1, 2, 4, 8, 16, 32], help='The ending number of threads. Pick 1, 2, 4, 8, 16, or 32')
parser.add_argument('--no-sorted', action='store_true', help='Don\'t include sorted data sets')
parser.add_argument('--no-optimistic', action='store_true', help='Don\'t use optimistic searching')
parser.add_argument('-t', '--set-type', action='store', type=str, default='largeset', choices=['largeset', 'smallset', 'distinctset'], help='Select type of dataset')

args = parser.parse_args()

if(args.outfile == None):
    print "Filename required. Use argument -h for more options."
    sys.exit()

thread = args.start
endthread = args.end
filename = args.outfile + '.csv'
out = open(filename, 'w')
out.write('Type,Threads,Total-Time,Thread-Id,Thread-Time,Root-Read-Wait,Root-Write-Wait,Readlock-Wait,Writelock-wait,Readlock-Aquired,Writelock-Aquired,Readlock-Failed,Writelock-Failed,LowFence-Overwrites,Optimistic-Successes,Optimistic-Failures\n')
FILE_ROOT = os.path.abspath(os.path.dirname(__file__))

# Run tests for different threads
while(thread <= endthread):
    
    #Check existence of files
    if(checkfiles(FILE_ROOT, args.set_type, "skew", thread)):
            #Execute command and write to file
            #print buildcommand(args.no_optimistic, args.set_type, "skew", thread) 
            print "Running test for skew %(threadnum)d case." % {"threadnum": thread }
            out.write(str(subprocess.check_output(buildcommand(args.no_optimistic, args.set_type, "skew", thread).split())))
            
    
    # Check if we should also do sorted values
    if not(args.no_sorted):
        if(checkfiles(FILE_ROOT, args.set_type, "sorted", thread)):
            # Execute command and write to file
            #print buildcommand(args.no_optimistic, args.set_type, "sorted", thread)
            print "Running test for sorted %(threadnum)d case." % {"threadnum": thread }
            out.write(str(subprocess.check_output(buildcommand(args.no_optimistic, args.set_type, "sorted", thread).split())))

    thread *= 2

