import argparse
import sys
import subprocess
import os.path

def checkfiles(fileroot, smallset, filetype, threadnum):
    #Check existence of files
    directory = ""
    if smallset:
        directory += "smallset/"
    else:
        directory += "largeset/"

    directory += filetype + str(threadnum).zfill(2)
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

def buildcommand(noOptimistic, smallset, filetype, threadnum):
    command = "./NewFosterbtreeg_URA 0 w"

    if(noOptimistic):
        command = command + " 0"
    else:
        command = command + " 1"
    
    for partnum in range(1, threadnum+1):
        if(smallset):
            command = command + " smallset/"
        else:
            command = command + " largeset/"

        command = command + filetype + str(threadnum).zfill(2) + "/part" + str(partnum).zfill(2)

    return command


parser = argparse.ArgumentParser(description='Run tests for foster b-tree')
parser.add_argument('outfile', nargs='?', type=str, help='Output file name.')
parser.add_argument('-s', '--start', action='store', default=1, type=int, choices=[1, 2, 4, 8, 16, 32], help='The starting number of threads. Pick 1, 2, 4, 8, 16, or 32')
parser.add_argument('-e', '--end', action='store', default=32, type=int, choices=[1, 2, 4, 8, 16, 32], help='The ending number of threads. Pick 1, 2, 4, 8, 16, or 32')
parser.add_argument('--no-sorted', action='store_true', help='Don\'t include sorted data sets')
parser.add_argument('--no-optimistic', action='store_true', help='Don\'t use optimistic searching')
parser.add_argument('--smallset', action='store_true', help='Use small dataset')

args = parser.parse_args()

if(args.outfile == None):
    print "Filename required. Use argument -h for more options."
    sys.exit()

thread = args.start
endthread = args.end
filename = args.outfile + '.csv'
out = open(filename, 'w')
out.write('Type,Threads,Total-Time,Thread-Id,Root-Read-Wait,Root-Write-Wait,Readlock-Wait,Writelock-wait,Readlock-Aquired,Writelock-Aquired,Readlock-Failed,Writelock-Failed,LowFence-Overwrites,Optimistic-Successes,Optimistic-Failures\n')
FILE_ROOT = os.path.abspath(os.path.dirname(__file__))

# Run tests for different threads
while(thread <= endthread):
    #Check existence of files
    if(checkfiles(FILE_ROOT, args.smallset, "skew", thread)):
            #Execute command and write to file
            #print buildcommand(args.no_optimistic, args.smallset, "skew", thread) 
            print "Running test for skew %(threadnum)d case." % {"threadnum": thread }
            out.write(str(subprocess.check_output(buildcommand(args.no_optimistic, args.smallset, "skew", thread).split())))
            
    
    # Check if we should also do sorted values
    if not(args.no_sorted):
        if(checkfiles(FILE_ROOT, args.smallset, "sorted", thread)):
            # Execute command and write to file
            #print buildcommand(args.no_optimistic, args.smallset, "sorted", thread)
            print "Running test for sorted %(threadnum)d case." % {"threadnum": thread }
            out.write(str(subprocess.check_output(buildcommand(args.no_optimistic, args.smallset, "sorted", thread).split())))

    thread *= 2

