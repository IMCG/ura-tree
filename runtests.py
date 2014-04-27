import argparse
import sys
import subprocess
import os.path

def checkfiles(fileroot, filetype, threadnum):
    #Check existence of files
    directory = filetype + str(threadnum).zfill(2)
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

def buildcommand(filetype, threadnum):
    command = "./NewFosterbtreeg_URA 0 w 0"
    
    for partnum in range(1, threadnum+1):
        command = command + " " + filetype + str(threadnum).zfill(2) + "/part" + str(partnum).zfill(2)

    return command


parser = argparse.ArgumentParser(description='Run tests for foster b-tree')
parser.add_argument('outfile', nargs='?', type=str, help='Output file name.')
parser.add_argument('-s', '--start', action='store', default=1, type=int, choices=[1, 2, 4, 8, 16, 32], help='The starting number of threads. Pick 1, 2, 4, 8, 16, or 32')
parser.add_argument('-e', '--end', action='store', default=32, type=int, choices=[1, 2, 4, 8, 16, 32], help='The ending number of threads. Pick 1, 2, 4, 8, 16, or 32')
parser.add_argument('--no-sorted', action='store_true', help='Don\'t include sorted data sets')

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
    if(checkfiles(FILE_ROOT, "skew", thread)):
            #Execute command and write to file
            #print buildcommand("skew", thread) 
            out.write('skew,')
            out.write(str(subprocess.check_output(buildcommand("skew", thread).split())))
            out.write('\n')
            
    
    # Check if we should also do sorted values
    if not(args.no_sorted):
        if(checkfiles(FILE_ROOT, "sorted", thread)):
            # Execute command and write to file
            #print buildcommand("sorted", thread)
            out.write('sorted,')
            out.write(str(subprocess.check_output(buildcommand("sorted", thread).split())))
            out.write('\n')            

    thread *= 2

