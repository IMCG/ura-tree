import argparse
import sys
import os.path

def getletter(idx):
    return str(unichr(idx + 97))

parser = argparse.ArgumentParser(description='Generate keys for testing')
parser.add_argument('outfile', nargs='?', type=str, help='Output file name.')
parser.add_argument('--length', action='store', default=5, type=int, choices=range(1, 8), help='Number of characters to permute (total strings = 26^length)')
parser.add_argument('--letters', action='store', default=26, type=int, choices=range(1, 27), help='Use less than full alphabet, if desired')
parser.add_argument('--repeats', action='store', default=0, type=int, help='Number of times to repeat strings for duplicates')
parser.add_argument('--scramble', action='store_true', help='Randomize key order')

args = parser.parse_args()

if(args.outfile == None):
    print "Filename required. Use argument -h for more options."
    sys.exit()

out = open(args.outfile, 'w')
repeats = args.repeats

while (repeats >= 0):
    for a in range(0, args.letters):
        if (args.length == 1):
            out.write(getletter(a) + '\n')
        
        for b in range(0, args.letters):
            if (args.length < 2):
                break
            else:
                out.write(getletter(a) + getletter(b) + '\n')

            for c in range(0, args.letters):
                if (args.length < 3):
                    break
                else:
                    out.write(getletter(a) + getletter(b) + getletter(c) + '\n')

                for d in range(0, args.letters):
                    if (args.length < 4):
                        break
                    else:
                        out.write(getletter(a) + getletter(b) + getletter(c) + getletter(d) + '\n')

                    for e in range(0, args.letters):
                        if (args.length < 5):
                            break
                        else:
                            out.write(getletter(a) + getletter(b) + getletter(c) + getletter(d) + getletter(e) + '\n')

                        for f in range(0, args.letters):
                            if (args.length < 6):
                                break
                            else:
                                out.write(getletter(a) + getletter(b) + getletter(c) + getletter(d) + getletter(e) + getletter(f) + '\n')

                            for g in range(0, args.letters):
                                if (args.length < 7):
                                    break
                                else:
                                    out.write(getletter(a) + getletter(b) + getletter(c) + getletter(d) + getletter(e) + getletter(f) + getletter(g) + '\n')
    repeats -= 1;

if (args.scramble):
    #randomize file
    print 'Scramble not yet implemented'
