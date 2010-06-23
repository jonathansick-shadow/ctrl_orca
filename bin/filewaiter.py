#!/usr/bin/env python
import os, os.path, sys, time
import optparse

# filewaiter.py - wait for creation of files
if __name__ == "__main__":


    usage = """usage: %prog [-f|-l] filenames.txt"""

    parser = optparse.OptionParser(usage)
    # TODO: handle "--dryrun"
    parser.add_option("-f", "--first", action="store_const", const=1, dest="bFirst", help="wait for first file in list")
    parser.add_option("-l", "--list", action="store_const", const=1, dest="bList", help="wait for all the files in the list")

    parser.opts = {}
    parser.args = []

    # parse and check command line arguments
    (parser.opts, parser.args) = parser.parse_args()
    if len(parser.args) < 1:
        print usage
        raise RuntimeError("Missing args: pipelinePolicyFile runId")

    filename = parser.args[0]

    bFirst = parser.opts.bFirst 
    bList = parser.opts.bList
    
    f = open(filename, 'r')

    lines = f.readlines()

    list = []
    for line in lines:
        list.append(line.split('\n')[0])


    if bFirst == True:
        item = list[0]
        while os.path.exists(item) == False:
            time.sleep(1)
        sys.exit(0)

    while len(list) > 0:
        newlist = [ item for item in list if (os.path.exists(item) == False)]
        list = newlist
        time.sleep(1)
    sys.exit(0)
