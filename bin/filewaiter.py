#!/usr/bin/env python
import os, os.path, sys, time

# filewaiter.py - wait for creation of files
if __name__ == "__main__":


    cnt = len(sys.argv)
    if cnt == 1:
        print "Need to watch for at least one file"
        print "usage:  python filewatcher [file]+"
        exit(0)

    list = sys.argv[1:]

    while len(list) > 0:
        newlist = [ item for item in list if (os.path.exists(item) == False)]
        list = newlist
        time.sleep(1)
