#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import argparse
import os
import sys

from textwrap import dedent
import glob
import re


def _line_to_args(self, line):
    for arg in shlex.split(line, comments=True, posix=True):
        if not arg.strip():
            continue
        yield arg


def makeArgumentParser(description, inRootsRequired=True, addRegistryOption=True):

    parser = argparse.ArgumentParser(
        description=description,
        fromfile_prefix_chars="@",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=" \n"
               "ly.")
    parser.convert_arg_line_to_args = _line_to_args

    parser.add_argument(
        "-s", "--source", dest="source",
        help="source input file")

    parser.add_argument("dirs", metavar='string', nargs='*')
    return parser

def writePreScriptFile(infile, dirs):
    """
    Write Condor Dag Submission files. 
    """

    ## Loop over input entries 
    fileObj = open(infile,"r")
    count = 0
    visitSet = set()
    for aline in fileObj:
        count+=1
        myData = aline.rstrip()

        # Searching for a space detects 
        # extended input like :  visit=887136081 raft=2,2 sensor=0,1
        # No space is something simple like a skytile id  
        if " " in myData:
            myList  = myData.split(' ');
            visit = myList[0].split('=')[1]
        else:
            newData=myData
            visit = myData

        visitSet.add(visit)

    print "Writing makedirs.sh script"
    outname = "makedirs.sh"
    outObj = open(outname,"w")

    outObj.write("#!/bin/sh\n")
    for dir in dirs:
        outObj.write("mkdir -p "+dir+"\n")

    for visit in visitSet:
        outObj.write("mkdir -p logs/"+str(visit)+"\n")
    outObj.close()


def main():
    print 'Starting genPreScript.py'
    parser = makeArgumentParser(description=
        "genPrescript.py write a prescript shell script that the Condor DAG uses for job submission")
    print 'Created parser'
    ns = parser.parse_args()
    print 'Parsed Arguments'
    print ns

    writePreScriptFile(ns.source, ns.dirs)


    sys.exit(0)







if __name__ == '__main__':
    main()

