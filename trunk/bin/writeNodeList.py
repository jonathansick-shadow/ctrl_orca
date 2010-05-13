#!/usr/bin/env python
import os, os.path, sys
import lsst.pex.policy as pol


# two args: 
# the first one is the working directory, the second the policy 
# filename
work = sys.argv[1]
name = sys.argv[2]
filename = os.path.join(work,name)

# read the policy in and get the count of the number of entries
p = pol.Policy(filename)
num = len(p.paramNames())

# write the "nodelist.scr" file out, preserving the order
nodelist = open(os.path.join(work, "nodelist.scr"), "w")

for x in range(0,num):
    val = p.get("node%d" % x)
    print >> nodelist, val

nodelist.close()
