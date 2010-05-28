#!/usr/bin/env python

import os
import sys
import re

class CondorJob:
    def __init__(self):
        return
    #Submitting job(s).
    #Logging submit event(s).
    #1 job(s) submitted to cluster 1317.
    
    def submitJob(condorFile):
        clusterexp = re.compile("1 job\(s\) submitted to cluster (\d+).")
    
        submitRequest = "condor_submit %s" % condorFile
    
        pop = os.popen(submitRequest, "r")
    
        line = pop.readline()
        line = pop.readline()
        line = pop.readline()
        num = clusterexp.findall(line)
        return num[0]
    
    
    
    #-- Submitter: srp@lsst6.ncsa.uiuc.edu : <141.142.15.103:40900> : lsst6.ncsa.uiuc.edu
    # ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD               
    #1016.0   srp             5/24 09:17   0+00:00:00 I  0   0.0  launch_joboffices_
    #1017.0   srp             5/24 09:18   0+00:00:00 R  0   0.0  launch_joboffices_
    
    def waitForJobToRun(num):
        jobNum = "%s.0" % num
        queueExp = re.compile("\S+")
        while 1:
            pop = os.popen("condor_q", "r")
            while 1:
                line = pop.readline()
                if not line:
                    break
                values = queueExp.findall(line)
                if len(values) == 0:
                    continue
                runstate = values[5]
                if (values[0] == jobNum) and (runstate == 'R'):
                    print values
                    pop.close()
                    return
                if (values[0] == jobNum) and (runstate == 'H'):
                    pop.close()
                    # throw exception here
                    return
            pop.close()
