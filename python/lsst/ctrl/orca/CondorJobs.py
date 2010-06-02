#!/usr/bin/env python

import os
import sys
import re
import time

#
# This class is highly dependent on the output of the condor commands 
# condor_submit and condor_q
#
class CondorJobs:
    def __init__(self):
        return


    # submit a condor file, and return the job number associated with it.
    # expected output:
    # Submitting job(s).
    # Logging submit event(s).
    # 1 job(s) submitted to cluster 1317.
    
    def submitJob(self, condorFile):
        clusterexp = re.compile("1 job\(s\) submitted to cluster (\d+).")
    
        submitRequest = "condor_submit %s" % condorFile
    
        pop = os.popen(submitRequest, "r")
    
        line = pop.readline()
        line = pop.readline()
        line = pop.readline()
        num = clusterexp.findall(line)
        return num[0]
    
    
    
    # wait for a condor job to reach it's run state.
    # expected output:
    #-- Submitter: srp@lsst6.ncsa.uiuc.edu : <141.142.15.103:40900> : lsst6.ncsa.uiuc.edu
    # ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD               
    #1016.0   srp             5/24 09:17   0+00:00:00 I  0   0.0  launch_joboffices_
    #1017.0   srp             5/24 09:18   0+00:00:00 R  0   0.0  launch_joboffices_
    
    def waitForJobToRun(self, num):
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
            time.sleep(1)

    def waitForAllJobsToRun(self, numList):
        queueExp = re.compile("\S+")
        jobList = list(numList)
        while 1:
            pop = os.popen("condor_q", "r")
            while 1:
                line = pop.readline()
                if not line:
                    break
                values = queueExp.findall(line)
                if len(values) == 0:
                    continue
                jobNum = values[0]
                runstate = values[5]
                for jobEntry in jobList:
                    if (jobNum == jobEntry) and (runstate == 'R'):
                        jobList = [job for job in jobList if job[:] != jobNum]
                        pop.close()
                        if len(jobList) == 0:
                            return
                        break
                    else:
                        continue
                    if (jobNum == jobEntry) and (runstate == 'H'):
                        pop.close()
                        # throw exception here
                        return
            pop.close()
            sleep(1)
