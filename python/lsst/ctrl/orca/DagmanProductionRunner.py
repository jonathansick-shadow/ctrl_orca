import os
from lsst.ctrl.orca.ProductionRunner import ProductionRunner

##
# @brief launches workflows
#
class DagmanProductionRunner(ProductionRunner):
    def __init__(self, runid, policy, workflowManagers):
        self.runid = runid
        self.policy = policy
        self.workflowManagers = workflowManagers
        self.donotrun = False
        self.verbose = True

    def runWorkflows(self):

        # perform the glide-in

        nodeCount = self.policy.get("productionRunner.nodeCount")
        idleTime = self.policy.get("productionRunner.idleTime")
        queueName = self.policy.get("productionRunner.queueName")
        localScratch = self.policy.get("localScratch")
        arch = self.policy.get("productionRunner.arch")

        tmpdir = os.path.join(localScratch,self.runid)
        os.chdir(tmpdir)

        #cmd = "condor_glidein -count %d -setup_jobmanager=jobmanager-fork -arch=7.4.0-i686-pc-Linux-2.4 -idletime %d %s" % (nodeCount, idleTime, queueName)
        cmd = "condor_glidein -count %d -setup_jobmanager=jobmanager-fork -arch=%s -idletime %d %s" % (nodeCount, arch, idleTime, queueName)

        if self.verbose == True:
            print "running cmd = "+cmd
        if self.donotrun == False:
            cmdArray = cmd.split()
            pid = os.fork()
            if not pid:
               os.execvp(cmdArray[0],cmdArray)
            os.wait()[0]


        # get the name of the already created "dagman_<runid>.dag" file

        dagmanFile = os.path.join(tmpdir,"dagman_"+self.runid+".dag")

        # launch the submit
        cmd = "condor_submit_dag "+dagmanFile
        print "running cmd = "+cmd

        if self.donotrun == False:
            cmdArray = cmd.split()
            pid = os.fork()
            if not pid:
               os.execvp(cmdArray[0],cmdArray)
            os.wait()[0]
