import os
##
# @brief launches pipelines
#
class DagmanProductionRunner:
    def __init__(self, runid, policy, pipelineManagers):
        self.runid = runid
        self.policy = policy
        self.pipelineManagers = pipelineManagers
        self.donotrun = False
        self.verbose = True

    def runPipelines(self):

        # perform the glide-in

        nodeCount = self.policy.get("nodeCount")
        idleTime = self.policy.get("idleTime")
        queueName = self.policy.get("queueName")

        tmpdir = os.path.join("/tmp",self.runid)
        os.chdir(tmpdir)

        cmd = "condor_glidein -count %d -setup_jobmanager=jobmanager-fork -arch=7.4.0-i686-pc-Linux-2.4 -idletime %d %s" % (nodeCount, idleTime, queueName)
        print "running cmd = "+cmd

        if self.verbose == True:
            print "would run: "+cmd
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
