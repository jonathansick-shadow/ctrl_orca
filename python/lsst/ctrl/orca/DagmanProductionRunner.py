import os
##
# @brief launches pipelines
#
class DagmanProductionRunner:
    def __init__(self, runid, pipelineManagers):
        self.runid = runid
        self.pipelineManagers = pipelineManagers
        self.donotrun = True
        self.verbose = True

    def runPipelines(self):

        # count the number of nodes we requested
        nodeCount = 0
        for pipelineManager in pipelineManagers:
            nodeCount = nodeCount + pipelineManager.getCPUCount()

        # perform the glide-in

        cmd = "condor_glidein -count "+nodeCount+" -setup_jobmanager=jobmanager-fork -arch=7.4.0-i686-pc-Linux-2.4 -idletime 5 grid-abe.ncsa.teragrid.org/jobmanager-pbs"
        if self.verbose == True:
            print "would run: "+cmd
        if self.donotrun == False:
            cmdArray = cmd.split()
            pid = os.fork()
            if not pid:
               os.execvp(cmdArray[0],cmdArray)
            os.wait()[0]


        # get the name of the already created "dagman_<runid>.dag" file

        tmpdir - os.path.join("/tmp",self.runid)
        dagmanFile = os.path.join(tmpdir,"dagman_"+self.runid+".dag")

        # launch the submit
        cmd = "condor_submit_dag "+dagmanFile

        if self.verbose == True:
            print "would run: "+cmd
        if self.donotrun == False:
            cmdArray = cmd.split()
            pid = os.fork()
            if not pid:
               os.execvp(cmdArray[0],cmdArray)
            os.wait()[0]
