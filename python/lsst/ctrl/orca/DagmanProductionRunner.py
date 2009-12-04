import os
##
# @brief launches pipelines
#
class DagmanProductionRunner:
    def __init__(self, runid, nodeCount, pipelineManagers):
        self.runid = runid
        self.nodeCount = nodeCount
        self.pipelineManagers = pipelineManagers
        self.donotrun = False
        self.verbose = True

    def runPipelines(self):

        # perform the glide-in

        cmd = "condor_glidein -count %d -setup_jobmanager=jobmanager-fork -arch=7.4.0-i686-pc-Linux-2.4 -idletime 5 grid-abe.ncsa.teragrid.org/jobmanager-pbs" % self.nodeCount
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

        tmpdir = os.path.join("/tmp",self.runid)
        dagmanFile = os.path.join(tmpdir,"dagman_"+self.runid+".dag")

        # launch the submit
        cmd = "condor_submit_dag "+dagmanFile
        print "running cmd = "+cmd

        if self.verbose == True:
            print "would run: "+cmd
        if self.donotrun == False:
            cmdArray = cmd.split()
            pid = os.fork()
            if not pid:
               os.execvp(cmdArray[0],cmdArray)
            os.wait()[0]
