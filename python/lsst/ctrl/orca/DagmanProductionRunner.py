##
# @brief launches pipelines
#
class DagmanProductionRunner:
    def __init__(self, pipelineManagers):
        self.pipelineManagers = pipelineManagers

    def launchPipelines(self):

        cmd = "condor_glidein -count "+nodeCount+" -setup_jobmanager=jobmanager-fork -arch=7.4.0-i686-pc-Linux-2.4 -idletime 5 grid-abe.ncsa.teragrid.org/jobmanager-pbs"
        cmdArray = cmd.split()
        pid = os.fork()
        if not pid:
           os.execvp(cmdArray[0],cmdArray)
        os.wait()[0]

        daglist = []
    
        job = 1
        for pipelineManager in self.pipelineManagers:
            datlist.append("JOB A"+str(cnt)+" "+pipelineManager.getLaunchFile())
            job = job + 1

        for pipelineManager in self.pipelineManagers:
            pipelineManager.runPipeline()
