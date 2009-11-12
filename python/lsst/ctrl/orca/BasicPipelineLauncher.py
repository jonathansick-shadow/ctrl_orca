import os
import subprocess
import lsst.ctrl.orca as orca

from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PipelineMonitor import PipelineMonitor
from lsst.ctrl.orca.PipelineLauncher import PipelineLauncher

##
# @brief used to launch on a local cluster
#
class BasicPipelineLauncher(PipelineLauncher):
    def __init__(self, cmd, pipeline, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:__init__")
        print "cmd = ",cmd
        self.cmd = cmd
        self.pipeline = pipeline


    ##
    # @brief perform cleanup after pipeline has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:cleanUp")

    ##
    # @brief perform checks on validity of configuration of this pipeline
    #
    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:checkConfiguration")

    ##
    # @brief launch this pipeline
    #
    def launch(self):
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:launch")

        if orca.dryrun == True:
            print "dryrun: would execute"
            print self.cmd
        else:
            self.logger.log(Log.DEBUG, "launching pipeline")

            # by convention the first node in the list is the "master" node
                       
            self.logger.log(Log.DEBUG, "executing: " + " ".join(self.cmd))

            print  "executing: ",self.cmd
            
            cmdArray = self.cmd.split()
            # perform this copy from the local machine to the remote machine
            pid = os.fork()
            if not pid:
               os.execvp(cmdArray[0],cmdArray)
            os.wait()[0]

            #if subprocess.call(self.cmd) != 0:
            #    raise RuntimeError("Failed to launch " + self.pipeline)

        self.pipelineMonitor = PipelineMonitor(self.logger)
        return self.pipelineMonitor # returns PipelineMonitor
