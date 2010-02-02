import subprocess
import lsst.ctrl.orca as orca

from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PipelineMonitor import PipelineMonitor
from lsst.ctrl.orca.PipelineLauncher import PipelineLauncher

##
# TODO: THIS IS GOING AWAY....We can use BasicPipelineLauncher instead
#

##
# @brief used to launch on a local cluster
#
class AbePipelineLauncher(PipelineLauncher):
    def __init__(self, cmd, pipeline, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "AbePipelineLauncher:__init__")
        self.cmd = cmd
        self.pipeline = pipeline


    ##
    # @brief perform cleanup after pipeline has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "AbePipelineLauncher:cleanUp")

    ##
    # @brief perform checks on validity of configuration of this pipeline
    #
    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "AbePipelineLauncher:checkConfiguration")

    ##
    # @brief launch this pipeline
    #
    def launch(self):
        self.logger.log(Log.DEBUG, "AbePipelineLauncher:launch")

        if orca.dryrun == True:
            print "dryrun: would execute"
            print self.cmd
            self.pipelineMonitor = PipelineMonitor(self.logger)
            return self.pipelineMonitor # returns PipelineMonitor
            return
        self.logger.log(Log.DEBUG, "launching pipeline")

        # by convention the first node in the list is the "master" node
                       
        self.logger.log(Log.INFO, "Submitting Condor job...")

        pid = os.fork()
        if not pid:
            os.execvp("condor_submit", cmd.split())
        os.wait()[0]
        self.logger.log(Log.INFO, "Condor job submitted.")

        self.pipelineMonitor = PipelineMonitor(self.logger)
        return self.pipelineMonitor # returns PipelineMonitor
