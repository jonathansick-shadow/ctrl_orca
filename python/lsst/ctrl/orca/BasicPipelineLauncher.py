import subprocess
import lsst.ctrl.orca as orca

from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PipelineMonitor import PipelineMonitor

class BasicPipelineLauncher:
    def __init__(self, cmd, pipeline, logger, verbosity):
        self.logger = logger
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:__init__")
        self.cmd = cmd
        self.pipeline = pipeline
        self.pipelineVerbosity = verbosity


    def launch(self):
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:launch")
        self.launchPipeline()

        self.pipelineMonitor = PipelineMonitor(self.logger)
        return self.pipelineMonitor # returns PipelineMonitor

    def cleanUp(self):
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:cleanUp")

    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:checkConfiguration")

    def launchPipeline(self):

        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:launchPipeline")

        if orca.dryrun == True:
            print "dryrun: would execute"
            print self.cmd
        else:
            self.logger.log(Log.DEBUG, "launching pipeline")

            # by convention the first node in the list is the "master" node
                       
            self.logger.log(Log.DEBUG, "executing: " + " ".join(self.cmd))

            if subprocess.call(self.cmd) != 0:
                raise RuntimeError("Failed to launch " + self.pipeline)
