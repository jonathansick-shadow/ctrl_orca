import subprocess
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PipelineMonitor import PipelineMonitor

class PipelineLauncher:
    ##
    # @brief
    #
    def __init__(self, cmd, pipeline, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "PipelineLauncher:__init__")
        self.cmd = cmd
        self.pipeline = pipeline

    ##
    # @brief launch this pipeline
    #
    def launch(self):
        self.logger.log(Log.DEBUG, "PipelineLauncher:launch")

        self.pipelineMonitor = PipelineMonitor(self.logger)
        return self.pipelineMonitor # returns PipelineMonitor

    ##
    # @brief perform cleanup after pipeline has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "PipelineLauncher:cleanUp")

    ##
    # @brief perform checks on validity of configuration of this pipeline
    #
    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "PipelineLauncher:checkConfiguration")
