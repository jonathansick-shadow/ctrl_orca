import subprocess
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor

class WorkflowLauncher:
    ##
    # @brief
    #
    def __init__(self, logger, workflowPolicy):
        logger.log(Log.DEBUG, "WorkflowLauncher:__init__")
        self.logger = logger
        self.workflowPolicy = workflowPolicy

    ##
    # @brief launch this workflow
    #
    def launch(self):
        self.logger.log(Log.DEBUG, "WorkflowLauncher:launch")

        self.workflowMonitor = WorkflowMonitor(self.logger, workflowPolicy)
        return self.workflowMonitor # returns WorkflowMonitor

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "WorkflowLauncher:cleanUp")

    ##
    # @brief perform checks on validity of configuration of this workflow
    #
    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "WorkflowLauncher:checkConfiguration")
