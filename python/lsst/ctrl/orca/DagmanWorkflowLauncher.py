import os
import subprocess
import lsst.ctrl.orca as orca

from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher

##
# @brief used to launch on a local cluster
#
class DagmanWorkflowLauncher(WorkflowLauncher):
    def __init__(self, cmd, workflow, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "DagmanWorkflowLauncher:__init__")
        self.cmd = cmd
        self.workflow = workflow


    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "DagmanWorkflowLauncher:cleanUp")

    ##
    # @brief perform checks on validity of configuration of this workflow
    #
    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "DagmanWorkflowLauncher:checkConfiguration")

    ##
    # @brief launch this workflow
    #
    def launch(self):
        self.logger.log(Log.DEBUG, "DagmanWorkflowLauncher:launch")

        # we don't actually launch anything here.  Dagman does.

        # create a WorkflowMonitor
        self.workflowMonitor = WorkflowMonitor(self.logger)
        return self.workflowMonitor # returns WorkflowMonitor
