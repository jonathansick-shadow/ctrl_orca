import subprocess
import lsst.ctrl.orca as orca

from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher

##
# TODO: THIS IS GOING AWAY....We can use BasicWorkflowLauncher instead
#

##
# @brief used to launch on a local cluster
#
class AbeWorkflowLauncher(WorkflowLauncher):
    def __init__(self, cmd, workflow, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "AbeWorkflowLauncher:__init__")
        self.cmd = cmd
        self.workflow = workflow


    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "AbeWorkflowLauncher:cleanUp")

    ##
    # @brief perform checks on validity of configuration of this workflow
    #
    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "AbeWorkflowLauncher:checkConfiguration")

    ##
    # @brief launch this workflow
    #
    def launch(self):
        self.logger.log(Log.DEBUG, "AbeWorkflowLauncher:launch")

        if orca.dryrun == True:
            print "dryrun: would execute"
            print self.cmd
            self.workflowMonitor = WorkflowMonitor(self.logger)
            return self.workflowMonitor # returns WorkflowMonitor
            return
        self.logger.log(Log.DEBUG, "launching workflow")

        # by convention the first node in the list is the "master" node
                       
        self.logger.log(Log.INFO, "Submitting Condor job...")

        pid = os.fork()
        if not pid:
            os.execvp("condor_submit", cmd.split())
        os.wait()[0]
        self.logger.log(Log.INFO, "Condor job submitted.")

        self.workflowMonitor = WorkflowMonitor(self.logger)
        return self.workflowMonitor # returns WorkflowMonitor
