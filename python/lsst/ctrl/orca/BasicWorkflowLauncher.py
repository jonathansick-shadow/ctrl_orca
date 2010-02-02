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
class BasicWorkflowLauncher(WorkflowLauncher):
    def __init__(self, cmd, workflow, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "BasicWorkflowLauncher:__init__")
        print "cmd = ",cmd
        self.cmd = cmd
        self.workflow = workflow


    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "BasicWorkflowLauncher:cleanUp")

    ##
    # @brief perform checks on validity of configuration of this workflow
    #
    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "BasicWorkflowLauncher:checkConfiguration")

    ##
    # @brief launch this workflow
    #
    def launch(self):
        self.logger.log(Log.DEBUG, "BasicWorkflowLauncher:launch")

        if orca.dryrun == True:
            print "dryrun: would execute"
            print self.cmd
        else:
            self.logger.log(Log.DEBUG, "launching workflow")

            # by convention the first node in the list is the "master" node
                       
            self.logger.log(Log.DEBUG, "executing: " + " ".join(self.cmd))

            print  "executing: ",self.cmd
            
            #cmdArray = self.cmd.split()
            # perform this copy from the local machine to the remote machine
            pid = os.fork()
            if not pid:
               os.execvp(self.cmd[0], self.cmd)
            os.wait()[0]

            #if subprocess.call(self.cmd) != 0:
            #    raise RuntimeError("Failed to launch " + self.workflow)

        self.workflowMonitor = WorkflowMonitor(self.logger)
        return self.workflowMonitor # returns WorkflowMonitor
