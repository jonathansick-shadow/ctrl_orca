import os, sys, subprocess
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.CondorWorkflowMonitor import CondorWorkflowMonitor
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher

class CondorWorkflowLauncher(WorkflowLauncher):
    ##
    # @brief
    #
    def __init__(self, cmd, prodPolicy, wfPolicy, logger = None):
        logger.log(Log.DEBUG, "CondorWorkflowLauncher:__init__")
        self.logger = logger
        self.cmd = cmd
        self.prodPolicy = prodPolicy
        self.wfPolicy = wfPolicy

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "CondorWorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener):
        self.logger.log(Log.DEBUG, "CondorWorkflowLauncher:launch")

        print "self.cmd = ",self.cmd
        pid = os.fork()
        if not pid:
            os.execvp(self.cmd[0], self.cmd)
        os.wait()[0]

        eventBrokerHost = self.prodPolicy.get("eventBrokerHost")
        shutdownTopic = self.wfPolicy.get("shutdownTopic")

        self.workflowMonitor = CondorWorkflowMonitor(eventBrokerHost, shutdownTopic, self.logger)
        if statusListener != None:
            self.workflowMonitor.addStatusListener(statusListener)
        self.workflowMonitor.startMonitorThread()
        return self.workflowMonitor
