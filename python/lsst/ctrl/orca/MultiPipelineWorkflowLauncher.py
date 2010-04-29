import os, sys, subprocess
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher
from lsst.ctrl.orca.SinglePipelineWorkflowMonitor import SinglePipelineWorkflowMonitor

class MultiPipelineWorkflowLauncher(WorkflowLauncher):
    ##
    # @brief
    #
    def __init__(self, cmds, prodPolicy, wfPolicy, logger = None):
        logger.log(Log.DEBUG, "MultiPipelineWorkflowLauncher:__init__")
        self.logger = logger
        self.cmds = cmds
        self.wfPolicy = wfPolicy
        self.prodPolicy = prodPolicy

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "MultiPipelineWorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener):
        self.logger.log(Log.DEBUG, "MultiPipelineWorkflowLauncher:launch")

        for key in self.cmds.keys():
            cmd = self.cmds[key]
            pid = os.fork()
            if not pid:
                os.execvp(cmd[0], cmd)
            os.wait()[0]

        eventBrokerHost = self.prodPolicy.get("eventBrokerHost")
        shutdownTopic = self.wfPolicy.get("shutdownTopic")

        self.workflowMonitor = SinglePipelineWorkflowMonitor(eventBrokerHost, shutdownTopic, self.logger)
        if statusListener != None:
            self.workflowMonitor.addStatusListener(statusListener)
        self.workflowMonitor.startMonitorThread()
        return self.workflowMonitor
