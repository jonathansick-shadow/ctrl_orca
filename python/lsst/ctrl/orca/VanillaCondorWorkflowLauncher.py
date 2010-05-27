import os, sys, subprocess
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher

class VanillaCondorWorkflowLauncher(WorkflowLauncher):
    ##
    # @brief
    #
    def __init__(self, cmds, wfPolicy, logger = None):
        logger.log(Log.DEBUG, "VanillaCondorWorkflowLauncher:__init__")
        self.logger = logger
        self.cmds = cmds
        self.wfPolicy = wfPolicy

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowLauncher:launch")

        # Three step launch process
        # 1 - deploy condor
        # 2 - Glidein request
        # wait for glidein to complete
        # 3 - start joboffice job
        # wait for joboffice to start
        # 4 - start all other jobs.
    
        # for now, make sure joboffice is the first job, launch and wait for it
        for key in self.cmds:
            cmd = key
            pid = os.fork()
            if not pid:
                os.execvp(cmd[0], cmd)
            os.wait()[0]

        eventBrokerHost = self.prodPolicy.get("eventBrokerHost")
        shutdownTopic = self.wfPolicy.get("shutdownTopic")

        self.workflowMonitor = VanillaCondorWorkflowMonitor(eventBrokerHost, shutdownTopic, self.runid, self.logger)
        if statusListener != None:
            self.workflowMonitor.addStatusListener(statusListener)
        self.workflowMonitor.startMonitorThread(self.runid)
        return self.workflowMonitor
