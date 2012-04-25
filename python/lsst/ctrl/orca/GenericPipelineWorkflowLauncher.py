# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import os, sys, subprocess
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher
from lsst.ctrl.orca.GenericPipelineWorkflowMonitor import GenericPipelineWorkflowMonitor

class GenericPipelineWorkflowLauncher(WorkflowLauncher):
    ##
    # @brief
    #
    def __init__(self, cmds, prodConfig, wfConfig, runid, fileWaiter, pipelineNames, logger = None):
        if logger != None:
            logger.log(Log.DEBUG, "GenericPipelineWorkflowLauncher:__init__")
        self.logger = logger
        self.cmds = cmds
        self.wfConfig = wfConfig
        self.prodConfig = prodConfig
        self.runid = runid
        self.fileWaiter = fileWaiter
        self.pipelineNames = pipelineNames

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "GenericPipelineWorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener, loggerManagers):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "GenericPipelineWorkflowLauncher:launch")

        eventBrokerHost = self.prodConfig.production.eventBrokerHost
        shutdownTopic = self.wfConfig.shutdownTopic

        # listen on this topic for "workers" sending messages

        # start the monitor first, because we want to catch any pipeline
        # events that might be sent from expiring pipelines.

        self.workflowMonitor = GenericPipelineWorkflowMonitor(eventBrokerHost, shutdownTopic, self.runid, self.pipelineNames, loggerManagers, self.logger)
        if statusListener != None:
            self.workflowMonitor.addStatusListener(statusListener)
        # start the thread
        self.workflowMonitor.startMonitorThread(self.runid)

        firstJob = True
        for key in self.cmds:
            cmd = key
            pid = os.fork()
            if not pid:
                os.execvp(cmd[0], cmd)
            if firstJob == True:
                self.fileWaiter.waitForFirstFile()
                firstJob = False

            # commented out - don't wait for it to end
            #os.wait()[0]

        self.fileWaiter.waitForAllFiles()

        return self.workflowMonitor
