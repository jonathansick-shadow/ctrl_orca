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
import lsst.ctrl.orca as orca
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher
from lsst.ctrl.orca.CondorJobs import CondorJobs
from lsst.ctrl.orca.CondorWorkflowMonitor import CondorWorkflowMonitor

class CondorWorkflowLauncher(WorkflowLauncher):
    ##
    # @brief
    #
    def __init__(self, prodConfig, wfConfig, runid, localStagingDir, dagFile, logger = None):
        if logger != None:
            logger.log(Log.DEBUG, "CondorWorkflowLauncher:__init__")
        self.prodConfig = prodConfig
        self.wfConfig = wfConfig
        self.runid = runid
        self.localStagingDir = localStagingDir
        self.dagFile = dagFile
        self.logger = logger

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "CondorWorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener, loggerManagers):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "CondorWorkflowLauncher:launch")

        # start the monitor first, because we want to catch any pipeline
        # events that might be sent from expiring pipelines.
        eventBrokerHost = self.prodConfig.production.eventBrokerHost
        shutdownTopic = self.prodConfig.production.productionShutdownTopic


        # Launch process
        startDir = os.getcwd()
        os.chdir(self.localStagingDir)

        cj = CondorJobs(self.logger)
        condorDagId = cj.condorSubmitDag(self.dagFile)
        print "Condor dag submitted as job ",condorDagId
        os.chdir(startDir)

        self.workflowMonitor = CondorWorkflowMonitor(eventBrokerHost, shutdownTopic, self.runid, condorDagId, loggerManagers, self.logger)
        if statusListener != None:
            self.workflowMonitor.addStatusListener(statusListener)
        self.workflowMonitor.startMonitorThread(self.runid)



    

        return self.workflowMonitor
