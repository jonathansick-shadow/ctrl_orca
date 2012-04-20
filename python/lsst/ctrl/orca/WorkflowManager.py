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

from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.StatusListener import StatusListener
from lsst.pex.logging import Log
import lsst.pex.config as pexConfig
from lsst.ctrl.orca.multithreading import SharedData
from lsst.ctrl.orca.DataAnnouncer import DataAnnouncer

class WorkflowManager:
    ##
    # @brief 
    #
    def __init__(self, name, runid, repository, prodConfig, wfConfig, logger=None):

        # _locked: a container for data to be shared across threads that 
        # have access to this object.
        self._locked = SharedData(False)

        if name != None:
            self.name = name
        else:
            self.name = "unnamed"
        self.runid = runid
        self.repository = repository
        self.wfConfig = wfConfig
        self.prodConfig = prodConfig
        self._workflowConfigurator = None

        # the logger used by this instance
        if not logger:
            logger = Log.getDefaultLogger()
        self.logger = Log(logger, "workflow")

        self.logger.log(Log.DEBUG, "WorkflowManager:__init__")

        self.urgency = 0
        self._launcher = None
        self._monitor = None

    ##
    # @brief return the name of this workflow
    #
    def getName(self):
        return self.name

    ##
    # @brief setup, launch and monitor a workflow to its completion, and then
    #            clean-up.
    #
    def runWorkflow(self, statusListener, loggerManagers):
        self.logger.log(Log.DEBUG, "WorkflowManager:runWorkflow")

        if not self.isRunnable():
            if self.isRunning():
                self.logger.log(Log.INFO, "Workflow %s is already running" % self.runid)
            if self.isDone():
                self.logger.log(Log.INFO,
                                "Workflow %s has already run; start with new runid" % self.runid)
            return False

        try:
            self._locked.acquire()

            if self._workflowConfigurator == None:
                self._workflowLauncher = self.configure()
            self._monitor = self._workflowLauncher.launch(statusListener, loggerManagers)
            
            # self.cleanUp()

        finally:
            self._locked.release()
        return self._monitor

    ##
    # @brief stop the workflow.
    #
    def stopWorkflow(self, urgency):
        self.logger.log(Log.DEBUG, "WorkflowManager:stopWorkflow")
        if self._monitor:
            self._monitor.stopWorkflow(urgency)
        else:
            self.logger.log(Log.INFO, "Workflow %s is not running" % self.name)

    ##
    # @brief carry out post-execution tasks for removing workflow data and
    #            state from the platform and archiving/ingesting products as
    #            needed.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "WorkflowManager:cleanUp")



    ##
    # @brief prepare a workflow for launching.
    # @param provSetup    a provenance setup object to pass to
    #                        DatabaseConfigurator instances 
    # @return WorkflowLauncher
    def configure(self, provSetup=None, workflowVerbosity=None):
        self.logger.log(Log.DEBUG, "WorkflowManager:configure")
        if self._workflowConfigurator:
            self.logger.log(Log.INFO-1, "production has already been configured.")
            return
        
        # lock this branch of code
        try:
            self._locked.acquire()

            self._workflowConfigurator = self.createConfigurator(self.runid, self.repository, self.name, self.wfConfig, self.prodConfig)
            self._workflowLauncher = self._workflowConfigurator.configure(provSetup, workflowVerbosity)
        finally:
            self._locked.release()

        # do specialized workflow level configuration here, this may include
        # calling ProvenanceSetup.getWorkflowCommands()
        return self._workflowLauncher

    ##
    # @brief  create a Workflow configurator for this workflow.
    #
    # @param runid       the production run id 
    # @param wfConfig    the config describing the workflow
    # @param prodConfig  the config describing the overall production.  This
    #                       provides common data (e.g. event broker host)
    #                       that needs to be shared with all pipelines. 
    def createConfigurator(self, runid, repository, wfName, wfConfig, prodConfig):
        self.logger.log(Log.DEBUG, "WorkflowManager:createConfigurator")

        className = wfConfig.configurationClass
        classFactory = NamedClassFactory()
        
        configuratorClass = classFactory.createClass(className)
        configurator = configuratorClass(self.runid, repository, prodConfig, wfConfig, wfName, self.logger) 
        return configurator

    ##
    # @brief determine whether production is currently running
    #
    def isRunning(self):
        if self._monitor:
            return self._monitor.isRunning()
        return False

    ##
    # @brief return True if the workflow has been run to completion.  This will
    #            be true if the workflow has run normally through cleaned up or
    #            if it was stopped and clean-up has been called.
    #
    def isDone(self):
        self.logger.log(Log.DEBUG, "WorkflowManager:isDone")
        if self._monitor:
            return self._monitor.isDone()
        return False

    ##
    # @brief return True if the workflow can still be called.  This may return
    #            False because the workflow has already been run and cannot be
    #            re-run.
    #
    def isRunnable(self):
        self.logger.log(Log.DEBUG, "WorkflowManager:isRunnable")
        return not self.isRunning() and not self.isDone()

    ##
    # @brief Runs checks that ensure that the Workflow has been properly set up.
    # @param care      the thoroughness of the checks.
    # @param issueExc  an instance of MultiIssueConfigurationError to add 
    #                   problems to.  If not None, this function will not 
    #                   raise an exception when problems are encountered; they
    #                   will merely be added to the instance.  It is assumed
    #                   that the caller will raise that exception is necessary.
    #
    def checkConfiguration(self, care=1, issueExc=None):
        # care - an indication of how throughly to check.  In general, a
        # higher number will result in more checks being run.
        self.logger.log(Log.DEBUG, "WorkflowManager:createConfiguration")

        myProblems = issueExc
        if myProblems is None:
            myProblems = MultiIssueConfigurationError("problems encountered while checking configuration")

        # do the checks

        # raise exception if problems found
        if not issueExc and myProblems.hasProblems():
            raise myProblems


    def getWorkflowName(self):
        return self.name

    def getNodeCount(self):
        return self._workflowConfigurator.getNodeCount()

    def announceData(self):
        announcer = DataAnnouncer(self.runid, self.prodConfig, self.wfConfig, self.logger)
        if announcer.announce():
            print "Data announced via config for %s" % self.name
        else:
            print "No data announced for %s.  Waiting for events from external source" % self.name
