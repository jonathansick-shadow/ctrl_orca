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

import os
from lsst.ctrl.orca.LoggerManager import LoggerManager
from lsst.ctrl.orca.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.WorkflowManager import WorkflowManager
from lsst.ctrl.orca.config.ProductionConfig import ProductionConfig
from lsst.pex.logging import Log
import lsst.pex.config as pexConfig
from lsst.ctrl.provenance.ProvenanceSetup import ProvenanceSetup
import lsst.pex.exceptions as pexEx

class ProductionRunConfigurator:
    ##
    # @brief create a basic production run.
    # Note that all ProductionRunConfigurator subclasses must support this
    # constructor signature.
    def __init__(self, runid, configFile, repository=None, logger=None, workflowVerbosity=None):

        # the logger used by this instance
        if not logger:
            logger = Log.getDefaultLogger()
        self.parentLogger = logger
        self.logger = Log(logger, "config")

        logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")

        self.runid = runid
        self._prodConfigFile = configFile

        self.prodConfig = ProductionConfig()
        self.prodConfig.load(configFile)

        self.repository = repository
        self.workflowVerbosity = workflowVerbosity
        self._provSetup = None

        self.provenanceDict = {}
        self._wfnames = None

        # cache the database configurators for checking the configuraiton.  
        self._databaseConfigurators = []

        # logger managers
        self._loggerManagers = []

        self.eventBrokerHost = None

        # these are config settings which can be overriden from what they
        # are in the workflow policies.
        self.configOverrides = dict()
        print "----"
        print self.prodConfig.production
        print "----"
        production = self.prodConfig.production
        if production.eventBrokerHost != None:
            self.eventBrokerHost = production.eventBrokerHost
            self.configOverrides["execute.eventBrokerHost"] = production.eventBrokerHost
        if production.logThreshold != None:
            self.configOverrides["execute.logThreshold"] = production.logThreshold
        if production.productionShutdownTopic != None:
            self.configOverrides["execute.shutdownTopic"] = production.productionShutdownTopic

    ##
    # @brief create the WorkflowManager for the pipelien with the given shortName
    #
    def createWorkflowManager(self, prodConfig, wfName, wfConfig):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createWorkflowManager")

        wfManager = WorkflowManager(wfName, self.runid, self.repository, prodConfig, wfConfig, self.logger)
        return wfManager

    ##
    # @brief return provenanceSetup
    #
    def getProvenanceSetup(self):
        return self._provSetup

    ##
    # @brief configure this production run
    #
    def configure(self, workflowVerbosity):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")

        # TODO - IMPORTANT - NEXT TWO LINES ARE FOR PROVENANCE
        # --------------
        #self._provSetup = ProvenanceSetup()
        #self._provSetup.addAllProductionConfigFiles(self._prodConfigFile, self.repository)
        # --------------
            
        #
        # setup the database for each database listed in production config.
        # cache the configurators in case we want to check the configuration
        # later. 
        #
        databaseConfigNames = self.prodConfig.databaseConfigNames
        databaseConfigs = self.prodConfig.database

        for databaseName in databaseConfigNames:
            databaseConfig = databaseConfigs[databaseName]
            cfg = self.createDatabaseConfigurator(databaseConfig)
            cfg.setup(self._provSetup)
            dbInfo = cfg.getDBInfo()
            # check to see if we're supposed to launch a logging daemon
            if databaseConfig.logger != None:
                loggerConfig = databaseConfig.logger
                if loggerConfig.launch != None:
                    launch = loggerConfig.launch
                    if launch == True:
                        loggerManager = LoggerManager(self.logger, self.eventBrokerHost, dbInfo["host"], dbInfo["port"], self.runid, dbInfo["dbrun"])
                    self._loggerManagers.append(loggerManager)
            self._databaseConfigurators.append(cfg)


        #
        # do specialized production level configuration, if it exists
        #
        if self.prodConfig.production.configuration.configurationClass != None:
            specialConfigurationConfig = self.prodConfig.production.configuration
            # XXX - specialConfigurationConfig maybe?
            self.specializedConfigure(specialConfigurationConfig)
        

        workflowNames = self.prodConfig.workflowNames
        workflowConfigs = self.prodConfig.workflow
        workflowManagers = []
        for wfName in workflowNames:
            wfConfig = workflowConfigs[wfName]
            # copy in appropriate production level info into workflow Node  -- ?

            workflowManager = self.createWorkflowManager(self.prodConfig, wfName, wfConfig)
            workflowLauncher = workflowManager.configure(self._provSetup, workflowVerbosity)
            workflowManagers.append(workflowManager)

        return workflowManagers

    def getLoggerManagers(self):
        return self._loggerManagers

    ##
    # @brief carry out production-wide configuration checks.
    # @param care      the thoroughness of the checks.
    # @param issueExc  an instance of MultiIssueConfigurationError to add 
    #                   problems to.  If not None, this function will not 
    #                   raise an exception when problems are encountered; they
    #                   will merely be added to the instance.  It is assumed
    #                   that the caller will raise that exception is necessary.
    #
    def checkConfiguration(self, care=1, issueExc=None):
        self.logger.log(Log.DEBUG, "checkConfiguration")
        myProblems = issueExc
        if myProblems is None:
            myProblems = MultiIssueConfigurationError("problems encountered while checking configuration")

        for dbconfig in self._databaseConfigurators:
            print "-> dbconfig = ",dbconfig
            dbconfig.checkConfiguration(care, issueExc)
        
        if not issueExc and myProblems.hasProblems():
            raise myProblems

    ##
    # @brief lookup and create the configurator for database operations
    #
    def createDatabaseConfigurator(self, databaseConfig):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createDatabaseConfigurator")
        className = databaseConfig.configurationClass
        classFactory = NamedClassFactory()
        configurationClass = classFactory.createClass(className)
        configurator = configurationClass(self.runid, databaseConfig, self.prodConfig, None, self.logger)
        return configurator

    ##
    # @brief do any production-wide setup not covered by the setup of the
    # databases or the individual workflows.
    #
    # This implementation does nothing.  Subclasses may override this method
    # to provide specialized production-wide setup.  
    #
    def _specializedConfigure(self, specialConfigurationConfig):
        pass

    ##
    # @brief return the workflow names to be used for this set of workflows
    #
    def getWorkflowNames(self):
        return self.prodConfig.workflowNames
