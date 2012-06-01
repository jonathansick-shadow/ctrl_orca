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

from lsst.pex.logging import Log
import lsst.pex.exceptions as pexEx

from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory

##
# @brief an abstract class for configuring a workflow
#
# This class should not be used directly but rather must be subclassed,
# providing an implementation for at least _configureWorkflowLauncher.
# Usually, _configureSpecialized() should also be overridden to carry
# out the non-database setup, including deploying the workflow onto the
# remote platform.
# 
class WorkflowConfigurator:


    class ConfigGroup(object):
        def __init__(self, name, config, number, offset):
            self.configName = name
            self.config = config
            self.configNumber = number
            self.globalOffset = offset

        def getConfig(self):
            return self.config

        def getConfigName(self):
            return self.configName

        def getConfigNumber(self):
            return self.configNumber

        def getGlobalOffset(self):
            return self.globalOffset

        def __str__(self):
            print "self.configName = ",self.configName,"self.config = ",self.config
            return "configName ="+self.configName
            
    ##
    # @brief create the configurator
    #
    # This constructor should only be called from a subclass's
    # constructor, in which case the fromSub parameter must be
    # set to True.
    # 
    # @param runid       the run identifier for the production run
    # @param wfConfig    the workflow config that describes the workflow
    # @param logger      the logger used by the caller.  This class
    #                       will set this create a child log with the
    #                       subname "config".  A sub class may wish to
    #                       reset the child logger for a different subname.
    # @param fromSub     set this to True to indicate that it is being called
    #                       from a subclass constructor.  If False (default),
    #                       an exception will be raised under the assumption
    #                       that one is trying instantiate it directly.
    def __init__(self, runid, prodConfig, wfConfig, logger, fromSub=False):
        self.runid = runid

        # the logger used by this instance
        if not logger:
            logger = Log.getDefaultLogger()
        self.parentLogger = logger
        self.logger = Log(logger, "config")

        sel.logger.log(Log.DEBUG, "WorkflowConfigurator:__init__")

        self.prodConfig = prodConfig
        self.wfConfig = wfConfig
        self.repository = repository

        if fromSub:
            raise RuntimeError("Attempt to instantiate abstract class, " +
                               "WorkflowConfigurator; see class docs")

    ###
    # @brief Configure the databases, and call an specialization required
    #
    def configure(self, provSetup, workflowVerbosity=None):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:configure")
        self._configureDatabases(provSetup)
        return self._configureSpecialized(self.wfConfig, workflowVerbosity)

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param config the workflow config
    # @param provSetup
    #
    def _configureDatabases(self, provSetup):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:_configureDatabases")

        #
        # setup the database for each database listed in workflow config
        #
        #print "self.wfConfig = "
        #print self.wfConfig
        #print "++++++++++++++++"
        
        if self.wfConfig.database != None:
            databaseConfigs = self.wfConfig.database

            for databaseConfig in databaseConfigs:
                databaseConfigurator = self.createDatabaseConfigurator(databaseConfig)
                databaseConfigurator.setup(provSetup)
        return

    ##
    # @brief complete non-database setup, including deploying the workflow and its
    # piplines onto the remote platform.
    #
    # This normally should be overriden.
    #
    # @return workflowLauncher
    def _configureSpecialized(self, wfConfig):
        workflowLauncher = self._createWorkflowLauncher()
        return workflowLauncher


    ##
    # @brief create the workflow launcher
    #
    # This "abstract" method must be overridden; otherwise an exception is raised
    # 
    # @return workflowLauncher
    def _createWorkflowLauncher(self):
        msg = 'called "abstract" WorkflowConfigurator._createWorkflowLauncher'
        self.logger.log(Log.FAIL, msg)
        raise RuntimeError(msg)

    ##
    # @brief lookup and create the configurator for database operations
    #
    def createDatabaseConfigurator(self, databaseConfig):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:createDatabaseConfigurator")
        className = databaseConfig.configurationClass
        classFactory = NamedClassFactory()
        configurationClass = classFactory.createClass(className)
        configurator = configurationClass(self.runid, databaseConfig, self.logger) 
        return configurator

    ##
    # @brief given a list of pipelinePolicies, number the section we're 
    # interested in based on the order they are in, in the productionConfig
    # We use this number Provenance to uniquely identify this set of pipelines
    #
    def expandConfigs(self, wfShortName):
        # Pipeline provenance requires that "activoffset" be unique and 
        # sequential for each pipeline in the production.  Each workflow
        # in the production can have multiple pipelines, and even a call for
        # duplicates of the same pipeline within it.
        #
        # Since these aren't numbered within the production config file itself,
        # we need to do this ourselves. This is slightly tricky, since each
        # workflow is handled individually by orca and had has no reference 
        # to the other workflows or the number of pipelines within 
        # those workflows.
        #
        # Therefore, what we have to do is go through and count all the 
        # pipelines in the other workflows so we can enumerate the pipelines
        # in this particular workflow correctly. This needs to be reworked.

        #wfNames = self.prodConfig.workflowNames
        print "expandConfigs wfShortName = ",wfShortName
        totalCount = 1
        for wfName in self.prodConfig.workflow:
            wfConfig = self.prodConfig.workflow[wfName]
            if wfName == wfShortName:
               # we're in the config which needs to be numbered
               expanded = []

               for pipelineName in wfConfig.pipeline:
                   config = wfConfig.pipeline[pipelineName]
                   # default to 1, if runCount doesn't exist
                   runCount = 1
                   if config.runCount != None:
                       runCount = config.runCount
                   for i in range(0,runCount):
                       expanded.append(self.ConfigGroup(pipelineName, config,i+1, totalCount))
                       totalCount = totalCount + 1
       
               return expanded
            else:
                
                for pipelineName in wfConfig.pipeline:
                    pipelineConfig = wfConfig.pipeline[pipelineName]
                    if pipelineConfig.runCount != None:
                        totalCount = totalCount + pipelineConfig.runCount
                    else:
                        totalCount = totalCount + 1
        return None # should never reach here - this is an error
