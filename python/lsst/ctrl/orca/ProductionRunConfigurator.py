import os
from lsst.ctrl.orca.ProductionRunConfigurator import ProductionRunConfigurator
from lsst.ctrl.orca.db.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class ProductionRunConfigurator:
    ##
    # @brief create a basic production run
    #
    def __init__(self, runid, logger, workflowVerbosity):
        logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")
        self.logger = logger
        self.runid = runid
        self.workflowVerbosity = workflowVerbosity

        self.productionPolicy = None
        self.databaseConfigurator = None
        self.provenanceDict = {}

        # these are policy settings which can be overriden from what they
        # are in the workflow policies.
        self.policyOverrides = Policy() 
        if self.policy.exists("eventBrokerHost"):
            self.policyOverrides.set("execute.eventBrokerHost",
                              self.productionPolicy.get("eventBrokerHost"))
        if self.policy.exists("logThreshold"):
            self.policyOverrides.set("execute.logThreshold",
                              self.productionPolicy.get("logThreshold"))
        if self.policy.exists("shutdownTopic"):
            self.policyOverrides.set("execute.shutdownTopic",
                              self.productionPolicy.get("shutdownTopic"))

    ##
    # @brief create the WorkflowManager for the pipelien with the given shortName
    #
    def createWorkflowManager(self, workflowPolicy):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createWorkflowManager")

        workflowManager = WorkflowManager(self.runid, self.logger, workflowPolicy, self.verbosity)
        return workflowManager

    ##
    # @brief configure this production run
    #
    def configure(self, productionPolicy):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
        self.productionPolicy = productionPolicy
        self.repository = self.productionPolicy.get("repositoryDirectory")

        provSetup = ProvenanceSetup()

        # cycle through the policies in the production policy file to get
        # a list of files to record.
        #
        # TODO here:  Don't blindly add files as below.  We have to cycle through
        # all of the files first, dereferencing each file at each level, add them
        # adding them to a Set() to avoid recording provenance on a file more than
        # once, and then add all of those files to provSetup.

        names = productionPolicy.fileNames()
        for name in names:
            if policy.getValueType(name) == Policy.FILE:
                # TODO: There's a bug in Policy that prevents retrieving all 
                # files -  we want all files, not just one.  
                filename = policy.getFile(name).getPath()
                provSetup.addProductionPolicyFile(name)
            
        #
        # setup the database for each database listed in production policy
        #
        databasePolicies = self.productionPolicy.getArray("database")
        for databasePolicy in databasePolicies:
            databaseConfigurator = self.createDatabaseConfigurator(databasePolicy)
            databaseConfigurator.setDatabase()

        #
        # do specialized production level configuration, if it exists
        #
        specialConfigurationPolicy = self.productionPolicy.get("configuration")
        if specialConfigurationPolicy != None:
            self.specializedConfigure(self.productionPolicy)

        workflowPolicies = self.productionPolicy.getArray("workflow")
        for workflowPolicy in workflowPolicies:
            # copy in appropriate production level info into workflow Node  -- ?

            workflowManager = self.createWorkflowManager(workflowPolicy)
            workflowManager.configure(provSetup)
            self.workflowManagers.append(workflowManager)

        return self.workflowManagers

    ##
    # @brief high-level configuration checks
    #
    def checkConfiguration(self, care):
        return
        
    ##
    # @brief lookup and create the configurator for database operations
    #
    def createDatabaseConfigurator(self, databasePolicy):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createDatabaseConfigurator")
        className = self.databasePolicy.get("configurationClass")
        classFactory = NamedClassFactory()
        configurationClass = classFactory.createClass(className)
        configurator = configurationClass(self.runid, self.logger, self.verbosity) 
        return configurator

    ##
    # @brief return the Workflowmanagers associated with this configurator
    #
    def getWorkflowManagers():
        return self.workflowManagers

    ##
    # @brief
    #
    def specializedConfigure(self, specialConfigurationPolicy):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:specializedConfigure")
        return
