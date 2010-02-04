import os
from lsst.ctrl.orca.ProductionRunConfigurator import ProductionRunConfigurator
from lsst.ctrl.orca.db.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class BasicProductionRunConfigurator:
    ##
    # @brief create a basic production run
    #
    def __init__(self, runid, logger, workflowVerbosity):
        logger.log(Log.DEBUG, "BasicProductionRunConfigurator:__init__")
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
    # @brief configure this production run
    #
    def configure(self, productionPolicy):
        self.logger.log(Log.DEBUG, "BasicProductionRunConfigurator:configure")
        self.productionPolicy = productionPolicy
        self.repository = self.productionPolicy.get("repositoryDirectory")

        #
        # setup the database for each database listed in production policy
        #
        databasePolicies = self.productionPolicy.getArray("database")
        for databasePolicy in databasePolicies:
            databaseConfigurator = self.createDatabaseConfigurator(databasePolicy)
            databaseConfigurator.setupDatabase()

        #
        # do specialized production level configuration, if it exists
        #
        specialConfigurationPolicy = self.productionPolicy.get("configuration")
        if specialConfigurationPolicy != None:
            self.specializedConfigure(specialConfigurationPolicy)

        workflowPolicies = self.productionPolicy.getArray("workflow")
        for workflowPolicy in workflowPolicies:
            # copy in appropriate production level info into workflow Node  -- ?

            workflowManager = self.createWorkflowManager(workflowPolicy)
            workflowManager.configure()
            self.workflowManagers.append(workflowManager)
        return self.workflowManagers

    def specializedConfigure(self, specialConfigurationPolicy):
        self.logger.log(Log.DEBUG, "BasicProductionRunConfigurator:specializedConfigure")
        return

    def createDatabaseConfigurator(self, databasePolicy):
        self.logger.log(Log.DEBUG, "BasicProductionRunConfigurator:createDatabaseConfigurator")
        className = self.databasePolicy.get"configurationClass")
        classFactory = NamedClassFactory()
        configurationClass = classFactory.createClass(className)
        configurator = configurationClass(self.runid, self.logger, self.verbosity) 
        return configurator

    ##
    # @brief create the WorkflowManager for the pipelien with the given shortName
    #
    def createWorkflowManager(self, workflowPolicy):
        self.logger.log(Log.DEBUG, "BasicProductionRunConfigurator:createWorkflowManager")

        workflowManager = WorkflowManager(self.runid, self.logger, workflowPolicy, self.verbosity)
        return workflowManager
