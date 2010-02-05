import os
from lsst.ctrl.orca.ProductionRunConfigurator import ProductionRunConfigurator
from lsst.ctrl.orca.db.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class BasicProductionRunConfigurator(ProductionRunConfigurator):
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
    # @brief setup the database
    #
    def setupDatabase(self):
        return
