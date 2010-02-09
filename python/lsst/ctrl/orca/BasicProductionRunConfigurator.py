import os
from lsst.ctrl.orca.ProductionRunConfigurator import ProductionRunConfigurator
from lsst.ctrl.orca.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class BasicProductionRunConfigurator(ProductionRunConfigurator):
    ##
    # @brief create a basic production run
    #
    def __init__(self, runid, policyFile, repository=None, logger=None, workflowVerbosity=None):

        # the logger used by this instance
        if not logger:
            logger = Log.getDefaultLogger()
        self._parentLogger = logger
        self.logger = Log(logger, "config")

        logger.log(Log.DEBUG, "BasicProductionRunConfigurator:__init__")

        self.runid = runid
        self._prodPolicyFile = policyFile
        self.productionPolicy = Policy.createPolicy(policyFile)

        self.repository = repository
        self.workflowVerbosity = workflowVerbosity

        self.provenanceDict = {}
        self._wfnames = None

        # cache the database configurators for checking the configuration.
        self._databaseConfigurators = []

        # these are policy settings which can be overriden from what they
        # are in the workflow policies.
        self.policyOverrides = Policy() 
        if self.productionPolicy.exists("eventBrokerHost"):
            self.policyOverrides.set("execute.eventBrokerHost",
                              self.productionPolicy.get("eventBrokerHost"))
        if self.productionPolicy.exists("logThreshold"):
            self.policyOverrides.set("execute.logThreshold",
                              self.productionPolicy.get("logThreshold"))
        if self.productionPolicy.exists("shutdownTopic"):
            self.policyOverrides.set("execute.shutdownTopic",
                              self.productionPolicy.get("shutdownTopic"))
    ##
    # @brief setup the database
    #
    def setupDatabase(self):
        return
