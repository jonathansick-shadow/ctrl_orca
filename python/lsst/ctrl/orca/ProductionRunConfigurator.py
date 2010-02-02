from lsst.pex.logging import Log
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.WorkflowManager import WorkflowManager

class ProductionRunConfigurator:
    ##
    # @brief a class for configuring a production run as a whole
    #
    def __init__(self, runid, policy, repository, logger, verbosity):
        self.logger = logger
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")
        self.runid = runid
        self.policy = policy
        self.verbosity = verbosity
        self.repository = repository
        self.provenanceDict = {}

    ##
    # @brief create the WorkflowManager for the pipelien with the given shortName
    #
    def createWorkflowManager(self, prodPolicy, configurationDict,  workflowVerbosity):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createWorkflowManager")

        workflowManager = WorkflowManager(self.runid, prodPolicy, configurationDict, self.repository, self.provenanceDict, self.logger, self.verbosity)
        return workflowManager

    ##
    # @brief return the provenance dictionary
    #
    def getProvenanceDict(self):
        return self.provenanceDict

    ##
    # @brief carry out the production-level configuration and setup. To
    #            complete the configuration, the createWorkflowManager()
    #            method must be called to create each of the workflow managers
    #
    def configure(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
        return {}

    ##
    # @brief finalize anything necessary for the production run
    #
    def finalize(self, workflowManagers):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:finalize")
