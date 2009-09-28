from lsst.pex.logging import Log
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.PipelineManager import PipelineManager

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
    # @brief create the PipelineManager for the pipelien with the given shortName
    #
    def createPipelineManager(self, prodPolicy, configurationDict,  pipelineVerbosity):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createPipelineManager")

        pipelineManager = PipelineManager(self.runid, prodPolicy, configurationDict, self.repository, self.provenanceDict, self.logger, self.verbosity)
        return pipelineManager

    ##
    # @brief
    #
    def getProvenanceDict(self):
        return None

    ##
    # @brief carry out the production-level configuration and setup. To
    #            complete the configuration, the createPipelineManager()
    #            method must be called to create each of the pipeline managers
    #
    def configure(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
        return {}

