from lsst.pex.logging import Log
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.PipelineManager import PipelineManager

class ProductionRunConfigurator:
    def __init__(self, runid, policy, repository, logger, verbosity):
        self.logger = logger
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")
        self.runid = runid
        self.policy = policy
        self.verbosity = verbosity
        self.repository = repository
        self.provenanceDict = {}

    def createPipelineManager(self, pipelinePolicy, configurationDict,  pipelineVerbosity):
        # shortName - the short name for the pipeline to be configured
        # prodPolicy - the policy that describes this production run
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createPipelineManager")

        #
        # we're given a pipelinePolicy, and things that need to be overridden
        #

        pipelineManager = PipelineManager(self.runid, pipelinePolicy, configurationDict, self.repository, self.provenanceDict, self.logger, self.verbosity)
        return pipelineManager

    def getProvenanceDict(self):
        return None

    def configure(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
        return {}

