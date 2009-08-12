from lsst.pex.logging import Log
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.PipelineManager import PipelineManager

class ProductionRunConfigurator:
    def __init__(self, runid, policy, verbosity, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")
        self.runid = runid
        self.policy = policy
        self.verbosity = verbosity

        # get pipelines
        pipePolicy = self.policy.get("pipelines")
        pipelines = pipelinePolicy.policyNames(True)

        for pipeline in pipelines:
            self.logger.log(Log.DEBUG, "pipeline --> "+pipeline)
            pipelinePolicy = pipePolicy.get(pipeline)
            # TODO: more stuff here

    def createPipelineManager(self, shortName, pipelinePolicy, pipelineVerbosity):
        # shortName - the short name for the pipeline to be configured
        # prodPolicy - the policy that describes this production run
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createPipelineManager")

        print "shortName = ",shortName
        #classFactory = NamedClassFactory()
        #pipelineManagerName = pipelinePolicy.get("managerClass")
        #pipelineManagerClass = classFactory.createClass(pipelineManagerName)
        #pipelineManager = pipelineManagerClass(pipelineVerbosity)
        pipelineManager = PipelineManager(self.logger)
        return pipelineManager

    def configure(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
