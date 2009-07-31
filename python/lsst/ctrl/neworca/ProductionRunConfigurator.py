class ProductionRunConfigurator:
    def __init__(self, runid, policy, verbosity, logger):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")
        self.runid = runid
        self.policy = policy
        self.verbosity = verbosity
        self.logger = logger

        # get pipelines
        pipePolicy = self.policy.get("pipelines")
        pipelines = pipelinePolicy.policyNames(True)

        for pipeline in pipelines:
            self.logger.log(Log.DEBUG, "pipeline --> "+pipeline)
            pipelinePolicy = pipePolicy.get(pipeline)
            if pipelinePolicy.get("launch",1) != 0:

    def createPipelineManager(self, shortName, prodPolicy):
        # shortName - the short name for the pipeline to be configured
        # prodPolicy - the policy that describes this production run
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createPipelineManager")

    def configure(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
