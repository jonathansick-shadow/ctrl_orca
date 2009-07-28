class ProductionRunConfigurator:
    def __init__(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")

    def createPipelineManager(self, shortName, prodPolicy):
        # shortName - the short name for the pipeline to be configured
        # prodPolicy - the policy that describes this production run
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createPipelineManager")

    def configure(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
