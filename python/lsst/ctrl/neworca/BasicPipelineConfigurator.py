class BasicPipelineConfigurator:
    def __init__(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:__init__")

    def configure(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:configure")
        return 0 # return PipelineLauncher

    def createNodeList(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:createNodeList")

    def prepPlatform(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:prepPlatform")

    def deploySetup(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:deploySetup")

    def createDirs(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:createDirs")

    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:setupDatabase")
