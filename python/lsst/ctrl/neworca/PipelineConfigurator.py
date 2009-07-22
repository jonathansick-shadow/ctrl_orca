class PipelineConfigurator:
    def __init__(self):
        self.logger.log(Log.DEBUG, "PipelineConfigurator:__init__")

    def configure(self):
        self.logger.log(Log.DEBUG, "PipelineConfigurator:configure")
        return 0 # return PipelineLauncher
