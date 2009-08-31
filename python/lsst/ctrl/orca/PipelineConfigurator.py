class PipelineConfigurator:
    def __init__(self, runid, logger):
        self.runid = runid
        self.logger = logger
        self.logger.log(Log.DEBUG, "PipelineConfigurator:__init__")

    def configure(self, pipelinePolicy, configurationDict, provenanceDict, repository):
        self.logger.log(Log.DEBUG, "PipelineConfigurator:configure")
        return 0 # return PipelineLauncher
