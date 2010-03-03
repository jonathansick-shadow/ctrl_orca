class PipelineConfigurator:
    def __init__(self, runid, logger):
        self.runid = runid
        self.logger = logger
        self.logger.log(Log.DEBUG, "PipelineConfigurator:__init__")

    ##
    # @brief Setup as much as possible in preparation to execute the pipeline
    #            and return a PipelineLauncher object that will launch the
    #            configured pipeline.
    # @param policy the pipeline policy to use for configuration
    # @param configurationDict a dictionary containing configuration info
    # @param provenanceDict a dictionary containing info to record provenance
    # @param repository policy file repository location
    #
    def configure(self, pipelinePolicy, configurationDict, provenanceDict, repository):
        self.logger.log(Log.DEBUG, "PipelineConfigurator:configure")
        return 0 # return PipelineLauncher
