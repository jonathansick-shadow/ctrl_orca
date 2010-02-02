class WorkflowConfigurator:
    def __init__(self, runid, logger):
        self.runid = runid
        self.logger = logger
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:__init__")

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param policy the workflow policy to use for configuration
    # @param configurationDict a dictionary containing configuration info
    # @param provenanceDict a dictionary containing info to record provenance
    # @param repository policy file repository location
    #
    def configure(self, workflowPolicy, configurationDict, provenanceDict, repository):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:configure")
        return 0 # return WorkflowLauncher
