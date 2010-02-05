class WorkflowConfigurator:
    def __init__(self, runid, logger):
        logger.log(Log.DEBUG, "WorkflowConfigurator:__init__")
        self.logger = logger
        self.runid = runid

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param policy the workflow policy to use for configuration
    # @param configurationDict a dictionary containing configuration info
    # @param provenanceDict a dictionary containing info to record provenance
    # @param repository policy file repository location
    #
    def configure(self, wfPolicy, provSetup):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:configure")

        #
        # setup the database for each database listed in workflow policy
        #
        databasePolicies = self.wfPolicy.getArray("database")
        for databasePolicy in databasePolicies:
            databaseConfigurator = self.createDatabaseConfigurator(databasePolicy)
            databaseConfigurator.setupDatabase(provSetup)

        workflowLauncher = WorkflowLauncher(wfPolicy)
        return workflowLauncher

    ##
    # @brief lookup and create the configurator for database operations
    #
    def createDatabaseConfigurator(self, databasePolicy):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:createDatabaseConfigurator")
        className = self.databasePolicy.get("configurationClass")
        classFactory = NamedClassFactory()
        configurationClass = classFactory.createClass(className)
        configurator = configurationClass(self.runid, self.logger, self.verbosity) 
        return configurator

