class WorkflowConfigurator:
    def __init__(self, runid, logger):
        logger.log(Log.DEBUG, "WorkflowConfigurator:__init__")
        self.logger = logger
        self.runid = runid

    ###
    # @brief Configure the databases, and call an specialization required
    #
    def configure(self, wfPolicy, provSetup):
        self._configureDatabases(wfPolicy, provSetup)
        return self._configureSpecialized(wfPolicy)

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param policy the workflow policy to use for configuration
    # @param provSetup
    #
    def _configureDatabases(self, wfPolicy, provSetup):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:configure")

        #
        # setup the database for each database listed in workflow policy
        #
        databasePolicies = self.wfPolicy.getArray("database")
        for databasePolicy in databasePolicies:
            databaseConfigurator = self.createDatabaseConfigurator(databasePolicy)
            databaseConfigurator.setupDatabase(provSetup)
        return

    ##
    # @brief Override this method to provide a specialized implementation
    # @return workflowLauncher
    def _configureSpecialized(self, wfPolicy):
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

