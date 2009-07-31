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

        #classFactory = NamedClassFactory()
        #databaseConfigName = self.policy.get("databaseConfig.configuratorClass")

        dbConfigPolicy = self.policy.getPolicy("databaseConfig")
        dbPolicy = dbConfigPolicy.loadPolicyFiles(self.repository)
        dbPolicy = dbConfigPolicy.getPolicy("database")
        dbPolicy.loadPolicyFiles(self.repository)
        dbType = self.policy.get("databaseConfig.type")

        #self.logger.log(Log.DEBUG, "databaseConfigName = " + databaseConfigName)
        #databaseConfiguratorClass = classFactory.createClass(databaseConfigName)
        #self.databaseConfigurator = databaseConfiguratorClass(dbType, dbPolicy)        
        self.dbConfigurator = DatabaseConfigurator(dbType, dbPolicy)
        self.dbConfigurator.setup()
        # these next two lines have to be done within setup in dbConfigurator
        #self.dbConfigurator.checkConfiguration(dbPolicy)
        #dbNames = self.dbConfigurator.prepareForNewRun(self.runid)

