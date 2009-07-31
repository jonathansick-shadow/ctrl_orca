class BasicProductionRunConfigurator(ProductionRunConfigurator):
    def __init__(self, runid, policy):
        self.logger.log(Log.DEBUG, "BasicProductionConfigurator:__init__")
        self.runid = runid
        self.databaseConfigurator = None

    def configure(self, policyFile):
        setupDatabase()

    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "BasicProductionConfigurator:setupBasicProduction")

        self.databaseConfigurator =  DatabaseConfigurator(runid)
        dbNames = self.databaseConfigurator.setup(runid)

        # record provenance for this database policy file
        dbBaseURL = self.databaseConfigurator.getHostURL()
        self.dbRun = dbBaseURL+"/"+dbNames[0]
        self.dbGlobal = dbBaseURL+"/"+dbNames[1]

        dbFileName = self.policy.getFile("databaseConfig.database").getPath()
        dbFileName = os.path.join(self.repository, dbFileName)

        provenance = Provence(databaseConfigurator.getUser(), self.runid, dbRun, dbGlobal)

        provenance.recordPolicy(dbFileName)
