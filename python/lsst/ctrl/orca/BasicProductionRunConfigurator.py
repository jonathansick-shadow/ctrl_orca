from lsst.pex.logging import Log
from lsst.ctrl.orca.ProductionRunConfigurator import ProductionRunConfigurator
from lsst.ctrl.orca.db.DatabaseConfigurator import DatabaseConfigurator

class BasicProductionRunConfigurator(ProductionRunConfigurator):
    def __init__(self, runid, policy, logger, pipelineVerbosity):
        self.logger = logger
        self.logger.log(Log.DEBUG, "BasicProductionConfigurator:__init__")
        self.runid = runid
        self.policy = policy
        self.databaseConfigurator = None
        self.pipelineVerbosity = pipelineVerbosity

    def configure(self):
        self.setupDatabase()

    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "BasicProductionConfigurator:setupBasicProduction")

        databaseConfigPolicy = self.policy.get("databaseConfig")
        self.databaseConfigurator =  DatabaseConfigurator(self.runid, databaseConfigPolicy, self.logger)
        dbNames = self.databaseConfigurator.setup()

        # record provenance for this database policy file
        dbBaseURL = self.databaseConfigurator.getHostURL()
        self.dbRun = dbBaseURL+"/"+dbNames[0]
        self.dbGlobal = dbBaseURL+"/"+dbNames[1]

        dbFileName = self.policy.getFile("databaseConfig.database").getPath()
        dbFileName = os.path.join(self.repository, dbFileName)

        provenance = Provence(databaseConfigurator.getUser(), self.runid, dbRun, dbGlobal)

        provenance.recordPolicy(dbFileName)
