from lsst.pex.logging import Log
from lsst.ctrl.orca.ProductionRunConfigurator import ProductionRunConfigurator
from lsst.ctrl.orca.db.DatabaseConfigurator import DatabaseConfigurator
from lsst.pex.policy import Policy

class BasicProductionRunConfigurator(ProductionRunConfigurator):
    def __init__(self, runid, policy, logger, pipelineVerbosity):
        self.logger = logger
        self.logger.log(Log.DEBUG, "BasicProductionConfigurator:__init__")
        self.runid = runid
        self.policy = policy
        self.databaseConfigurator = None
        self.pipelineVerbosity = pipelineVerbosity

        # these are policy settings which can be overriden from what they
        # are in the pipeline policies.
        self.policyOverrides = Policy() 
        if self.policy.exists("eventBrokerHost"):
            self.policyOverrides.set("execute.eventBrokerHost",
                              self.policy.get("eventBrokerHost"))
        if self.policy.exists("logThreshold"):
            self.policyOverrides.set("execute.logThreshold",
                              self.policy.get("logThreshold"))
        if self.policy.exists("shutdownTopic"):
            self.policyOverrides.set("execute.shutdownTopic",
                              self.policy.get("shutdownTopic"))


    def configure(self):
        dbNames = self.setupDatabase()

        dbRun = dbNames[0]
        dbGlobal = dbNames[1]
        dbFileName = dbNames[2]

        self.provenance = self.createProvenanceRecorder(self.databaseConfigurator.getUser(), self.runid, dbRun, dbGlobal)

        self.recordPolicy(dbFileName)
        return [ dbRun, dbGlobal]


    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "BasicProductionConfigurator:setupBasicProduction")

        databaseConfigPolicy = self.policy.get("databaseConfig")
        self.databaseConfigurator =  DatabaseConfigurator(self.runid, databaseConfigPolicy, self.logger)
        dbNames = self.databaseConfigurator.setup()

        # record provenance for this database policy file
        dbBaseURL = self.databaseConfigurator.getHostURL()
        dbRun = dbBaseURL+"/"+dbNames[0]
        dbGlobal = dbBaseURL+"/"+dbNames[1]

        dbFileName = self.policy.getFile("databaseConfig.database").getPath()
        dbFileName = os.path.join(self.repository, dbFileName)

        return [ dbRun, dbGlobal, dbFileName]

    def recordPolicy(self, fileName):
        self.provenance.recordPolicy(fileName)

    def createProvenanceRecorder(self, user, runid, dbRun, dbGlobal):
        provenance = Provenance(databaseConfigurator.getUser(), self.runid, dbRun, dbGlobal)

        return provenance
