import os
from lsst.ctrl.orca.ProductionRunConfigurator import ProductionRunConfigurator
from lsst.ctrl.orca.db.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class BasicProductionRunConfigurator(ProductionRunConfigurator):
    ##
    # @brief create a production run given 
    #
    def __init__(self, runid, policy, repository, logger, verbosity):
        self.logger = logger
        self.logger.log(Log.DEBUG, "BasicProductionConfigurator:__init__")
        self.runid = runid
        self.policy = policy
        self.databaseConfigurator = None
        self.verbosity = verbosity
        self.repository = repository
        self.provenanceDict = {}

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


    ##
    # @brief configure this production run
    #
    def configure(self):
        dbFileName = self.policy.getFile("databaseConfig.database").getPath()
        dbFileName = os.path.join(self.repository, dbFileName)

        dbNames = self.setupDatabase()


        dbNamesDict = {}
        dbNamesDict["dbrun"] = dbNames[0]
        dbNamesDict["dbglobal"] = dbNames[1]
        #dbRun = dbNames[0]
        #dbGlobal = dbNames[1]


        self.provenanceDict["user"] = self.databaseConfigurator.getUser()
        self.provenanceDict["runid"] = self.runid
        self.provenanceDict["dbrun"] = dbNamesDict["dbrun"]
        self.provenanceDict["dbglobal"] = dbNamesDict["dbglobal"]
        self.provenanceDict["repos"] = self.repository

        self.provenance = self.createProvenanceRecorder(self.databaseConfigurator.getUser(), self.runid, dbNamesDict["dbrun"], dbNamesDict["dbglobal"])

        self.recordPolicy(dbFileName)
        return dbNamesDict

    ##
    # @brief
    #
    def getProvenanceRecorder(self):
        return self.provenance

    ##
    # @brief
    #
    def getProvenanceDict(self):
        return self.provenanceDict

    ##
    # @brief
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "BasicProductionConfigurator:setupBasicProduction")

        databaseConfigPolicy = self.policy.get("databaseConfig")
        databaseConfigPolicy.loadPolicyFiles(self.repository)
        
       
        dbPolicy = databaseConfigPolicy.getPolicy("database")
        dbPolicy.loadPolicyFiles(self.repository)
      
     
        self.databaseConfigurator =  DatabaseConfigurator(self.runid, dbPolicy, self.logger)
        dbNames = self.databaseConfigurator.setup()

        # record provenance for this database policy file
        dbBaseURL = self.databaseConfigurator.getHostURL()
        dbRun = dbBaseURL+"/"+dbNames[0]
        dbGlobal = dbBaseURL+"/"+dbNames[1]

        return [ dbRun, dbGlobal]

    ##
    # @brief
    #
    def recordPolicy(self, fileName):
        self.provenance.recordPolicy(fileName)

    ##
    # @brief
    #
    def createProvenanceRecorder(self, user, runid, dbRun, dbGlobal):
        provenance = Provenance(self.databaseConfigurator.getUser(), self.runid, dbRun, dbGlobal)

        return provenance
