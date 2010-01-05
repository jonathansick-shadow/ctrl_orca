import os
from lsst.ctrl.orca.BasicProductionRunConfigurator import BasicProductionRunConfigurator
from lsst.ctrl.orca.DagConfigurator import DagConfigurator
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class DagmanProductionRunConfigurator(BasicProductionRunConfigurator):
    ##
    # @brief finalize this production run, by creating a dagman file from
    # the template supplied in the policy file
    #
    def finalize(self, pipelineManagers):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:finalize")

        dagmanTemplate = self.policy.get("production.productionRunConfigurator.dagmanTemplate")
        localScratch = self.policy.get("production.localScratch")

        dagConfigurator = DagConfigurator(self.runid, localScratch, pipelineManagers)
        
        tempdir = os.path.join(localScratch, self.runid)

        dagmanFile = os.path.join(tempdir,"dagman_"+self.runid+".dag")
        dagConfigurator.rewrite(dagmanTemplate, dagmanFile)
        return


    ##
    # @brief configure this production run
    #
    def configure(self):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:configure")

        # create temporary directory for dagman-related files
        localScratchName = self.policy.get("production.localScratch")
        tempdir = os.path.join(localScratchName, self.runid)
        os.mkdir(tempdir)

        # grab the file name of the database before we load the policy
        dbFileName = self.policy.getFile("databaseConfig.database").getPath()
        dbFileName = os.path.join(self.repository, dbFileName)

        # create the database
        dbNamesDict = self.setupDatabase()

        # record the database filename provenance

        self.provenanceDict["user"] = self.databaseConfigurator.getUser()
        self.provenanceDict["runid"] = self.runid
        self.provenanceDict["dbrun"] = dbNamesDict["dbrun"]
        self.provenanceDict["dbglobal"] = dbNamesDict["dbglobal"]
        self.provenanceDict["repos"] = self.repository

        self.provenance = self.createProvenanceRecorder(self.databaseConfigurator.getUser(), self.runid, dbNamesDict)

        self.recordPolicy(dbFileName)
        return dbNamesDict
