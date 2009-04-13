import os, os.path, sets
import EventMonitor
import lsst.ctrl.orca as orca
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
import lsst.ctrl.orca.pipelines
import lsst.ctrl.orca.dbservers
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.dbservers.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.ctrl.orca.EnvString import EnvString


class ProductionRunManager:
    def __init__(self, pipeVerb=None, logger=None):
        """
        create a ProductionRunManager.  
        @param pipeVerb  the verbosity level to pass onto the pipelines for 
                            pipeline messages
        @param logger    the caller's Log instance from which this manager can.
                            create a child Log
        """
        self.pipelineVerbosity = pipeVerb
        if logger is None:  logger = orca.logger
        self.logger = Log(logger, "productionRunMgr")

        self.eventMonitor = None

        self.pipelineManagers = []

        self.policySet = sets.Set()


    def checkConfiguration(self):
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.checkConfiguration()

    def createDatabase(self, runId):
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
        self.dbConfigurator.checkConfiguration(dbPolicy)
        dbNames = self.dbConfigurator.prepareForNewRun(runId)
        return dbNames

    def configure(self, policyFile, runId):
        self.logger.log(Log.DEBUG, "ProductionRunManager:configure")

        fullPolicyFilePath = ""
        if os.path.isabs(policyFile) == True:
            fullPolicyFilePath = policyFile
        else:
            fullPolicyFilePath = os.path.join(os.path.realpath('.'), policyFile)

        # create policy file - but don't dereference yet
        self.policy = Policy.createPolicy(fullPolicyFilePath, False)
        if orca.repository == None:
            reposValue = self.policy.get("repositoryDirectory")
            if reposValue == None:
                self.repository = "."
            else:
                self.repository = EnvString.resolve(reposValue)
        else:
            self.repository = orca.repository

        if not os.path.exists(self.repository):
            raise RuntimeError("specified repository " + self.repository + ": directory not found");
        
        if not os.path.isdir(self.repository): 
            raise RuntimeError("specified repository "+ self.repository + ": not a directory");

        # TODO: next, get all the referenced files, check if they exist in the 
        # policySet object.  If they don't, record provenance, and add them to
        # the set.


        dbFilename = self.policy.getFile("databaseConfig.database").getPath()
        dbFilename = os.path.join(self.repository, dbFilename)

        

        # end of TODO

        #self.policy.loadPolicyFiles(self.repository, True)


        dbNames = self.createDatabase(runId)
        dbBaseURL = self.dbConfigurator.getHostURL()
        dbRun = dbBaseURL+"/"+dbNames[0];
        dbGlobal = dbBaseURL+"/"+dbNames[1];

        provenance = Provenance(self.dbConfigurator.getUser(), runId, dbRun, dbGlobal)
        provenance.recordEnvironment()

        # record policy file handed in from command line
        provenance.recordPolicy(fullPolicyFilePath)
        self.policySet.add(fullPolicyFilePath)

        # databaseConfig.database policy 
        provenance.recordPolicy(dbFilename)
        self.policySet.add(dbFilename)
        
        classFactory = NamedClassFactory()

        pipePolicy = self.policy.get("pipelines")
        pipelines = pipePolicy.policyNames(True)


        for pipeline in pipelines:
            self.logger.log(Log.DEBUG, "pipeline ---> "+pipeline)
            pipelinePolicy = pipePolicy.get(pipeline)
            if pipelinePolicy.get("launch",1) != 0:

                # create the pipelineManager object that will actually do
                # the work.

                platformFilename = pipelinePolicy.getFile("platform").getPath()
                platformFilename = os.path.join(self.repository, platformFilename)
                if (platformFilename in self.policySet) == False:
                    provenance.recordPolicy(platformFilename)
                    self.policySet.add(platformFilename)

                pipelinePolicy.loadPolicyFiles(self.repository, True)
            
                pipelineManagerName = pipelinePolicy.get("platform.deploy.managerClass")
                pipelineManagerClass = classFactory.createClass(pipelineManagerName)
           
                pipelineManager = pipelineManagerClass(self.pipelineVerbosity)

                # configure this pipeline
                pipelineManager.configure(pipeline, pipelinePolicy, runId, self.repository, provenance, dbRun, self.policySet)
                self.pipelineManagers.append(pipelineManager)


    def launch(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:launchPipelines")
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.launchPipeline()

    def startEventMonitor(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:startEventMonitor")
        monitorFile = self.policy.get("eventMonitorConfig")
        self.eventMonitor = EventMonitor.EventMonitor(monitorFile)

    def stopEventMonitor(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:stopEventMonitor")
        self.eventMonitor.stop()

    def runPostLaunchProcess(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:runPostLaunchProcess")
        # launch event generator (or whatever) at this point

    def stop(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:stop")
        self.stopEventMonitor()
        self.cleanup()

    def cleanup(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:cleanup")

    def handleEvent(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:handleEvent")

    def handleFailure(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:handleFailure")
