import os, os.path
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
    def __init__(self):
        self.logger = Log(Log.getDefaultLog(), "d3pipe")

        self.eventMonitor = None

        self.pipelineManagers = []


    def checkConfiguration(self):
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.checkConfiguration()

    def createDatabase(self, runId):
        #classFactory = NamedClassFactory()
        #databaseConfigName = self.policy.get("databaseConfig.configuratorClass")

        print self.policy
        dbConfigPolicy = self.policy.getPolicy("databaseConfig")
        print "dbConfigPolicy  ",dbConfigPolicy
        print "self.repository  "+self.repository
        dbPolicy = dbConfigPolicy.loadPolicyFiles(self.repository)
        print "NEW dbConfigPolicy  ",dbConfigPolicy
        dbPolicy = dbConfigPolicy.getPolicy("database")
        dbPolicy.loadPolicyFiles(self.repository)
        print "------- dbPolicy start ---------"
        print dbPolicy.toString()
        print "------- dbPolicy end   ---------"
        dbType = self.policy.get("databaseConfig.type")

        #self.logger.log(Log.DEBUG, "databaseConfigName = " + databaseConfigName)
        #databaseConfiguratorClass = classFactory.createClass(databaseConfigName)
        #self.databaseConfigurator = databaseConfiguratorClass(dbType, dbPolicy)        
        self.dbConfigurator = DatabaseConfigurator(dbType, dbPolicy)
        self.dbConfigurator.checkConfiguration(dbPolicy)
        dbNames = self.dbConfigurator.prepareForNewRun(runId)
        print "dbNames"
        print dbNames
        return dbNames

    def configure(self, policyFile, runId):
        self.logger.log(Log.DEBUG, "ProductionRunManager:configure")

        print "configure: policyFile = "+policyFile
        self.policy = Policy.createPolicy(policyFile, False)
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
        self.policy.loadPolicyFiles(self.repository, True)


        dbNames = self.createDatabase(runId)
        dbBaseURL = self.dbConfigurator.getHostURL()
        dbRun = dbBaseURL+"/"+dbNames[0];
        dbGlobal = dbBaseURL+"/"+dbNames[1];
        print "getUser = "+self.dbConfigurator.getUser()
        print "runId = "+runId
        print "dbRun = "+dbRun
        print "dbGlobal = "+dbGlobal


        provenance = Provenance(self.dbConfigurator.getUser(), runId, dbRun, dbGlobal)
        print "configure: policyFile = "+policyFile
        #realLocation = os.path.join(self.repository, policyFile)
        #print "configure: realLocation = "+realLocation

        provenance.recordPolicy(policyFile)
        
        classFactory = NamedClassFactory()

        pipePolicy = self.policy.get("pipelines")
        pipelines = pipePolicy.policyNames(True)


        for pipeline in pipelines:
            self.logger.log(Log.DEBUG, "pipeline ---> "+pipeline)
            pipelinePolicy = pipePolicy.get(pipeline)
            if pipelinePolicy.get("launch",1) != 0:

                # create the pipelineManager object that will actually do
                # the work.
                pipelineManagerName = pipelinePolicy.get("platform.deploy.managerClass")
            
                print "pipelinePolicy is "
                print pipelinePolicy
                pipelineManagerClass = classFactory.createClass(pipelineManagerName)
           
                pipelineManager = pipelineManagerClass()

                # configure this pipeline
                pipelineManager.configure(pipeline, pipelinePolicy, runId, self.repository, provenance, dbRun)
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
