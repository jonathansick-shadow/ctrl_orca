import EventMonitor
import lsst.ctrl.orca as orca
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
import lsst.ctrl.orca.pipelines
import lsst.ctrl.orca.dbservers
from lsst.pex.logging import Log


class ProductionRunManager:
    def __init__(self, policy):
        self.logger = Log(Log.getDefaultLog(), "d3pipe")

        self.policy = policy

        self.eventMonitor = None

        self.pipelineManagers = []

    def configure(self, runId):
        self.logger.log(Log.DEBUG, "ProductionRunManager:configure")

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
            
                pipelineManagerClass = classFactory.createClass(pipelineManagerName)
           
                pipelineManager = pipelineManagerClass()

                # configure this pipeline
                pipelineManager.configure(pipeline, pipelinePolicy, runId)
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
