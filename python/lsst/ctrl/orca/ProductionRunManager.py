import EventMonitor
from NamedClassFactory import NamedClassFactory
import pipelines
import dbservers

class ProductionRunManager:
    def __init__(self, policy):
        self.policy = policy

        self.eventMonitor = None

        self.pipelineManagers = []

    def configure(self, runId):
        print "ProductionRunManager:configure"

        classFactory = NamedClassFactory()

        pipePolicy = self.policy.get("pipelines")
        pipelines = pipePolicy.policyNames(True)

        for pipeline in pipelines:
            print "pipeline ---> ",pipeline
            pipelinePolicy = pipePolicy.get(pipeline)
            if pipelinePolicy.get("launch",1) != 0:

                # create the pipelineManager object that will actually do
                # the work.
                pipelineManagerName = self.policy.get("pipelineManager")
            
                pipelineManagerClass = classFactory.createClass(pipelineManagerName)
           
                pipelineManager = pipelineManagerClass()

                # configure this pipeline
                pipelineManager.configure(pipeline, pipelinePolicy, runId)
                self.pipelineManagers.append(pipelineManager)


    def launch(self):
        print "ProductionRunManager:launchPipelines"
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.launchPipeline()

    def startEventMonitor(self):
        print "ProductionRunManager:startEventMonitor"
        monitorFile = self.policy.get("eventMonitorConfig")
        self.eventMonitor = EventMonitor.EventMonitor(monitorFile)

    def stopEventMonitor(self):
        print "ProductionRunManager:stopEventMonitor"
        self.eventMonitor.stop()

    def runPostLaunchProcess(self):
        print "ProductionRunManager:runPostLaunchProcess"
        # launch event generator (or whatever) at this point

    def stop(self):
        print "ProductionRunManager:stop"
        self.stopEventMonitor()
        self.cleanup()

    def cleanup(self):
        print "ProductionRunManager:cleanup"

    def handleEvent(self):
        print "ProductionRunManager:handleEvent"

    def handleFailure(self):
        print "ProductionRunManager:handleFailure"
