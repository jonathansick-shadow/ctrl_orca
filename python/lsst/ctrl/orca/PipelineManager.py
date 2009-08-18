from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.pex.logging import Log
import lsst.pex.policy as pol

class PipelineManager:
    def __init__(self, policy, logger, verbosity):
        self.logger =  logger
        self.logger.log(Log.DEBUG, "PipelineManager:__init__")
        self.urgency = 0
        self.pipelineLauncher = None
        self.policy = policy
        self.verbosity = verbosity

    def runPipeline(self):
        self.logger.log(Log.DEBUG, "PipelineManager:runPipeline")
        if self.pipelineConfigurator == None:
            configure()
        self.pipelineLauncher.launch()

    def stopPipeline(self, timeout):
        self.logger.log(Log.DEBUG, "PipelineManager:stopPipeline")

    def cleanUp(self):
        self.logger.log(Log.DEBUG, "PipelineManager:cleanUp")

    def configure(self):
        self.logger.log(Log.DEBUG, "PipelineManager:configure")

        self.pipelineConfigurator = self.createConfigurator()
        self.pipelineLauncher = self.pipelineConfigurator.configure(self.policy)

    def createConfigurator(self):
        self.logger.log(Log.DEBUG, "PipelineManager:createConfigurator")
        print "create Configurator -> ",self.policy.toString()
        className = self.policy.get("className")
        classFactory = NamedClassFactory()
        configuratorClass = classFactory.createClass(className)
        configurator = configuratorClass() 
        return configurator

    def isDone(self):
        self.logger.log(Log.DEBUG, "PipelineManager:isDone")
        return True

    def isRunnable(self):
        self.logger.log(Log.DEBUG, "PipelineManager:isRunnable")
        return True

    def checkConfiguration(self, care):
        # care - an indication of how throughly to check.  In general, a
        # higher number will result in more checks being run.
        self.logger.log(Log.DEBUG, "PipelineManager:createConfiguration")
