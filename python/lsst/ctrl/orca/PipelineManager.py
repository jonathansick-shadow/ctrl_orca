class PipelineManager:
    def __init__(self):
        self.logger.log(Log.DEBUG, "PipelineManager:__init__")
        self.urgency = 0

    def runPipeline(self):
        self.logger.log(Log.DEBUG, "PipelineManager:runPipeline")
        if self.pipelineConfigurator == None:
            configure()

    def stopPipeline(self, timeout):
        self.logger.log(Log.DEBUG, "PipelineManager:stopPipeline")

    def cleanUp(self):
        self.logger.log(Log.DEBUG, "PipelineManager:cleanUp")

    def configure(self):
        self.logger.log(Log.DEBUG, "PipelineManager:configure")
        #
        # XXX - should be a factory to create this BasicPipelineConfigurator object
        #
        self.pipelineConfigurator = createConfigurator(policy)
        self.pipelineConfigurator.configure()
        return 0 # return PipelineLauncher

    def createConfigurator(self, policy):
        self.logger.log(Log.DEBUG, "PipelineManager:createConfigurator")
        className = policy.get("className")
        configuratorClass = NamedClassFactory.createClass(className)
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
