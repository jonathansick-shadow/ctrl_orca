from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.pex.logging import Log
import lsst.pex.policy as pol

class PipelineManager:
    ##
    # @brief 
    #
    def __init__(self, runid, pipelinePolicy, configurationDict, repository, provenanceDict, logger, verbosity):
        self.logger =  logger
        self.logger.log(Log.DEBUG, "PipelineManager:__init__")
        self.runid = runid
        self.pipelinePolicy = pipelinePolicy
        self.configurationDict = configurationDict
        self.provenanceDict = provenanceDict
        self.repository = repository
        self.verbosity = verbosity

        self.urgency = 0
        self.pipelineLauncher = None
        self.hasCompleted = False
        self.isActive = False

    ##
    # @brief setup, launch and monitor a pipeline to its completion, and then
    #            clean-up.
    #
    def runPipeline(self):
        self.isActive = True
        self.logger.log(Log.DEBUG, "PipelineManager:runPipeline")
        if self.pipelineConfigurator == None:
            self.configure()
        self.pipelineLauncher.launch()
        self.cleanUp()

    def getCPUCount():
        return self.pipelineConfigurator.getCPUCount()

    ##
    # @brief stop the pipeline.
    #
    def stopPipeline(self, timeout):
        self.logger.log(Log.DEBUG, "PipelineManager:stopPipeline")

    ##
    # @brief carry out post-execution tasks for removing pipeline data and
    #            state from the platform and archiving/ingesting products as
    #            needed.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "PipelineManager:cleanUp")
        self.hasCompleted = True

    ##
    # @brief prepare a pipeline for launching.
    #
    def configure(self):
        self.logger.log(Log.DEBUG, "PipelineManager:configure")

        self.pipelineConfigurator = self.createConfigurator()
        self.pipelineLauncher = self.pipelineConfigurator.configure(self.pipelinePolicy, self.configurationDict, self.provenanceDict, self.repository)

    def createConfigurator(self):
        self.logger.log(Log.DEBUG, "PipelineManager:createConfigurator")
        
        className = self.pipelinePolicy.get("configuratorClass")
        classFactory = NamedClassFactory()
        
        configuratorClass = classFactory.createClass(className)
        configurator = configuratorClass(self.runid, self.logger, self.verbosity) 
        return configurator

    ##
    # @brief return True if the pipeline has been run to completion.  This will
    #            be true if the pipeline has run normally through cleaned up or
    #            if it was stopped and clean-up has been called.
    #
    def isDone(self):
        self.logger.log(Log.DEBUG, "PipelineManager:isDone")
        return self.hasCompleted

    ##
    # @brief return True if the pipeline can still be called.  This may return
    #            False because the pipeline has already been run and cannot be
    #            re-run.
    #
    def isRunnable(self):
        self.logger.log(Log.DEBUG, "PipelineManager:isRunnable")
        if isActive == True:
            return False
        return True

    ##
    # @brief Runs checks that ensure that the Pipeline has been properly set up.
    #
    def checkConfiguration(self, care):
        # care - an indication of how throughly to check.  In general, a
        # higher number will result in more checks being run.
        self.logger.log(Log.DEBUG, "PipelineManager:createConfiguration")
