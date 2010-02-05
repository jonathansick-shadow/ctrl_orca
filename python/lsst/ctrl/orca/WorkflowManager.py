from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.pex.logging import Log
import lsst.pex.policy as pol

class WorkflowManager:
    ##
    # @brief 
    #
    def __init__(self, runid, logger, wfPolicy, verbosity):
        self.logger =  logger
        self.logger.log(Log.DEBUG, "WorkflowManager:__init__")
        self.runid = runid
        self.wfPolicy = wfPolicy
        self.verbosity = verbosity

        self.workflowName = self.wfPolicy.get("shortName")
        self.urgency = 0
        self.workflowLauncher = None
        self.hasCompleted = False
        self.isActive = False

    ##
    # @brief setup, launch and monitor a workflow to its completion, and then
    #            clean-up.
    #
    def runWorkflow(self):
        self.isActive = True
        self.logger.log(Log.DEBUG, "WorkflowManager:runWorkflow")
        if self.workflowConfigurator == None:
            self.configure()
        self.workflowLauncher.launch()
        self.cleanUp()

    ##
    # @brief stop the workflow.
    #
    def stopWorkflow(self, urgency):
        self.logger.log(Log.DEBUG, "WorkflowManager:stopWorkflow")

    ##
    # @brief carry out post-execution tasks for removing workflow data and
    #            state from the platform and archiving/ingesting products as
    #            needed.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "WorkflowManager:cleanUp")
        self.hasCompleted = True


    ##
    # @brief prepare a workflow for launching.
    #
    def configure(self, provSetup):
        self.logger.log(Log.DEBUG, "WorkflowManager:configure")

        self.workflowConfigurator = self.createConfigurator(self.wfPolicy)

        # do specialized workflow level configuration here, this may include
        # calling ProvenanceSetup.getWorkflowCommands()
        return self.workflowConfigurator

    def createConfigurator(self, wfPolicy):
        self.logger.log(Log.DEBUG, "WorkflowManager:createConfigurator")
        
        className = wfPolicy.get("configurationClass")
        classFactory = NamedClassFactory()
        
        configuratorClass = classFactory.createClass(className)
        configurator = configuratorClass(self.runid, self.logger, self.verbosity) 
        return configurator

    ##
    # @brief return True if the workflow has been run to completion.  This will
    #            be true if the workflow has run normally through cleaned up or
    #            if it was stopped and clean-up has been called.
    #
    def isDone(self):
        self.logger.log(Log.DEBUG, "WorkflowManager:isDone")
        return self.hasCompleted

    ##
    # @brief return True if the workflow can still be called.  This may return
    #            False because the workflow has already been run and cannot be
    #            re-run.
    #
    def isRunnable(self):
        self.logger.log(Log.DEBUG, "WorkflowManager:isRunnable")
        if isActive == True:
            return False
        return True

    ##
    # @brief Runs checks that ensure that the Workflow has been properly set up.
    #
    def checkConfiguration(self, care):
        # care - an indication of how throughly to check.  In general, a
        # higher number will result in more checks being run.
        self.logger.log(Log.DEBUG, "WorkflowManager:createConfiguration")

    def getWorkflowName(self):
        return self.workflowName

    def getNodeCount(self):
        return self.workflowConfigurator.getNodeCount()

