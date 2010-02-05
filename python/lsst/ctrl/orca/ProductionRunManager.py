from __future__ import with_statement

import os, os.path, sets
import lsst.daf.base as base
import lsst.pex.policy as pol
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString

from lsst.pex.orca.threading import SharedData

##
# @brief configures, checks, and launches workflows
#
class ProductionRunManager:
    ##
    # @brief initialize
    # @param runid name of the run
    # @param policyFileName production run policy file
    # @param logger Log object 
    # @param workflowVerbosity verbosity level for the workflows
    #
    def __init__(self, runid, policyFileName, logger, workflowVerbosity=None, skipConfigCheck=False):

        # _locked: a container for data to be shared across threads that have access
        # to this object.
        self._locked = threading.SharedData(False,
                                            {"running": False, "done": False})

        # the logger used by this instance
        self.logger = Log(logger, "productionRunManager")

        # the default verbosity to hand down to the pipelines
        self.workflowVerbosity = workflowVerbosity

        # the run id for this production
        self.runid = runid

        # once the workflows that make up this production is created we will
        # cache them here
        self._workflowManagers = None

        # the cached ProductionRunConfigurator instance
        self.productionRunConfigurator = None

        # if False, runProduction() will do a check of the configuration before
        # actually launching the workflows.
        self._skipConfigCheck = skipConfigCheck
        
        self.dbNames = []
        self.totalNodeCount = 0

        self.fullPolicyFilePath = ""
        if os.path.isabs(policyFileName) == True:
            self.fullPolicyFilePath = policyFileName
        else:
            self.fullPolicyFilePath = os.path.join(os.path.realpath('.'), policyFileName)

        # create policy file - but don't dereference yet
        self.policy = pol.Policy.createPolicy(self.fullPolicyFilePath, False)

        # determine the repository
        reposValue = self.policy.get("repositoryDirectory")
        if reposValue == None:
            self.repository = "."
        else:
            self.repository = EnvString.resolve(reposValue)
            
        # do a little sanity checking on the repository before we continue.
        if not os.path.exists(self.repository):
            raise RuntimeError("specified repository " + self.repository + ": directory not found");        
        if not os.path.isdir(self.repository):
            raise RuntimeError("specified repository "+ self.repository + ": not a directory");

        # set configuration check care level.
        # Note: this is not a sanctioned pattern; should be replaced with use
        # of default policy.
        self._checkCare = 1
        if self.policy.exists("configCheckCare"):
            self._checkCare = self.policy.getInt("configCheckCare")
        if self._checkCare < 0:
            self._skipConfigCheck = True

    ##
    # @brief returns the runid of this production run
    #
    def getRunId(self):
        return self.runid

    ##
    # @brief run the entire production
    # @return bool  False is returned if the production was already started once
    #
    def runProduction(self):
        self.logger.log(Log.DEBUG, "Running production: " + self.runid)

        if not self.isRunnable():
            if self.isRunning():
                self.logger.log(Log.INFO, "Production Run %s is already running" % self.runid)
            if self.isDone():
                self.logger.log(Log.INFO,
                                "Production Run %s has already run; start with new runid" % self.runid)
            return False

        # lock this branch of code
        try:
            self._locked.acquire()
            self._locked.running = True

            # create Production Run Configurator specified by the policy
            if not self.productionRunConfigurator:
                self.productionRunConfigurator = self.createConfigurator()
                self.workflowManagers = self.productionRunConfigurator.configure()
                if not self.workflowManagers:
                    raise ConfigurationError("Failed to obtain workflowManagers from configurator")
            if self._skipConfigCheck:
                self.productionRunConfigurator.checkConfiguration(self._checkCare)


            for workflow in self.workflowManagers["__order"]:
                mgr = self.workflowManagers[workflow]

                # this will block until the monitor is created.
                mgr.runWorkflow()

        finally:
            self._locked.release()

        # start the thread that will listen for shutdown events
        if self.policy.exists("shutdownTopic"):
            self._startShutdownThread()

    ##
    # @brief determine whether production is currently running
    #
    def isRunning(self):
        return self._locked.running

    ##
    # @brief determine whether production has completed
    #
    def isDone(self):
        return self._locked.done

    ##
    # @brief determine whether production can be run
    #
    def isRunnable(self):
        return not self.isRunning() and not self.isDone()

    ##
    # @brief setup and create the ProductionRunConfigurator
    # @return ProductionRunConfigurator
    #
    #
    def createConfigurator(self):
        # prodPolicy - the production run policy
        self.logger.log(Log.DEBUG, "ProductionRunManager:createConfigurator")

        configuratorClass = BasicProductionRunConfigurator
        configuratorClassName = None
        if self.policy.exists("configurationClass"):
            configuratorClassName = self.policy.get("configurationClass")
        if configuratorClassName:
            classFactory = NamedClassFactory()
            configuratorClass = classFactory.createClass(configuratorClassName)

        return configuratorClass(self.runid, self.policy, self.repository,
                                 self.logger, self.workflowVerbosity)

    ##
    # @brief
    # @param care      the thoroughness of the checks.
    # @param issueExc  an instance of MultiIssueConfigurationError to add problems
    #                    to.  If not None, this function will not raise
    #                    an exception when problems are encountered; they
    #                    will merely be added to the instance.  It is assumed
    #                    that the caller will raise that exception is necessary.
    def checkConfiguration(self, care=1, issueExc=None):
        # care - level of "care" in checking the configuration to take. In
        # general, the higher the number, the more checks that are made.
        self.logger.log(Log.DEBUG, "checkConfiguration")

        if not self.workflowManagers:
            msg = "%s: production has not been configured yet" % self.runid
            if self._name:
                msg = "%s %s" % (self._name, msg)
            if issueExc is None:
                raise ConfigurationError(msg)
            else:
                issueExc.addProblem(msg)
                return

        myProblems = issueExc
        if myProblems is None:
            myProblems = MultiIssueConfigurationError("problems encountered while checking configuration")

        for workflow in self.workflowManagers["__order"]:
            workflowMgr = self.workflowManagers[workflow]
            workflowMgr.checkConfiguration(care, myProblems)

    ##
    # @brief
    #
    def stopProduction(self, urgency, timeout=1800):
        # urgency - an indicator of how urgently to carry out the shutdown.  
        #
        # Recognized values are: 
        #   FINISH_PENDING_DATA - end after all currently available data has 
        #                         been processed 
        #   END_ITERATION       - end after the current data ooping iteration 
        #   CHECKPOINT          - end at next checkpoint opportunity 
        #                         (typically between stages) 
        #   NOW                 - end as soon as possible, forgoing any 
        #                         checkpointing
        if not self.isRunning():
            self.logger.log(Log.INFO-1,
                           "shutdown requested when production is not running")
            return
        
        if self.logger.sends(Log.INFO):
            self.logger.log(Log.INFO,
                            "Shutting down production (urgency=%s)" % urgency)

        for workflow in self.workflowManagers["__order"]:
            workflowMgr = self.workflowManagers[workflow]
            workflowMgr.stopProduction(urgency)

        pollintv = 0.2
        running = self.isRunning()
        lasttime = time.time()
        while running and timeout > 0:
            time.sleep(pollintv)
            for workflow in self.workflowManagers["__order"]:
                running = self.workflowManagers[workflow]
                if running:  break
            timeout -= time.time() - lasttime
        if not running:
            with self._locked:
                self._locked.running = False
                self._locked.done = True
        else:
            self.logger.log(Log.FAIL, "Failed to shutdown pipelines within timeout: %ss" % timeout)
            return False

        return True
                

    def getWorkflowNames(self):
        if self.workflowManagers:
            return self.workflowManagers["__order"]
        else:
            ProductionRunManager.determineWorkflowNames(self.policy)

    @staticmethod
    def determineWorkflowNames(policy):
        workflows = self.policy.getArray("workflow")
        names = []
        i = 1
        for wf in workflows:
            name = ProductionRunManager._workflowName(wf, i)
            while name in names:
                name = ProductionRunManager._workflowName(None, i, name)
            names.append(name)
            i += 1
            
    @staticmethod
    def _workflowName(wfpolicy, which, defbase="Workflow-"):
        if wfpolicy and wfpolicy.exists("shortName"):
            return wfpolicy.get("shortName")

        return "%s%d" % (defbase, which)

    ##
    # @brief return the workflow manager for the given named workflow
    # @return   a WorkflowManager instance or None if it has not been
    #             created yet or name is not one of the names returned
    #             by getWorkflowNames()
    def getWorkflowManager(self, name):
        if not self.workflowManagers or not self.workflowManagers.has_key(name):
            return None
        return self.workflowManagers[name]

    class _ShutdownThread(threading.Thread):
        def __init__(self, parent, pollingIntv=0.2, listenTimeout=10):
            self._parent = parent
            self._pollintv = pollingIntv
            self._timeout = listenTimeout
            brokerhost = parent.policy.get("eventBrokerHost")
            self._topic = parent.policy.get("shutdownTopic")
            self._evsys = events.EventSystem.getDefaultEventSystem()
            self._evsys.createReceiver(brokerhost, topic)

        def run(self):
            self.parent.logger.log(Log.DEBUG,
                                   "listening for shutdown event at %s s intervals" % self._pollintv)

            self.parent.logger.log(Log.DEBUG-10, "checking for shutdown event")
            shutdownData = self._evsys.receive(self._topic, self._timeout)
            while self.parent.isRunning() and shutdownData is None:
                time.sleep(self._pollintv)
                self.parent.logger.log(Log.DEBUG-10, "checking for shutdown event")
                shutdownData = self._evsys.receive(self._topic, self._timeout)

            if shutdownData:
                self.parent.stopProduction(shutdownData.getInt("level"))

    def _startShutdownThread(self):
        sdthread = _ShutdownThread(self)
        sdthread.start()
