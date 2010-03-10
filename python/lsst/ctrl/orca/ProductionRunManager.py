from __future__ import with_statement

import os, os.path, sets, threading, time
import lsst.daf.base as base
import lsst.pex.policy as pol
import lsst.ctrl.events as events
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.StatusListener import StatusListener
from lsst.pex.logging import Log

from EnvString import EnvString
from exceptions import ConfigurationError
from multithreading import SharedData
from ProductionRunConfigurator import ProductionRunConfigurator
#from threading import SharedData

##
# @brief A class in charge of launching, monitoring, managing, and stopping
# a production run
#
class ProductionRunManager:

    ##
    # @brief initialize
    # @param runid           name of the run
    # @param policyFileName  production run policy file
    # @param logger          the Log object to use
    # @param repository      the policy repository to assume; this will
    #                          override the value in the policy.
    #
    def __init__(self, runid, policyFileName, logger=None, repository=None):

        # _locked: a container for data to be shared across threads that 
        # have access to this object.
        self._locked = SharedData(False,
                                            {"running": False, "done": False})

        # the logger used by this instance
        if not logger:
            logger = Log.getDefaultLogger()
        self.logger = Log(logger, "manager")

        # the run id for this production
        self.runid = runid

        # once the workflows that make up this production is created we will
        # cache them here
        self._workflowManagers = None

        # a list of workflow Monitors
        self._workflowMonitors = []

        # the cached ProductionRunConfigurator instance
        self._productionRunConfigurator = None

        self.fullPolicyFilePath = ""
        if os.path.isabs(policyFileName) == True:
            self.fullPolicyFilePath = policyFileName
        else:
            self.fullPolicyFilePath = os.path.join(os.path.realpath('.'), policyFileName)

        # create policy file - but don't dereference yet
        self.policy = pol.Policy.createPolicy(self.fullPolicyFilePath, False)

        # determine the repository
        self.repository = repository
        if not self.repository:
            self.repository = self.policy.get("repositoryDirectory")
        if not self.repository:
            self.repository = "."
        else:
            self.repository = EnvString.resolve(self.repository)
            
        # do a little sanity checking on the repository before we continue.
        if not os.path.exists(self.repository):
            raise RuntimeError("specified repository " + self.repository + ": directory not found");        
        if not os.path.isdir(self.repository):
            raise RuntimeError("specified repository "+ self.repository + ": not a directory");

        # shutdown thread
        self._sdthread = None

    ##
    # @brief returns the runid of this production run
    #
    def getRunId(self):
        return self.runid

    ##
    # @brief setup this production to run.
    #
    # If the production was already configured, it will not be
    # reconfigured.
    # 
    # @param workflowVerbosity  the verbosity to pass down to configured
    #                             workflows and the pipelines they run.
    # @throws ConfigurationError  raised if any error arises during configuration or
    #                             while checking the configuration.
    #
    def configure(self, workflowVerbosity=None):
        if self._productionRunConfigurator:
            self.logger.log(Log.INFO-1, "production has already been configured.")
            return
        
        # lock this branch of code
        try:
            self._locked.acquire()

            self._productionRunConfigurator = self.createConfigurator(self.runid,
                                                                     self.fullPolicyFilePath)
            workflowManagers = self._productionRunConfigurator.configure(workflowVerbosity)

            self._workflowManagers = { "__order": [] }
            for wfm in workflowManagers:
                self._workflowManagers["__order"].append(wfm)
                self._workflowManagers[wfm.getName()] = wfm

        finally:
            self._locked.release()
            
    ##
    # @brief run the entire production
    # @param skipConfigCheck    skip the checks that ensures that configuration
    #                             was completed correctly.
    # @param workflowVerbosity  if not None, override the policy-specified logger
    #                             verbosity.  This is only used if the run has not
    #                             already been configured via configure().
    # @return bool  False is returned if the production was already started
    #                   once
    # @throws ConfigurationError  raised if any error arises during configuration or
    #                             while checking the configuration.
    def runProduction(self, skipConfigCheck=False, workflowVerbosity=None):
        self.logger.log(Log.DEBUG, "Running production: " + self.runid)

        if not self.isRunnable():
            if self.isRunning():
                self.logger.log(Log.INFO, "Production Run %s is already running" % self.runid)
            if self.isDone():
                self.logger.log(Log.INFO,
                                "Production Run %s has already run; start with new runid" % self.runid)
            return False

        # set configuration check care level.
        # Note: this is not a sanctioned pattern; should be replaced with use
        # of default policy.
        checkCare = 1
        if self.policy.exists("configCheckCare"):
            checkCare = self.policy.getInt("configCheckCare")
        if checkCare < 0:
            skipConfigCheck = True
        

        # lock this branch of code
        try:
            self._locked.acquire()
            self._locked.running = True

            # configure the production run (if it hasn't been already)
            if not self._productionRunConfigurator:
                self.configure(workflowVerbosity)

            # make sure the configuration was successful.
            if not self._workflowManagers:
                raise ConfigurationError("Failed to obtain workflowManagers from configurator")
            if skipConfigCheck:
                self.checkConfiguration(checkCare)

            provSetup = self._productionRunConfigurator.getProvenanceSetup()
            # SRP - temp comment out the next line
            provSetup.recordProduction()

            for workflow in self._workflowManagers["__order"]:
                mgr = self._workflowManagers[workflow.getName()]

                statusListener = StatusListener(self.logger)
                # this will block until the monitor is created.
                monitor = mgr.runWorkflow(statusListener)
                self._workflowMonitors.append(monitor)

        finally:
            self._locked.release()

        # start the thread that will listen for shutdown events
        if self.policy.exists("productionShutdownTopic"):
            self._startShutdownThread()

    ##
    # @brief determine whether production is currently running
    #
    def isRunning(self):
        #
        # check each monitor.  If any of them are still running,
        # the production is still running.
        #
        for monitor in self._workflowMonitors:
            if monitor.isRunning() == True:
                return True

        with self._locked:
            self._locked.running = False

        return False

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
    def createConfigurator(self, runid, policyFile):
        # prodPolicy - the production run policy
        self.logger.log(Log.DEBUG, "ProductionRunManager:createConfigurator")

        configuratorClass = ProductionRunConfigurator
        configuratorClassName = None
        if self.policy.exists("configurationClass"):
            configuratorClassName = self.policy.get("configurationClass")
        if configuratorClassName:
            classFactory = NamedClassFactory()
            configuratorClass = classFactory.createClass(configuratorClassName)

        return configuratorClass(runid, policyFile, self.repository, self.logger)

    ##
    # @brief
    # @param care      the thoroughness of the checks.
    # @param issueExc  an instance of MultiIssueConfigurationError to add 
    #                   problems to.  If not None, this function will not 
    #                   raise an exception when problems are encountered; they
    #                   will merely be added to the instance.  It is assumed
    #                   that the caller will raise that exception is necessary.
    def checkConfiguration(self, care=1, issueExc=None):
        # care - level of "care" in checking the configuration to take. In
        # general, the higher the number, the more checks that are made.
        self.logger.log(Log.DEBUG, "checkConfiguration")

        if not self._workflowManagers:
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

        # check production-wide configuration
        self._productionRunConfigurator.checkConfiguration(care, myProblems)

        # check configuration for each workflow
        for workflow in self._workflowManagers["__order"]:
            workflowMgr = self._workflowManagers[workflow]
            workflowMgr.checkConfiguration(care, myProblems)

        if not issueExc and myProblems.hasProblems():
            raise myProblems

    ##
    # @brief  stops all workflows in this production run 
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

        for workflow in self._workflowManagers["__order"]:
            workflowMgr = self._workflowManagers[workflow.getName()]
            workflowMgr.stopWorkflow(urgency)

        pollintv = 0.2
        running = self.isRunning()
        lasttime = time.time()
        while running and timeout > 0:
            time.sleep(pollintv)
            for workflow in self._workflowManagers["__order"]:
                running = self._workflowManagers[workflow.getName()].isRunning()
                if running:  break
            timeout -= time.time() - lasttime
        if not running:
            with self._locked:
                self._locked.running = False
                self._locked.done = True
        else:
            self.logger.log(Log.DEBUG, "Failed to shutdown pipelines within timeout: %ss" % timeout)
            return False

        return True
                
    ##
    # @brief  return the "short" name for each workflow in this
    # production.
    #
    # These may have been tweaked to ensure a unique list.  These are names
    # that can be passed to getWorkflowManager()
    #
    def getWorkflowNames(self):
        if self._workflowManagers:
            return self._workflowManagers["__order"]
        elif self._productionRunConfigurator:
            return self._productionRunConfigurator.getWorkflowNames()
        else:
            cfg = self.createConfigurator(self.fullPolicyFilePath)
            return cfg.getWorkflowNames()

    ##
    # @brief return the workflow manager for the given named workflow
    # @return   a WorkflowManager instance or None if it has not been
    #             created yet or name is not one of the names returned
    #             by getWorkflowNames()
    def getWorkflowManager(self, name):
        if not self._workflowManagers or not self._workflowManagers.has_key(name):
            return None
        return self._workflowManagers[name]

    class _ShutdownThread(threading.Thread):
        def __init__(self, parent, pollingIntv=0.2, listenTimeout=10):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self._parent = parent
            self._pollintv = pollingIntv
            self._timeout = listenTimeout
            brokerhost = parent.policy.get("eventBrokerHost")
            self._topic = parent.policy.get("productionShutdownTopic")
            self._evsys = events.EventSystem.getDefaultEventSystem()
            self._evsys.createReceiver(brokerhost, self._topic)

        def run(self):
            self._parent.logger.log(Log.DEBUG,
                                   "listening for shutdown event at %s s intervals" % self._pollintv)

            self._parent.logger.log(Log.DEBUG-10, "checking for shutdown event")
            self._parent.logger.log(Log.DEBUG, "self._timeout = %s" % self._timeout)
            shutdownData = self._evsys.receive(self._topic, self._timeout)
            while self._parent.isRunning() and shutdownData is None:
                time.sleep(self._pollintv)
                #self._parent.logger.log(Log.DEBUG-10, "checking for shutdown event")
                shutdownData = self._evsys.receive(self._topic, self._timeout)
                #time.sleep(1)
                #shutdownData = self._evsys.receive(self._topic, 10)
            self._parent.logger.log(Log.DEBUG, "DONE!")

            if shutdownData:
                self._parent.stopProduction(shutdownData.getInt("level"))
            self._parent.logger.log(Log.DEBUG, "Everything shutdown - All finished")

    def _startShutdownThread(self):
        self._sdthread = ProductionRunManager._ShutdownThread(self)
        self._sdthread.start()

    def getShutdownThread(self):
        return self._sdthread

    def joinShutdownThread(self):
        if self._sdthread is not None:
            self._sdthread.join()
