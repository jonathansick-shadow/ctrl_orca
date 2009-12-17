import os, os.path, sets
import lsst.daf.base as base
import lsst.pex.policy as pol
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.EventListener import EventListener
from lsst.ctrl.orca.EventResolver import EventResolver

##
# @brief configures, checks, and launches pipelines
#
class ProductionRunManager:
    ##
    # @brief initialize
    # @param runid name of the run
    # @param policyFileName production run policy file
    # @param logger Log object 
    # @param pipelineVerbosity verbosity level for the pipelines
    #
    def __init__(self, runid, policyFileName, logger, pipelineVerbosity=None):
        self.logger = logger
        self.pipelineVerbosity = pipelineVerbosity
        self.logger.log(Log.DEBUG, "ProductionRunManager:__init__")
        self.runid = runid
        self.pipelineManagers = []
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


    ##
    # @brief returns the runid of this production run
    #
    def getRunId(self):
        return self.runid

    ##
    # @brief returns the policy of this production run
    #
    def getPolicy(self):
        return self.policy

    ##
    # @brief run the entire production
    #
    def runProduction(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:runProduction")

        # create Production Run Configurator specified by the policy
        self.productionRunConfigurator = self.createConfigurator()

        # configure each pipeline
        for pipelineMgr in self.pipelineManagers:
            pipelineLauncher = pipelineMgr.configure()
            # XXX - next line (?)
            #self.pipelineLaunchers.append(pipelineLauncher)

        # XXX - we were going to do it this way, but now the number of nodes
        # for this dag is specified, rather than calculated. 
        # finalize configuration
        #self.totalNodeCount = self.productionRunConfigurator.finalize(self.pipelineManagers)

        self.totalNodeCount = self.policy.get("nodeCount")

        # Check the configururation
        # TODO: add "care" parameter
        self.checkConfiguration(0)


        # TODO: spawn listener object here

        # create ProductionRunner
        productionRunnerName = self.policy.get("productionRunnerClass")

        if productionRunnerName == None:
            print "Couldn't find 'productionRunnerName' in:"
            print self.policy.toString()
        classFactory = NamedClassFactory()
        productionRunnerClass = classFactory.createClass(productionRunnerName)
        productionRunner = productionRunnerClass(self.runid, self.totalNodeCount, self.pipelineManagers)

        # 12/2/09 - DC3a - 
        #for pipelineManager in self.pipelineManagers:
        #    pipelineManager.runPipeline()
        productionRunner.runPipelines()

        #resolver = EventResolver()
        #listener = EventListener(self.topic, resolver)
        #listener.start()
        #listener.join()

    ##
    # @brief determine whether production is currently running
    #
    def isRunning(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isRunning")
        return False

    ##
    # @brief determine whether production has completed
    #
    def isDone(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isDone")
        return False

    ##
    # @brief determine whether production can be run
    #
    def isRunnable(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isRunnable")
        return False

    ##
    # @brief setup and create the ProductionRunConfigurator
    # @return ProductionRunConfigurator
    #
    #
    def createConfigurator(self):
        # prodPolicy - the production run policy
        self.logger.log(Log.DEBUG, "ProductionRunManager:createConfigurator")

        # these are policy settings which can be overriden from what they
        # are in the pipeline policies. Save them for when we create the
        # PipelineManager, and let the PipelineManger use these overrides
        # as it sees fit to do so.
        policyOverrides = pol.Policy()
        if self.policy.exists("eventBrokerHost"):
            policyOverrides.set("execute.eventBrokerHost",
                              self.policy.get("eventBrokerHost"))
        if self.policy.exists("logThreshold"):
            policyOverrides.set("execute.logThreshold",
                              self.policy.get("logThreshold"))
        if self.policy.exists("shutdownTopic"):            
            policyOverrides.set("execute.shutdownTopic", self.policy.get("shutdownTopic"))

        productionRunConfiguratorName = self.policy.get("productionRunConfiguratorClass")

        if productionRunConfiguratorName == None:
            print "Couldn't find 'productionRunConfiguratorName' in:"
            print self.policy.toString()
        classFactory = NamedClassFactory()
        productionRunConfiguratorClass = classFactory.createClass(productionRunConfiguratorName)
        productionRunConfigurator = productionRunConfiguratorClass(self.runid, self.policy, self.repository, self.logger, self.pipelineVerbosity)


        self.dbNames = productionRunConfigurator.configure()

        productionRunConfigurator.recordPolicy(self.fullPolicyFilePath)

        # get pipelines
        pipelinePolicies = self.policy.get("pipelines")
        pipelinePolicyNames = pipelinePolicies.policyNames(True)

        platformSet = sets.Set()

        # create a pipelineManager for each pipeline, and save it.
        for policyName in pipelinePolicyNames:
            self.logger.log(Log.DEBUG, "policyName --> "+policyName)
            pipelinePolicy = pipelinePolicies.get(policyName)
            # - pex.policy api change fix - if pipelinePolicy.get("launch",1) != 0:
            doLaunch = pipelinePolicy.get("launch")
            if doLaunch != 0:
                # - pex.policy api change fix - shortName = pipelinePolicy.get("shortname", policyName)
                shortName = pipelinePolicy.get("shortname")
                if shortName == None:
                    shortName = policyName
                configuration = pipelinePolicy.getFile("configuration").getPath()
                configuratorClassName = pipelinePolicy.get("configuratorClass")

                # record the platform policy, if that platform hasn't been recorded yet
                platformFilename = pipelinePolicy.getFile("platform").getPath()
                platformFilename = os.path.join(self.repository, platformFilename)
                if (platformFilename in platformSet) == False:
                    productionRunConfigurator.recordPolicy(platformFilename)
                    platformSet.add(platformFilename)

                pipelinePolicy.loadPolicyFiles(self.repository, True)

                configurationDict = self.rewritePolicy(configuration, pipelinePolicy, policyOverrides)
                pipelineManager = productionRunConfigurator.createPipelineManager(pipelinePolicy, configurationDict, self.pipelineVerbosity)
                self.pipelineManagers.append(pipelineManager)



        return productionRunConfigurator

    ##
    # @brief
    #
    def rewritePolicy(self, configuration, pipelinePolicy, policyOverrides):
        # NOTE:  pipelinePolicy must be fully de-referenced by this point.

        #  read in default policy        
        #  read in given policy
        #  in given policy:
        #     set: execute.eventBrokerHost
        #     set: execute.dir
        #     set: execute.database.url
        #  write new policy file with overridden values        
        #  write file to self.dirs["work"]
        polfile = os.path.join(self.repository, configuration)

        newPolicy = pol.Policy.createPolicy(polfile, False)

        if policyOverrides is not None:
            for name in policyOverrides.paramNames():
                newPolicy.set(name, policyOverrides.get(name))

        executeDir = pipelinePolicy.get("platform.dir")
        newPolicy.set("execute.dir", executeDir)

        dbRunURL = self.dbNames["dbrun"] 

        newPolicy.set("execute.database.url", dbRunURL)

        propDictionary = {}
        propDictionary["filename"] =  os.path.basename(polfile)
        propDictionary["policy"]= newPolicy

        return propDictionary
        
        # provenance really should be recorded here
        #self.provenance.recordPolicy(newPolicyFile)

        
    ##
    # @brief
    #
    def checkConfiguration(self, care):
        # care - level of "care" in checking the configuration to take. In
        # general, the higher the number, the more checks that are made.
        self.logger.log(Log.DEBUG, "ProductionRunManager:checkConfiguration")
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.checkConfiguration(care)

    ##
    # @brief
    #
    def stopProduction(self, urgency):
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
        self.logger.log(Log.DEBUG, "ProductionRunManager:stopProduction")
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.stopProduction(urgency)
            
