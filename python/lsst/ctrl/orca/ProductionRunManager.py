import os, os.path, sets
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class ProductionRunManager:
    def __init__(self, runid, policyFileName, logger, pipelineVerbosity=None):
        self.logger = logger
        self.pipelineVerbosity = pipelineVerbosity
        self.logger.log(Log.DEBUG, "ProductionRunManager:__init__")
        self.runid = runid
        self.pipelineManagers = []

        fullPolicyFilePath = ""
        if os.path.isabs(policyFileName) == True:
            fullPolicyFilePath = policyFileName
        else:
            fullPolicyFilePath = os.path.join(os.path.realpath('.'), policyFileName)

        # create policy file - but don't dereference yet
        self.policy = Policy.createPolicy(fullPolicyFilePath, False)


    def getRunId(self):
        return self.runid

    def getPolicy(self):
        return self.policy

    ##
    # @brief run the entire production
    #
    def runProduction(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:runProduction")

        # create configurator
        self.productionRunConfigurator = self.createConfigurator()

        # configure each pipeline
        for pipelineMgr in self.pipelineManagers:
            pipelineLauncher = pipelineMgr.configure()
            self.pipelineLaunchers.append(pipelineLauncher)

        # Check the configururation
        self.checkConfiguration()

        for pipelineMgr in self.pipelineManagers:
            pipelineManager.runPipeline()

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

    def createConfigurator(self):
        # prodPolicy - the production run policy
        self.logger.log(Log.DEBUG, "ProductionRunManager:createConfigurator")


        productionRunConfiguratorName = self.policy.get("productionRunConfiguratorClass")
        print self.policy.toString()
        print "---"
        print productionRunConfiguratorName

        classFactory = NamedClassFactory()
        productionRunConfiguratorClass = classFactory.createClass(productionRunConfiguratorName)
        productionRunConfigurator = productionRunConfiguratorClass(self.runid, self.policy, self.logger, self.pipelineVerbosity)

        # get pipelines
        pipelinePolicies = self.policy.get("pipelines")
        pipelinePolicyNames = pipelinePolicies.policyNames(True)

        # create a pipelineManager for each pipeline, and save it.
        for policyName in pipelinePolicyNames:
            self.logger.log(Log.DEBUG, "policyName --> "+policyName)
            pipelinePolicy = pipelinePolicies.get(policyName)
            if pipelinePolicy.get("launch",1) != 0:
                shortName = pipelinePolicy.get("shortname", policyName)
                pipelineManager = productionRunConfigurator.createPipelineManager(shortName, pipelinePolicy, self.pipelineVerbosity)
                self.pipelineManagers.append(pipelineManager)

        productionRunConfigurator.configure()

        return productionRunConfigurator
        
    def checkConfiguration(self, care):
        # care - level of "care" in checking the configuration to take. In
        # general, the higher the number, the more checks that are made.
        self.logger.log(Log.DEBUG, "ProductionRunManager:checkConfiguration")
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.checkConfiguration()

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
            
