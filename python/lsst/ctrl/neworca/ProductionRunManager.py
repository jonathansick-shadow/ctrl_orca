class ProductionRunManager:
    def __init__(self, runid, policy, verbosity, logger):
        self.logger = logger
        self.verbosity = verbosity
        self.logger.log(Log.DEBUG, "ProductionRunManager:__init__")
        self.runid = runid
        self.policy = policy
        self.pipelineManagers = []


    def getRunId(self):
        return self.runid

    def getPolicy(self):
        return self.policy

    def runProduction(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:runProduction")

        # create configurator
        self.productionRunConfigurator = createConfigurator(policy)

        # configure each pipeline
        for pipelineMgr in self.pipelineManagers:
            pipelineLauncher = pipelineMgr.configure()
            self.pipelineLaunchers.append(pipelineLauncher)

        # Check the configururation
        checkConfiguration()

        for pipelineMgr in self.pipelineManagers:
            pipelineManager.runPipeline()

    def isRunning(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isRunning")
        return False

    def isDone(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isDone")
        return False

    def isRunnable(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isRunnable")
        return False

    def createConfigurator(self, prodPolicy):
        # prodPolicy - the production run policy
        self.logger.log(Log.DEBUG, "ProductionRunManager:createConfigurator")
        productionRunConfigurator = ProductionRunConfiguratorFactory.createProductionRunConfigurator(runid, policy, self.verbosity, self.logger)

        # get pipelines
        pipelinePolicies = prodPolicy.get("pipelines")
        pipelinePolicyNames = pipelinePolicies.policyNames(True)

        # create a pipelineManager for each pipeline, and save it.
        for policyName in pipelinePolicyNames:
            self.logger.log(Log.DEBUG, "pipeline --> "+pipeline)
            pipelinePolicy = pipelines.get(policyName)
            if pipelinePolicy.get("launch",1) != 0:
                shortName = pipelinePolicy.get("shortname", policyName)
                pipelineManager = productionRunConfigurator.createPipelineManager(shortName, pipelinePolicy)
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
            
