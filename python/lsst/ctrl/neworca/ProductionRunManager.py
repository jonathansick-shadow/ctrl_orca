class ProductionRunManager:
    def __init__(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:__init__")
        self.runid = ""
        self.policy = None
        self.pipelineManager = []


    def getRunId(self):
        return self.runid

    def setRunId(self, id):
        self.runid = id

    def getPolicy(self):
        return self.policy

    def setPolicy(self, policy):`
        self.policy = policy

    def runProduction(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:runProduction")
        self.productionRunConfigurator = createConfigurator()

        # create all pipelineManagers
        pipelineManager = productionRunConfigurator.createPipelineManager(name, policy)
        self.pipelineManagers.append(pipelineManager)

        productionRunConfigurator.configure()

        for pipelineMgr in self.pipelineManagers:
            pipelineLauncher = pipelineMgr.configure()
            self.pipelineLaunchers.append(pipelineLauncher)

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
        return 0 # returns a productionRunConfigurator
        
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
            
            
