class ProductionRunManager:
    def __init__(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:__init__")
        self.runid = ""
        self.policy = 0

    def runProduction(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:runProduction")

    def isRunning(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isRunning")
        return False

    def isDone(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isDone")
        return False

    def isRunnable(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isRunnable")
        return False

    def createConfigurator(self, policy):
        self.logger.log(Log.DEBUG, "ProductionRunManager:createConfigurator")
        
    def checkConfiguration(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:checkConfiguration")

    def stopProduction(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:stopProduction")
