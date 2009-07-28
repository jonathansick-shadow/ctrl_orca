class ProductionRun:
    def __init__(self):
        self.logger.log(Log.DEBUG, "ProductionRun:__init__")
        self.runid = ""

    def getRunId(self):
        return self.runid

    def setRunId(self, id):
        self.runid = id
