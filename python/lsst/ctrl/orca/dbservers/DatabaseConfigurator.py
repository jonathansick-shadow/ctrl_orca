from lsst.pex.logging import Log

class DatabaseConfigurator:
    def __init__(self):
        self.logger = Log(Log.getDefaultLog(), "dc3pipe")

    def configureDatabase(self, policy, runId):
        self.logger.log(Log.DEBUG, "DatabaseConfigurator:configure called")
