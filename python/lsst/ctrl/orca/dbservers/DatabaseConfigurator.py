from lsst.pex.logging import Log
from lsst.ctrl.orca.DryRun import DryRun

class DatabaseConfigurator:
    def __init__(self):
        self.logger = Log(Log.getDefaultLog(), "dc3pipe")
        self.dryrun = DryRun()

    def configureDatabase(self, policy, runId):
        self.logger.log(Log.DEBUG, "DatabaseConfigurator:configure called")
