import os
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.DryRun import DryRun

class DatabaseConfigurator:
    def __init__(self):
        self.logger = Log(Log.getDefaultLog(), "dc3pipe")
        self.dryrun = DryRun()
        dbPolicyFile = os.path.join(os.environ["HOME"], ".mysql/lsst-db-auth.paf")
        self.dbPolicy = Policy.createPolicy(dbPolicyFile)

        print self.dbPolicy.toString()

    def configureDatabase(self, policy, runId):
        self.logger.log(Log.DEBUG, "DatabaseConfigurator:configure called")


