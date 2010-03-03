import os, stat
import lsst.ctrl.orca as orca
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.db.MySQLConfigurator import MySQLConfigurator

class DatabaseConfigurator:
    def __init__(self, runid, dbPolicy, logger:
        """
        create a generic 
        @param runid     the run id
        @param policy    the policy to use in the configuration
        @param logger    the caller's Log instance from which this manager can.
                            create a child Log
        """
        return

    def setup(self, provSetup):
        return None

    def checkConfiguration(self, val):
        return

