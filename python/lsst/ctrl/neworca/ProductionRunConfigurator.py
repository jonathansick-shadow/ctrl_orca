import os, os.path, sets
import EventMonitor
import lsst.ctrl.orca as orca
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
import lsst.ctrl.orca.pipelines
import lsst.ctrl.orca.dbservers
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.dbservers.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.ctrl.orca.EnvString import EnvString


class ProductionRunConfigurator:
    def __init__(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")

    def createPipelineManager(self, shortName, prodPolicy):
        # shortName - the short name for the pipeline to be configured
        # prodPolicy - the policy that describes this production run
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createPipelineManager")

    def configure(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
