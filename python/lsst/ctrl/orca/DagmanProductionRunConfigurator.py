import os
from lsst.ctrl.orca.ProductionRunConfigurator import ProductionRunConfigurator
from lsst.ctrl.orca.db.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class DagmanProductionRunConfigurator(BasicProductionRunConfigurator):
    ##
    # @brief finalize this production run, by creating a dagman file from
    # the template supplied in the policy file
    #
    def finalize(self, pipelineManagers):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:finalize")

        dagmanTemplate = self.policy.get("dagmanTemplate")

        dagConfigurator = DagConfigurator(self.runid, pipelineManagers)
        
        tempdir = os.path.join("/tmp", self.runid)

        dagmanFile = os.path.join(self.tempdir,"dagman_"+self.runid"+.dag")
        dagConfigurator.rewrite(dagmanTemplate, dagmanFile)
        return dagConfigurator.totalNodeCount()
