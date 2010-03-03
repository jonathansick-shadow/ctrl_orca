from lsst.ctrl.orca.provenance.Recorder import Recorder
from lsst.ctrl.orca.provenance.Provenance import Provenance

class BasicRecorder(Recorder):
    def __init__(self, configurationDict):
        self.user = configurationDict["user"]
        self.runid = configurationDict["runid"]
        self.dbRun = configurationDict["dbrun"]
        self.dbGlobal = configurationDict["dbglobal"]

        self.provenance = Provenance(self.user, self.runid, self.dbRun, self.dbGlobal)
        return
        
    def recordPolicy(self, filename):

        self.provenance.recordPolicy(filename)

        return
