from lsst.ctrl.orca.provenance.ProvenanceRecorder import ProvenanceRecorder
class BasicProvenanceRecorder(ProvenanceRecorder):
    def __init__(self, configurationDict):
        self.user = configurationDict["user"]
        self.runid = configurationDict["runid"]
        self.dbRun = configurationDict["dbrun"]
        self.dbGlobal = configurationDict["dbglobal"]

        #    self.provenance = Provenance(self.user, self.runid, self.dbRun, self.dbGlobal)
        print "user = ",self.user
        print "runid = ",self.runid
        print "dbRun = ",self.dbRun
        print "dbGlobal = ",self.dbGlobal
        return
        
    def recordPolicy(self, filename):

        #self.provenance.recordPolicy(filename)
        print "would record ->",filename

        return
