##
# @brief
#
class ProvenanceSetup:
    def __init__(self):
        self.provenanceRecorders = []
        self.policyFilenames = []
        self.workflowRecordCommands = []

    ##
    # @brief
    #
    def recordProduction(self):
        for provenanceRecorder in self.provenanceRecorders:
            for policyFilename in self.policyFilenames:
                provenanceRecorder.record(policyFilename);
        return

    ##
    # @brief
    #
    def addProductionRecorder(self, provenanceRecorder):
        self.provenanceRecordes.append(provenanceRecorder)
        return
    ##
    # @brief
    #
    def addWorkflowRecordCmd(self, string):
        self.workflowRecordCommands.append(string)
        return
