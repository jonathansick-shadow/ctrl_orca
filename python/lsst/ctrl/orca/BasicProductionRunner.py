##
# @brief launches pipelines
#
class BasicProductionRunner:
    def __init__(self, runid, nodeCount, pipelineManagers):
        self.runid = runid
        self.nodeCount = nodeCount
        self.pipelineManagers = pipelineManagers

    def runPipelines(self):
        for pipelineManager in self.pipelineManagers:
            pipelineManager.runPipeline()
