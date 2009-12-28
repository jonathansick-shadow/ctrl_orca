##
# @brief launches pipelines
#
class BasicProductionRunner:
    def __init__(self, runid, policy, pipelineManagers):
        self.runid = runid
        self.policy = policy
        self.pipelineManagers = pipelineManagers

    def runPipelines(self):
        for pipelineManager in self.pipelineManagers:
            pipelineManager.runPipeline()
