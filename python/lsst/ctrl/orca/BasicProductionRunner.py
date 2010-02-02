from lsst.ctrl.orca.ProductionRunner import ProductionRunner

##
# @brief launches workflows
#
class BasicProductionRunner(ProductionRunner):
    def __init__(self, runid, policy, workflowManagers):
        self.runid = runid
        self.policy = policy
        self.workflowManagers = workflowManagers

    def runWorkflows(self):
        for workflowManager in self.workflowManagers:
            workflowManager.runWorkflow()
