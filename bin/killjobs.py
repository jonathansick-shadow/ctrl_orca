import os, sys
import lsst.pex.policy as pol

class WorkflowNumerator:

    class PolicyGroup:
        def __init__(self, wfName, localScratchDir, pipelineName, number):
            self.wfName = wfName
            self.localScratchDir = localScratchDir
            self.pipelineName = pipelineName
            self.pipelineNumber = number

        def getWorkflowName(self):
            return self.wfName

        def getLocalScratchDir(self):
            return self.localScratchDir

        def getPipelineName(self):
            return self.pipelineName

        def getPipelineNumber(self):
            return self.pipelineNumber

    def __init__(self, prodPolicy):
        self.prodPolicy = prodPolicy

    ##
    # @brief given a list of pipelinePolicies, number the section we're 
    # interested in based on the order they are in, in the productionPolicy
    # We use this number Provenance to uniquely identify this set of pipelines
    #   
    def expandPolicies(self):
        wfPolicies = self.prodPolicy.getArray("workflow")
        expanded = []
        for wfPolicy in wfPolicies:
            wfShortName = wfPolicy.get("shortName")
            pipelinePolicies = wfPolicy.getArray("pipeline")
            localScratch = self.getLocalScratch(wfPolicy)
            for policy in pipelinePolicies:
                runCount = 1  # default to 1, if runCount doesn't exist
                if policy.exists("runCount"):
                    runCount = policy.get("runCount")
                for i in range(0,runCount):
                    pipelineName = policy.get("shortName")
                    expanded.append(self.PolicyGroup(wfShortName, localScratch, pipelineName,i+1))
        
        return expanded

    def getLocalScratch(self, wfPolicy):
        configurationPolicy = wfPolicy.get("configuration")
        condorDataPolicy = configurationPolicy.get("condorData")
        localScratch = condorDataPolicy.get("localScratch")
        return localScratch



if __name__ == "__main__":
    prodPolicyFile = sys.argv[1]
    runid = sys.argv[2]

    prodPolicy = pol.Policy.createPolicy(prodPolicyFile)

    wfn = WorkflowNumerator(prodPolicy)

    policyGroups = wfn.expandPolicies()

    for group in policyGroups:
        name = group.getPipelineName()
        num = group.getPipelineNumber()
        indexName = "%s_%s" % (name, num)
        pipelineJobFile = "%s.job" % indexName
        localScratchDir = group.getLocalScratchDir()
        localRunDir = os.path.join(localScratchDir, runid)
        localWorkflowDir = os.path.join(localRunDir, group.getWorkflowName())
        fullname = os.path.join(localWorkflowDir, indexName)
        fullname = os.path.join(fullname, "work")
        fullname = os.path.join(fullname, pipelineJobFile)
        print fullname

