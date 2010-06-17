#!/usr/bin/env python
import os, sys
import subprocess
import optparse
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

class JobKiller:
    def __init__(self):
        return

    def kill(self, filename):
        try :
            input = open(filename, 'r')
        except Exception, e:
            # couldn't find that file, so pass
            return
        line = input.readline()
        line = line.strip('\n')
        cmd = ["condor_rm", line]
        jobname = os.path.basename(filename).split('.')[0]
        print "killing %s" % jobname
        pid = os.fork()
        if not pid:
            os.execvp(cmd[0], cmd)
        os.wait()[0]
        return


if __name__ == "__main__":
    usage = """usage %prog [-w workflow[-p pipeline [-n pipelineNum]] productionPolicyFile runId"""

    parser = optparse.OptionParser(usage)

    parser.add_option("-w", "--workflow", action="store", dest="workflowArg", default=None, help="workflow shortname")
    parser.add_option("-p", "--pipeline", action="store", dest="pipelineArg", default=None, help="pipeline shortname")
    parser.add_option("-n", "--pipelineNum", action="store", dest="pipelineNumArg", default=None, help="pipeline number")

    parser.opts = {}
    parser.args = []

    (parser.opts, parser.args) = parser.parse_args()

    workflowArg = parser.opts.workflowArg
    pipelineArg = parser.opts.pipelineArg
    pipelineNumArg = parser.opts.pipelineNumArg

    if len(parser.args) < 2:
        print usage
        raise RuntimeError("Missing args: productionPolicyFile runId")


    
    prodPolicyFile = parser.args[0]
    runid = parser.args[1]

    prodPolicy = pol.Policy.createPolicy(prodPolicyFile)

    wfn = WorkflowNumerator(prodPolicy)

    policyGroups = wfn.expandPolicies()

    killer = JobKiller()
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

        if workflowArg == None:
            killer.kill(fullname)
        elif workflowArg == group.getWorkflowName():
            if pipelineArg == None:
                killer.kill(fullname)
            elif pipelineArg == pipelineName:
                if pipelineNumArg == None:
                    killer.kill(fullname)
                elif pipelineNumArg == pipelineNum:
                    killer.kill(fullname)
        
