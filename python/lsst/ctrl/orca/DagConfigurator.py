import re,os
import sys
class DagConfigurator:

    def __init__(self, runid, pipelineManagers):
        self.tmpdir = os.path.join("/tmp", runid)
        self.pipelineManagers = pipelineManagers
        self.totalNodeCount = 0

    
    def helpme(self, match):
        name = match.group(1)
        print "name = "+name
        # this counts the number of nodes when a pipeline is substituted.
        for pipelineManager in self.pipelineManagers:
            if pipelineManager.getPipelineName() == name:
                self.totalNodeCount = self.totalNodeCount + pipelineManager.getNodeCount()
                print "pipelineManager.getNodeCount = %d" % pipelineManager.getNodeCount()
        return os.path.join(self.tmpdir, name + ".condor")

    def rewrite(self, inFile, outFile):
        self.totalNodeCount = 0
        input = open(inFile,"r")
        output = open(outFile,"w")
        while True:
            line = input.readline()
            if not line:
                break
            rewritten = re.subn("lsstpipe://(\S*)",self.helpme,line)
            output.write(rewritten[0])
        input.close()
        output.close()
        print "totalNodeCount after rewrite = %d" % self.totalNodeCount

    def getTotalNodeCount(self):
        return self.totalNodeCount
