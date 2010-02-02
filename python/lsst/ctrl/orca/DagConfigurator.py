import re,os
import sys
class DagConfigurator:

    def __init__(self, runid, localScratch, workflowManagers):
        self.tmpdir = os.path.join(localScratch, runid)
        self.workflowManagers = workflowManagers
    
    def helpme(self, match):
        name = match.group(1)
        print "name = "+name
        return os.path.join(self.tmpdir, name + ".condor")

    def rewrite(self, inFile, outFile):
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
