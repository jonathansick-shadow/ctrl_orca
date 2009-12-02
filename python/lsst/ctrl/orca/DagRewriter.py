import re,os
class DagRewriter:

    def __init__(self, runid):
        self.tmpdir = os.path.join("/tmp", runid)

    def helpme(self, match):
      return os.path.join(self.tmpdir, match.group(1) + ".condor")

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
        

if __name__ == "__main__":
    dagre = DagRewriter("foobar")
    dagre.rewrite("dagsample.dag","/tmp/dagoutput.txt")
