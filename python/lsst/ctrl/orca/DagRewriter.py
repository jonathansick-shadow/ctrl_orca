import re,os
class DagRewriter:


    def helpme(self, match ):
      return match.group(1) + ".condor"

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
    dagre = DAGRewriter()
    dagre.rewrite("dagsample.dag","/tmp/dagoutput.txt")
