import os
from NamedClassFactory import NamedClassFactory
class PipelineManager:

    def __init__(self):
        self.defaultDomain = ""
        self.rootDir = ""

        self.pipeline = ""
        self.policy = None
        self.runId = ""


    def configure(self, pipeline, policy, runId):
        print "PipelineManager:configure"

        self.pipeline = pipeline
        self.policy = policy
        self.runId = runId

        self.defaultDomain = policy.get("defaultDomain")
        print "defaultDomain = ",self.defaultDomain
        self.rootDir = policy.get("defRootDir")

        self.createDirectories()
        self.createNodeList()
        self.createDatabase()
        self.deploySetup()

    def createDatabase(self):
        classFactory = NamedClassFactory()
        databaseConfigName = self.policy.get("databaseConfigurator")
        print "databaseConfigName = ",databaseConfigName
        databaseConfiguratorClass = classFactory.createClass(databaseConfigName)
        databaseConfigurator = databaseConfiguratorClass()
        databaseConfigurator.configureDatabase(self.policy, self.runId)

    def createNodeList(self):
        print "PipelineManager:createNodeList"
        nodeArray = self.policy.getPolicy("nodelist")

        node = nodeArray.getArray("node")

        nodes = map(self.expandNodeHost, node)
        nodelist = open(os.path.join(self.workingDirectory, "nodelist.scr"), 'w')
        for node in nodes:
            print >> nodelist, node
        nodelist.close()


    def expandNodeHost(self, nodeentry):
        """Add a default network domain to a node list entry if necessary """

        if nodeentry.find(".") < 0:
            node = nodeentry
            colon = nodeentry.find(":")
            if colon == 0:
                raise RuntimeError("bad nodelist format: " + nodeentry)
            elif colon > 0:
                node = nodeentry[0:colon]
                if len(node) < 3:
                    #logger.log(Log.WARN, "Suspiciously short node name: " + node)
                    print "Suspiciously short node name: " + node
                print "-> nodeentry  =",nodeentry
                print "-> node  =",node
                node += self.defaultDomain
                nodeentry = "%s:%s" % (node, nodeentry[colon+1:])
            else:
                nodeentry = "%s%s:1" % (node, self.defaultDomain)

        print "returning nodeentry = ",nodeentry
        return nodeentry


    def createDirectories(self):
        print "PipelineManager:createDirectories"

    def createDirectoryList(self):
        print "PipelineManager:createDirectoryList"

        names = self.policy.get("directoryNames")
        dirs = []

        # construct the path of the pipeline below rootDir

        # Set the pipeline name.  If "shortName" is in the policy
        # file, use that, otherwise default to the pipeline name
        pdir = self.policy.get("shortName", self.pipeline)

        # Construct the name of the pipeline directory
        wdir = os.path.join(rootDir, runid, pdir)

        # For each of the names we passed into the method, create the path 
        # name for it.
        for name in names:
            dir = os.path.join(wdir, name)
            dirs.append(dir)
        
        # return the list of directories
        return dirs

    def deploySetup(self):
        print "PipelineManager:deploySetup"

    def launchPipeline(self):
        print "PipelineManager:launchPipeline"
