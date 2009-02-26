import os
import lsst.ctrl.orca as orca
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.pex.logging import Log

class PipelineManager:

    def __init__(self):
        self.defaultDomain = ""
        self.rootDir = ""

        self.pipeline = ""
        self.policy = None
        self.runId = ""

        self.logger = Log(Log.getDefaultLog(), "dc3pipe")

        self.masterNode = ""

    def configure(self, pipeline, policy, runId):
        self.logger.log(Log.DEBUG, "PipelineManager:configure")

        self.pipeline = pipeline
        self.policy = policy
        self.runId = runId

        self.defaultDomain = policy.get("platform.deploy.defaultDomain")
        self.logger.log(Log.DEBUG, "defaultDomain = "+self.defaultDomain)
        self.rootDir = policy.get("defRootDir")

        repository = os.path.join(os.environ["DC2PIPE_DIR"], "pipeline")

        if not os.path.exists(repository):
            raise RuntimeError(repository + ": directory not found");

        if not os.path.isdir(repository):
            raise RuntimeError(repository + ": not a directory");

        self.createDirectories()
        print "in superclass: self.dirs['work']: ",self.dirs["work"]
        self.nodes = self.createNodeList()
        self.createDatabase()
        self.deploySetup(repository)

    def createDatabase(self):
        classFactory = NamedClassFactory()
        databaseConfigName = self.policy.get("database.configuratorClass")
        self.logger.log(Log.DEBUG, "databaseConfigName = " + databaseConfigName)
        databaseConfiguratorClass = classFactory.createClass(databaseConfigName)
        databaseConfigurator = databaseConfiguratorClass()
        databaseConfigurator.configureDatabase(self.policy, self.runId)

    def createNodeList(self):
        self.logger.log(Log.DEBUG, "PipelineManager:createNodeList")

        node = self.policy.getArray("platform.deploy.nodes")

        nodes = map(self.expandNodeHost, node)
        # by convention, the master node is the first node in the list
        # we use this later to launch things, so strip out the info past ":", if it's there.
        self.masterNode = nodes[0]
        colon = self.masterNode.find(':')
        if colon > 1:
            self.masterNode = self.masterNode[0:colon]
        if orca.dryrun == False:
            nodelist = open(os.path.join(self.dirs["work"], "nodelist.scr"), 'w')
            for node in nodes:
                print >> nodelist, node
            nodelist.close()

        return nodes


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
                    self.logger.log(Log.DEBUG, "Suspiciously short node name: " + node)
                self.logger.log(Log.DEBUG, "-> nodeentry  =" + nodeentry)
                self.logger.log(Log.DEBUG, "-> node  =" + node)
                node += "."+self.defaultDomain
                nodeentry = "%s:%s" % (node, nodeentry[colon+1:])
            else:
                nodeentry = "%s%s:1" % (node, self.defaultDomain)

        self.logger.log(Log.DEBUG, "returning nodeentry = " + nodeentry)
        return nodeentry


    def createDirectories(self):
        self.logger.log(Log.DEBUG, "PipelineManager:createDirectories")

    def createDirectoryList(self):
        self.logger.log(Log.DEBUG, "PipelineManager:createDirectoryList")

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

    def deploySetup(self, repository):
        self.logger.log(Log.DEBUG, "PipelineManager:deploySetup")

    def launchPipeline(self):
        self.logger.log(Log.DEBUG, "PipelineManager:launchPipeline")
