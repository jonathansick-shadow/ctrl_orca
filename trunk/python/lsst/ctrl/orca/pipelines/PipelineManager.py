import os
import lsst.ctrl.orca as orca
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.dbservers.DatabaseConfigurator import DatabaseConfigurator
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
        self.dbConfigurator = None

    def checkConfiguration(self):
        self.logger.log(Log.DEBUG, "PipelineManager:checkConfiguration")

    def configure(self, pipeline, policy, runId, repository, provenance, dbRunURL):
        self.logger.log(Log.DEBUG, "PipelineManager:configure")

        self.pipeline = pipeline
        self.policy = policy
        self.runId = runId
        self.repository = repository
        self.provenance = provenance
        self.dbRunURL = dbRunURL

        self.defaultDomain = policy.get("platform.deploy.defaultDomain")
        self.logger.log(Log.DEBUG, "defaultDomain = "+self.defaultDomain)
        self.rootDir = policy.get("defRootDir")

        self.createDirectories()
        print "in superclass: self.dirs['work']: ",self.dirs["work"]
        self.nodes = self.createNodeList()
        #dbNames = self.createDatabase()
        self.deploySetup()

    #def createDatabase(self):
    #    classFactory = NamedClassFactory()
    #    databaseConfigName = self.policy.get("databaseConfig.configuratorClass")
    #
    #    dbPolicy = self.policy.getPolicy("databaseConfig.database")
    #    dbType = self.policy.get("databaseConfig.type")
    #
    #    self.logger.log(Log.DEBUG, "databaseConfigName = " + databaseConfigName)
    #    #databaseConfiguratorClass = classFactory.createClass(databaseConfigName)
    #
    #   #self.databaseConfigurator = databaseConfiguratorClass(dbType, dbPolicy)
    #   self.dbConfigurator = DatabaseConfigurator(dbType, dbPolicy)
    #   self.dbConfigurator.checkConfiguration(dbPolicy)
    #   dbNames = self.dbConfigurator.prepareForNewRun(self.runId)
    #   return dbNames

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

    def deploySetup(self):
        self.logger.log(Log.DEBUG, "PipelineManager:deploySetup")

    def launchPipeline(self):
        self.logger.log(Log.DEBUG, "PipelineManager:launchPipeline")
