import os, os.path
import sets
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.dbservers.DatabaseConfigurator import DatabaseConfigurator
from lsst.pex.logging import Log


class PipelineManager:

    def __init__(self, pipeVerb=None, logger=None):
        """
        create a generic PipelineManager
        @param pipeVerb  the verbosity level to pass onto the pipelines for 
                            pipeline messages
        @param logger    the caller's Log instance from which this manager can.
                            create a child Log
        """
        self.defaultDomain = ""
        self.rootDir = ""

        self.pipeline = ""
        self.policy = None
        self.runId = ""

        if logger is None:  logger = orca.logger
        self.logger = Log(logger, "pipelineMgr")
        self.pipelineVerbosity = pipeVerb

        self.masterNode = ""
        self.dbConfigurator = None

    def checkConfiguration(self):
        self.logger.log(Log.DEBUG, "PipelineManager:checkConfiguration")

    def configureDatabase(self):
        self.logger.log(Log.DEBUG, "PipelineManager:configureDatabase")

    def configure(self, pipeline, policy, runId, repository, provenance, dbRunURL, policySet, prodPolicyOverrides):
        self.logger.log(Log.DEBUG, "PipelineManager:configure")

        # TODO: redesign this ....many too many arguments to this method
        self.pipeline = pipeline
        self.policy = policy
        self.runId = runId
        self.repository = repository
        self.provenance = provenance
        self.dbRunURL = dbRunURL
        self.policySet = policySet
        self.prodPolicyOverrides = prodPolicyOverrides

        self.defaultDomain = policy.get("platform.deploy.defaultDomain")
        self.logger.log(Log.DEBUG, "defaultDomain = "+self.defaultDomain)
        self.rootDir = policy.get("defRootDir")

        self.createDirectories()
        self.nodes = self.createNodeList()
    
        self.deploySetup()

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
            nodelist = open(os.path.join(self.dirs.get("work"), "nodelist.scr"), 'w')
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


    def recordChildPolicies(self, repos, policy, pipelinePolicySet):
        names = policy.fileNames()
        for name in names:
            if name.rfind('.') > 0:
                desc = name[0:name.rfind('.')]
                field = name[name.rfind('.')+1:]
                policyObjs = policy.getPolicyArray(desc)
                for policyObj in policyObjs:
                    if policyObj.getValueType(field) == pol.Policy.FILE:
                        filename = policyObj.getFile(field).getPath()
                        filename = os.path.join(repos, filename)
                        if (filename in self.policySet) == False:
                            self.provenance.recordPolicy(filename)
                            self.policySet.add(filename)
                        if (filename in pipelinePolicySet) == False:
                            pipelinePolicySet.add(filename)
                        newPolicy = pol.Policy.createPolicy(filename, False)
                        self.recordChildPolicies(repos, newPolicy, pipelinePolicySet)
            else:
                field = name
                if policy.getValueType(field) == pol.Policy.FILE:
                    filename = policy.getFile(field).getPath()
                    filename = policy.getFile(field).getPath()
                    filename = os.path.join(repos, filename)
                    if (filename in self.policySet) == False:
                        self.provenance.recordPolicy(filename)
                        self.policySet.add(filename)
                    if (filename in pipelinePolicySet) == False:
                        pipelinePolicySet.add(filename)
                    newPolicy = pol.Policy.createPolicy(filename, False)
                    self.recordChildPolicies(repos, newPolicy, pipelinePolicySet)
