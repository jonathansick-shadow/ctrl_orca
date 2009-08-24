import os, os.path, shutil, sets
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.pex.harness.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PipelineConfigurator import PipelineConfigurator
from lsst.ctrl.orca.BasicPipelineLauncher import BasicPipelineLauncher

class BasicPipelineConfigurator(PipelineConfigurator):
    def __init__(self, runid, logger, verbosity):
        self.runid = runid
        self.logger = logger
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:__init__")
        self.verbosity = verbosity

        self.nodes = None
        self.dirs = None
        self.policySet = sets.Set()


    def configure(self, policy, configurationInfo, repository):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:configure")
        self.policy = policy
        self.configurationInfo = configurationInfo
        self.repository = repository
        self.pipeline = self.policy.get("shortname")
        self.nodes = self.createNodeList()
        self.prepPlatform()
        self.deploySetup()
        self.setupDatabase()
        cmd = self.createLaunchCommand()
        pipelineLauncher = BasicPipelineLauncher(cmd, self.pipeline, self.logger, self.verbosity)
        return pipelineLauncher

    def createLaunchCommand(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:createLaunchCommand")

        execPath = self.policy.get("configuration.framework.exec")
        launchcmd = EnvString.resolve(execPath)
        configurationPolicyFile =  os.path.join(self.dirs.get("work"), self.configurationInfo[0])

        cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -L %s" % (self.dirs.get("work"), self.script, launchcmd, configurationPolicyFile, self.runid, self.verbosity) ]
        return cmd


    def createNodeList(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:createNodeList")
        node = self.policy.getArray("platform.deploy.nodes")
        self.defaultDomain = self.policy.get("platform.deploy.defaultDomain")

        nodes = map(self.expandNodeHost, node)
        # by convention, the master node is the first node in the list
        # we use this later to launch things, so strip out the info past ":", if it's there.
        self.masterNode = nodes[0]
        colon = self.masterNode.find(':')
        if colon > 1:
            self.masterNode = self.masterNode[0:colon]
        return nodes

    def prepPlatform(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:prepPlatform")
        self.createDirs()
        self.writeNodeList()

    def writeNodeList(self):
        nodelist = open(os.path.join(self.dirs.get("work"), "nodelist.scr"), 'w')
        for node in self.nodes:
            print >> nodelist, node
        nodelist.close()


    def deploySetup(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:deploySetup")

       # copy /bin/sh script responsible for environment setting

        setupPath = self.policy.get("configuration.framework.environment")
        if setupPath == None:
             raise RuntimeError("couldn't find configuration.framework.environment")
        self.script = EnvString.resolve(setupPath)

        ## TODO: We did this same thing in DC2. We shouldn't be
        ## depending the system we launch on to determine which version
        ## of the setup.*sh script to run.   The remote systems aren't
        ## guaranteed to be running the same shell as the interactive
        ## shell from which orca was launched.

        if orca.envscript == None:
            print "using default setup.sh"
        else:
            self.script = orca.envscript

        shutil.copy(self.script, self.dirs.get("work"))

        # now point at the new location for the setup script
        self.script = os.path.join(self.dirs.get("work"),os.path.basename(self.script))

        # This policy has the override values, but must be written out and
        # recorded.
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()
        # 
        # copy the policies to the working directory
        
        configurationFileName = self.configurationInfo[0]
        print "configurationFileName = ",configurationFileName
        configurationPolicy = self.configurationInfo[1]
        newPolicyFile = os.path.join(self.dirs.get("work"), configurationFileName)
        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN, "Working directory already contains %s")
        else:
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(configurationPolicy)
            pw.close()

        # TODO: Provenance script needs to write out newPolicyFile
        #self.provenance.recordPolicy(newPolicyFile)
        self.policySet.add(newPolicyFile)

        # TODO: cont'd - also needs to writeout child policies
        newPolicyObj = pol.Policy.createPolicy(newPolicyFile, False)
        pipelinePolicySet = sets.Set()
        self.extractChildPolicies(self.repository, newPolicyObj, pipelinePolicySet)

        if os.path.exists(os.path.join(self.dirs.get("work"), self.pipeline)):
            self.logger.log(Log.WARN,
              "Working directory already contains %s directory; won't overwrite" % self.pipeline)
        else:
            #shutil.copytree(os.path.join(self.repository, self.pipeline), os.path.join(self.dirs.get("work"),self.pipeline))
            #
            # instead of blindly copying the whole directory, take the set
            # if files from policySet and copy those.
            #
            # This is slightly tricky, because we want to copy from the policy file     
            # repository directory to the "work" directory, but we also want to keep    
            # that partial directory hierarchy we're copying to as well.
            #
            for filename  in pipelinePolicySet:
                destinationDir = self.dirs.get("work")
                destName = filename.replace(self.repository+"/","")
                tokens = destName.split('/')
                tokensLength = len(tokens)
                # if the destination directory heirarchy doesn't exist, create all          
                # the missing directories
                destinationFile = tokens[len(tokens)-1]
                for newDestinationDir in tokens[:len(tokens)-1]:
                    newDir = os.path.join(destinationDir, newDestinationDir)
                    if os.path.exists(newDir) == False:
                        os.mkdir(newDir)
                    destinationDir = newDir
                shutil.copyfile(filename, os.path.join(destinationDir, destinationFile))


    def createDirs(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:createDirs")

        dirPolicy = self.policy.getPolicy("platform.dir")
        directories = Directories(dirPolicy, self.pipeline, self.runid)
        self.dirs = directories.getDirs()

        for name in self.dirs.names():
            if not os.path.exists(self.dirs.get(name)): os.makedirs(self.dirs.get(name))


    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:setupDatabase")

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
        return nodeentry
        
    def extractChildPolicies(self, repos, policy, pipelinePolicySet):
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
                            #self.provenance.recordPolicy(filename)
                            self.policySet.add(filename)
                        if (filename in pipelinePolicySet) == False:
                            pipelinePolicySet.add(filename)
                        newPolicy = pol.Policy.createPolicy(filename, False)
                        self.extractChildPolicies(repos, newPolicy, pipelinePolicySet)
            else:
                field = name
                if policy.getValueType(field) == pol.Policy.FILE:
                    filename = policy.getFile(field).getPath()
                    filename = policy.getFile(field).getPath()
                    filename = os.path.join(repos, filename)
                    if (filename in self.policySet) == False:
                        #self.provenance.recordPolicy(filename)
                        self.policySet.add(filename)
                    if (filename in pipelinePolicySet) == False:
                        pipelinePolicySet.add(filename)
                    newPolicy = pol.Policy.createPolicy(filename, False)
                    self.extractChildPolicies(repos, newPolicy, pipelinePolicySet)
