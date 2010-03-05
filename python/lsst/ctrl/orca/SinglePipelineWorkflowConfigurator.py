import sys,os, os.path, shutil, sets, stat
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.ctrl.orca.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.PolicyUtils import PolicyUtils
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.SinglePipelineWorkflowLauncher import SinglePipelineWorkflowLauncher

##
#
# SinglePipelineWorkflowConfigurator 
#
class SinglePipelineWorkflowConfigurator(WorkflowConfigurator):
    def __init__(self, runid, prodPolicy, wfPolicy, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "SinglePipelineWorkflowConfigurator:__init__")
        self.runid = runid
        self.prodPolicy = prodPolicy
        self.wfPolicy = wfPolicy

        self.wfVerbosity = None

        self.nodes = None
        self.dirs = None

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param provSetup
    # @param wfVerbosity
    #
    def configure(self, provSetup, wfVerbosity):
        self.wfVerbosity = wfVerbosity
        self._configureDatabases(provSetup)
        return self._configureSpecialized(provSetup, self.wfPolicy)
    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param policy the workflow policy to use for configuration
    # @param configurationDict a dictionary containing configuration info
    # @param provenanceDict a dictionary containing info to record provenance
    # @param repository policy file repository location
    #
    
    def _configureSpecialized(self, provSetup, wfPolicy):
        self.logger.log(Log.DEBUG, "SinglePipelineWorkflowConfigurator:configure")
        self.shortName = wfPolicy.get("shortName")
        if wfPolicy.getValueType("platform") == pol.Policy.FILE:
            filename = wfPolicy.getFile("platform").getPath()
            platformPolicy = pol.Policy.createPolicy(filename)
        else:
            platformPolicy = wfPolicy.getPolicy("platform")

        self.defaultDomain = platformPolicy.get("deploy.defaultDomain")
        pipelinePolicies = wfPolicy.getPolicyArray("pipeline")
        for pipelinePolicy in pipelinePolicies:
            self.nodes = self.createNodeList(pipelinePolicy)
            self.createDirs(platformPolicy, pipelinePolicy)
            launchCmd = self.deploySetup(provSetup, wfPolicy, pipelinePolicy)
            self.logger.log(Log.DEBUG, "launchCmd = %s" % launchCmd)
        workflowLauncher = SinglePipelineWorkflowLauncher(launchCmd, self.prodPolicy, wfPolicy, self.logger)
        return workflowLauncher


    ##
    # @brief creates a list of nodes from platform.deploy.nodes
    # @return the list of nodes
    #
    def createNodeList(self,  pipelinePolicy):
        self.logger.log(Log.DEBUG, "SinglePipelineWorkflowConfigurator:createNodeList")
        node = pipelinePolicy.getArray("deploy.processesOnNode")

        nodes = map(self.expandNodeHost, node)
        # by convention, the master node is the first node in the list
        # we use this later to launch things, so strip out the info past ":", if it's there.
        self.masterNode = nodes[0]
        colon = self.masterNode.find(':')
        if colon > 1:
            self.masterNode = self.masterNode[0:colon]
        return nodes

    def getWorkflowName(self):
        return self.workflow
    
    def getNodeCount(self):
        return len(self.nodes)

    ##
    # @brief write the node list to the "work" directory
    #
    def writeNodeList(self):
        
        # write this only for debug
        nodelist = open(os.path.join(self.dirs.get("work"), "nodelist.scr"), 'w')
        for node in self.nodes:
            print >> nodelist, node
        nodelist.close()

        p = pol.Policy()
        x = 0
        for node in self.nodes:
            p.set("node%d" % x, node)
            x = x + 1
        pw = pol.PAFWriter(os.path.join(self.dirs.get("work"), "nodelist.paf"))
        pw.write(p)
        pw.close()


    def rewritePipelinePolicy(self, pipelinePolicy):
        workDir = self.dirs.get("work")
        filename = pipelinePolicy.getFile("definition").getPath()
        oldPolicy = pol.Policy.createPolicy(filename, False)
        if self.prodPolicy.exists("eventBrokerHost"):
            oldPolicy.set("execute.eventBrokerHost", self.prodPolicy.get("eventBrokerHost"))

        if self.wfPolicy.exists("shutdownTopic"):
            oldPolicy.set("execute.shutdownTopic", self.wfPolicy.get("shutdownTopic"))
        if self.prodPolicy.exists("logThreshold"):
            oldPolicy.set("execute.logThreshold", self.prodPolicy.get("logThreshold"))
        newPolicyFile = os.path.join(workDir, filename)
        pw = pol.PAFWriter(newPolicyFile)
        pw.write(oldPolicy)
        pw.close()

    ##
    # @brief 
    #
    def deploySetup(self, provSetup, wfPolicy, pipelinePolicy):
        self.logger.log(Log.DEBUG, "SinglePipelineWorkflowConfigurator:deploySetup")

        # add things to the pipeline policy and write it out to "work"
        #self.rewritePipelinePolicy(pipelinePolicy)

        workDir = self.dirs.get("work")
        filename = pipelinePolicy.getFile("definition").getPath()
        definitionPolicy = pol.Policy.createPolicy(filename, False)
        if self.prodPolicy.exists("eventBrokerHost"):
            definitionPolicy.set("execute.eventBrokerHost", self.prodPolicy.get("eventBrokerHost"))

        if self.wfPolicy.exists("shutdownTopic"):
            definitionPolicy.set("execute.shutdownTopic", self.wfPolicy.get("shutdownTopic"))
        if self.prodPolicy.exists("logThreshold"):
            definitionPolicy.set("execute.logThreshold", self.prodPolicy.get("logThreshold"))
        newPolicyFile = os.path.join(workDir, filename)
        pw = pol.PAFWriter(newPolicyFile)
        pw.write(definitionPolicy)
        pw.close()

        
        # write the nodelist to "work"
        self.writeNodeList()

        # copy /bin/sh script responsible for environment setting


        setupPath = definitionPolicy.get("framework.environment")
        if setupPath == None:
             raise RuntimeError("couldn't find framework.environment")
        self.script = EnvString.resolve(setupPath)

        if orca.envscript == None:
            print "using default setup.sh"
        else:
            self.script = orca.envscript

        shutil.copy(self.script, workDir)

        # now point at the new location for the setup script
        self.script = os.path.join(workDir, os.path.basename(self.script))

        #
        # TODO: Write all policy files out to the work directory, 
        #
        
        # first, grab all the file names, and throw them into a Set() to 
        # avoid duplication
        pipelinePolicySet = sets.Set()
        repos = self.prodPolicy.get("repositoryDirectory")
        PolicyUtils.getAllFilenames(repos, definitionPolicy, pipelinePolicySet)

        # Cycle through the file names, creating subdirectories as required,
        # and copy them to the destination directory
        for policyFile in pipelinePolicySet:
            destName = policyFile.replace(repos+"/","")
            tokens = destName.split('/')
            tokensLength = len(tokens)
            destinationFile = tokens[len(tokens)-1]
            destintationDir = workDir
            for newDestinationDir in tokens[:len(tokens)-1]:
                newDir = os.path.join(workDir, newDestinationDir)
                if os.path.exists(newDir) == False:
                    os.mkdir(newDir)
                destinationDir = newDir
            shutil.copyfile(policyFile, os.path.join(destinationDir, destinationFile))


        # create the launch command
        execPath = definitionPolicy.get("framework.exec")
        execCmd = EnvString.resolve(execPath)

        #cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -L %s" % (self.dirs.get("work"), self.script, execCmd, filename, self.runid, self.wfVerbosity) ]

        # write out the launch script
        # write out the script we use to kick things off
        name = os.path.join(self.dirs.get("work"), "orca_launch.sh")


        launcher = open(name, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("cd %s\n" % self.dirs.get("work"))
        launcher.write("source %s\n" %self.script)
        launcher.write("eups list 2>/dev/null | grep Setup >eups-env.txt\n")

        cmds = provSetup.getCmds()
        workflowPolicies = self.prodPolicy.getArray("workflow")

        # append the other information we previously didn't have access to, but need for recording.
        for cmd in cmds:
            wfShortName = wfPolicy.get("shortName")
            cmd.append("--activityname=%s" % wfShortName)
            cmd.append("--platform=%s" % wfPolicy.get("platform").getPath())
            cmd.append("--localrepos=%s" % self.dirs.get("work"))
            workflowIndex = 1
            for wfPolicy in workflowPolicies:
                if wfPolicy.get("shortName") == wfShortName:
                    cmd.append("--activoffset=%s" % workflowIndex)
                    break
                workflowIndex = workflowIndex + 1
            launchCmd = ' '.join(cmd)

            # extract the pipeline policy and all the files it includes, and add it to the command
            filelist = provSetup.extractSinglePipelineFileNames(pipelinePolicy, repos, self.logger)
            fileargs = ' '.join(filelist)
            launcher.write("%s %s\n" % (launchCmd, fileargs))

        
        launcher.write("%s %s %s -L %s\n" % (execCmd, filename, self.runid, self.wfVerbosity))
        launcher.close()
        # make it executable
        os.chmod(name, stat.S_IRWXU)

        launchCmd = ["ssh", self.masterNode, name]

        print "cmd to execute is: ",launchCmd
        return launchCmd

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self, platformPolicy, pipelinePolicy):
        self.logger.log(Log.DEBUG, "SinglePipelineWorkflowConfigurator:createDirs")

        dirPolicy = platformPolicy.getPolicy("dir")
        dirName = pipelinePolicy.get("shortName")
        directories = Directories(dirPolicy, dirName, self.runid)
        self.dirs = directories.getDirs()

        for name in self.dirs.names():
            localDirName = self.dirs.get(name)
            if not os.path.exists(localDirName):
                os.makedirs(localDirName)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "SinglePipelineWorkflowConfigurator:setupDatabase")

    ##
    # @brief perform a node host name expansion
    #
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

                if self.defaultDomain is not None:
                    node += "."+self.defaultDomain
                nodeentry = "%s:%s" % (node, nodeentry[colon+1:])
            else:
                if self.defaultDomain is not None:
                    nodeentry = "%s%s:1" % (node, self.defaultDomain)
                else:
                    nodeentry = "%s:1" % node
        return nodeentry
