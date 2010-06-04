import sys,os, os.path, shutil, sets, stat
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.pex.harness.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.PolicyUtils import PolicyUtils
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.GenericPipelineWorkflowLauncher import GenericPipelineWorkflowLauncher

##
#
# GenericPipelineWorkflowConfigurator 
#
class GenericPipelineWorkflowConfigurator(WorkflowConfigurator):
    def __init__(self, runid, prodPolicy, wfPolicy, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:__init__")
        self.runid = runid
        self.prodPolicy = prodPolicy
        self.wfPolicy = wfPolicy

        self.wfVerbosity = None

        self.nodes = None
        self.dirs = None
        self.defaultRunDir = None

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
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:configure")
        self.shortName = wfPolicy.get("shortName")
        if wfPolicy.getValueType("platform") == pol.Policy.FILE:
            filename = wfPolicy.getFile("platform").getPath()
            platformPolicy = pol.Policy.createPolicy(filename)
        else:
            platformPolicy = wfPolicy.getPolicy("platform")

        self.defaultDomain = platformPolicy.get("deploy.defaultDomain")
        pipelinePolicies = wfPolicy.getPolicyArray("pipeline")
        expandedPipelinePolicies = self.expandPolicies(self.shortName, pipelinePolicies)
        #launchCmd = {}
        launchCmd = []
        for pipelinePolicyGroup in expandedPipelinePolicies:
            pipelinePolicy = pipelinePolicyGroup[0]
            num = pipelinePolicyGroup[1]
            self.nodes = self.createNodeList(pipelinePolicy)
            self.createDirs(platformPolicy, pipelinePolicy)
            pipelineShortName = pipelinePolicyGroup[0].get("shortName")
            launchName = "%s_%d" % (pipelineShortName, num)
            self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator: launchName = %s" % launchName)
            #launchCmd[launchName] = self.deploySetup(provSetup, wfPolicy, pipelinePolicyGroup)
            val = self.deploySetup(provSetup, wfPolicy, platformPolicy, pipelinePolicyGroup)
            launchCmd.append(val)
            self.logger.log(Log.DEBUG, "launchCmd = %s" % launchCmd)
        self.deployData(wfPolicy)
        workflowLauncher = GenericPipelineWorkflowLauncher(launchCmd, self.prodPolicy, wfPolicy, self.runid, self.logger)
        return workflowLauncher

    ##
    # @brief given a list of pipelinePolicies, number the section we're 
    # interested in based on the order they are in, in the productionPolicy
    # We use this number Provenance to uniquely identify this set of pipelines
    #
    def expandPolicies(self, wfShortName, pipelinePolicies):
        # Pipeline provenance requires that "activoffset" be unique and 
        # sequential for each pipeline in the production.  Each workflow
        # in the production can have multiple pipelines, and even a call for
        # duplicates of the same pipeline within it.
        #
        # Since these aren't numbered within the production policy file itself,
        # we need to do this ourselves. This is slightly tricky, since each
        # workflow is handled individually by orca and had has no reference 
        # to the other workflows or the number of pipelines within 
        # those workflows.
        #
        # Therefore, what we have to do is go through and count all the 
        # pipelines in the other workflows so we can enumerate the pipelines
        # in this particular workflow correctly. This needs to be reworked.

        wfPolicies = self.prodPolicy.getArray("workflow")
        totalCount = 1
        for wfPolicy in wfPolicies:
            if wfPolicy.get("shortName") == wfShortName:
                # we're in the policy which needs to be numbered
               expanded = []
               for policy in pipelinePolicies:
                   # default to 1, if runCount doesn't exist
                   runCount = 1
                   if policy.exists("runCount"):
                       runCount = policy.get("runCount")
                   for i in range(0,runCount):
                       expanded.append((policy,i+1, totalCount))
                       totalCount = totalCount + 1
       
               return expanded
            else:
                policies = wfPolicy.getPolicyArray("pipeline")
                for policy in policies:
                    if policy.exists("runCount"):
                        totalCount = totalCount + policy.get("runCount")
                    else:
                        totalCount = totalCount + 1
        return None # should never reach here - this is an error
                
                

    ##
    # @brief creates a list of nodes from platform.deploy.nodes
    # @return the list of nodes
    #
    def createNodeList(self,  pipelinePolicy):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:createNodeList")
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
    def writeNodeList(self, logDir):
        
        # write this only for debug
        nodelist = open(os.path.join(logDir, "nodelist.scr"), 'w')
        #nodelist = open(os.path.join(self.dirs.get("work"), "nodelist.scr"), 'w')
        for node in self.nodes:
            print >> nodelist, node
        nodelist.close()

        p = pol.Policy()
        x = 0
        for node in self.nodes:
            p.set("node%d" % x, node)
            x = x + 1
        pw = pol.PAFWriter(os.path.join(logDir, "nodelist.paf"))
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

    def deployData(self, wfPolicy):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:deployData")

        # add data deploy here

        if wfPolicy.exists("configuration"):
            configuration = wfPolicy.get("configuration")
            if configuration.exists("deployData"):
                deployPolicy = configuration.get("deployData")
                dataRepository = deployPolicy.get("dataRepository")
                dataRepository = EnvString.resolve(dataRepository)
                deployScript = deployPolicy.get("script")
                deployScript = EnvString.resolve(deployScript)
                collection = deployPolicy.get("collection")
                
                if os.path.isfile(deployScript) == True:
                    runDir = os.path.join(self.defaultRootDir, self.runid)
                    deployCmd = [deployScript, runDir, dataRepository, collection]
                    print ">>> ",deployCmd
                    pid = os.fork()
                    if not pid:
                        os.execvp(deployCmd[0], deployCmd)
                    os.wait()[0]
                else:
                    self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:deployData: warning: script '%s' doesn't exist" % deployScript)
        # end data deploy here


    ##
    # @brief 
    #
    def deploySetup(self, provSetup, wfPolicy, platformPolicy, pipelinePolicyGroup):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:deploySetup")

        pipelinePolicy = pipelinePolicyGroup[0]
        shortName = pipelinePolicy.get("shortName")

        pipelinePolicyNumber = pipelinePolicyGroup[1]
        pipelineName = "%s_%d" % (shortName, pipelinePolicyNumber)

        globalPipelineOffset = pipelinePolicyGroup[2]
        # add things to the pipeline policy and write it out to "work"
        #self.rewritePipelinePolicy(pipelinePolicy)

        workDir = self.dirs.get("work")

        # create the subdirectory for the pipeline specific files
        logDir = os.path.join(self.dirs.get("work"), pipelineName)
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        
        # only write out the policyfile once
        filename = pipelinePolicy.getFile("definition").getPath()
        definitionPolicy = pol.Policy.createPolicy(filename, False)
        if pipelinePolicyNumber == 1:
            if platformPolicy.exists("dir"):
                definitionPolicy.set("execute.dir", platformPolicy.get("dir"))
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
        self.writeNodeList(logDir)

        # copy /bin/sh script responsible for environment setting

        setupPath = definitionPolicy.get("framework.environment")
        if setupPath == None:
             raise RuntimeError("couldn't find framework.environment")
        #self.script = EnvString.resolve(setupPath)
        self.script = setupPath

        if orca.envscript == None:
            print "using default setup.sh"
        else:
            self.script = orca.envscript

        # only copy the setup script once
        if pipelinePolicyNumber == 1:
            shutil.copy(self.script, workDir)

        # now point at the new location for the setup script
        self.script = os.path.join(workDir, os.path.basename(self.script))

        #
        # Write all policy files out to the work directory, 
        # but only do it once.
        
        if pipelinePolicyNumber == 1:
        
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
        #execCmd = EnvString.resolve(execPath)
        execCmd = execPath

        #cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -L %s" % (self.dirs.get("work"), self.script, execCmd, filename, self.runid, self.wfVerbosity) ]

        # write out the launch script
        # write out the script we use to kick things off
        launchName = "launch_%s.sh" % pipelineName

        name = os.path.join(logDir, launchName)


        launcher = open(name, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("export SHELL=/bin/sh\n")
        launcher.write("cd %s\n" % self.dirs.get("work"))
        launcher.write("source %s\n" % self.script)
        launcher.write("eups list 2>/dev/null | grep Setup >%s/eups-env.txt\n" % logDir)

        cmds = provSetup.getCmds()
        workflowPolicies = self.prodPolicy.getArray("workflow")

        repos = self.prodPolicy.get("repositoryDirectory")
        # append the other information we previously didn't have access to, but need for recording.
        for cmd in cmds:
            wfShortName = wfPolicy.get("shortName")
            cmd.append("--activityname=%s_%s" % (wfShortName, pipelineName))
            cmd.append("--platform=%s" % wfPolicy.get("platform").getPath())
            cmd.append("--localrepos=%s" % self.dirs.get("work"))
            workflowIndex = 1
            for wfPolicy in workflowPolicies:
                if wfPolicy.get("shortName") == wfShortName:
                    #cmd.append("--activoffset=%s" % workflowIndex)
                    cmd.append("--activoffset=%s" % globalPipelineOffset)
                    break
                workflowIndex = workflowIndex + 1
            launchCmd = ' '.join(cmd)

            # extract the pipeline policy and all the files it includes, and add it to the command
            filelist = provSetup.extractSinglePipelineFileNames(pipelinePolicy, repos, self.logger)
            fileargs = ' '.join(filelist)
            launcher.write("%s %s\n" % (launchCmd, fileargs))

        
        launcher.write("nohup %s %s %s -L %s --logdir %s >%s/launch.log 2>&1 &\n" % (execCmd, filename, self.runid, self.wfVerbosity, logDir, logDir))
        launcher.close()
        # make it executable
        os.chmod(name, stat.S_IRWXU)

        launchCmd = ["ssh", self.masterNode, name]

        # print "cmd to execute is: ",launchCmd
        return launchCmd

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self, platformPolicy, pipelinePolicy):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:createDirs")

        dirPolicy = platformPolicy.getPolicy("dir")
        dirName = pipelinePolicy.get("shortName")
        directories = Directories(dirPolicy, dirName, self.runid)
        self.dirs = directories.getDirs()
        self.defaultRootDir = directories.getDefaultRootDir()

        for name in self.dirs.names():
            localDirName = self.dirs.get(name)
            if not os.path.exists(localDirName):
                os.makedirs(localDirName)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:setupDatabase")

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
