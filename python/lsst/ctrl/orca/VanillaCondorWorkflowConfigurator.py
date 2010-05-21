import os, os.path, shutil, sets, stat
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.ctrl.orca.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PolicyUtils import PolicyUtils
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.VanillaCondorWorkflowLauncher import VanillaCondorWorkflowLauncher

##
#
# VanillaCondorWorkflowConfigurator 
#
class VanillaCondorWorkflowConfigurator(WorkflowConfigurator):
    def __init__(self, runid, prodPolicy, wfPolicy, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:__init__")

        self.runid = runid
        self.prodPolicy = prodPolicy
        self.wfPolicy = wfPolicy

        self.wfVerbosity = None

        self.dirs = None
        self.directories = None
        self.nodes = None
        self.numNodes = None

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
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:configure")

        self.remoteLoginName = wfPolicy.get("configuration.condorData.loginNode")
        self.remoteFTPName = wfPolicy.get("configuration.condorData.ftpNode")
        self.transferProtocolPrefix = wfPolicy.get("configuration.condorData.transferProtocol")+"://"+self.remoteFTPName
        self.localScratch = wfPolicy.get("configuration.condorData.localScratch")

        if wfPolicy.getValueType("platform") == pol.Policy.FILE:
            filename = wfPolicy.getFile("platform").getPath()
            platformPolicy = pol.Policy.createPolicy(filename)
        else:
            platformPolicy = wfPolicy.getPolicy("platform")

        self.repository = self.prodPolicy.get("repositoryDirectory")

        self.shortName = self.wfPolicy.get("shortName")

        pipelinePolicies = wfPolicy.getPolicyArray("pipeline")
        expandedPipelinePolicies = self.expandPolicies(self.shortName, pipelinePolicies)

        launchCmd = []
        for pipelinePolicyGroup in expandedPipelinePolicies:
            pipelinePolicy = pipelinePolicyGroup[0]
            num = pipelinePolicyGroup[1]
            self.createDirs(platformPolicy, pipelinePolicy)
            pipelineShortName = pipelinePolicy.get("shortName")
            launchName = "%s_%d" % (pipelineShortName, num)

            self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator: launchName = %s" % launchName)
            #launchCmd = self.deploySetup(pipelinePolicy)
            self.deploySetup(provSetup, wfPolicy, platformPolicy, pipelinePolicyGroup)
            condorFile = self.writeCondorFile(launchName, "%s.sh" % launchName)
            launchCmd.append(condorFile)
            self.setupDatabase()
        self.logger.log(Log.DEBUG, "launchCmd = %s" % launchCmd)

        # after all the pipelines are created, copy them to the remote location
        # all at once.
        remoteDir = os.path.join(self.directories.getDefaultRootDir(), self.runid)
        # the extra "/" is required below to copy the entire directory
        self.copyToRemote(self.localStagingDir+"/", remoteDir+"/")

        # TODO: run the linking script to the data 
        #self.deployData(wfPolicy)

        # make remote scripts executable;  this is required because the copy doesn't
        # retain the execution bit setting.
        for pipelinePolicyGroup in expandedPipelinePolicies:
            pipelinePolicy = pipelinePolicyGroup[0]
            num = pipelinePolicyGroup[1]

            pipelineShortName = pipelinePolicy.get("shortName")

            filename = "launch_%s_%d.sh" % (pipelineShortName, num)
            remoteDir = os.path.join(self.dirs.get("work"), "%s_%d" % (pipelineShortName, num))
            remoteFilename = os.path.join(remoteDir, filename)
            self.remoteChmodX(remoteFilename)
        
        workflowLauncher = VanillaCondorWorkflowLauncher("", self.shortName, self.logger)
        return workflowLauncher

    ##
    # @brief create the command which will launch the workflow
    # @return a string containing the shell commands to execute
    #
    def writeCondorFile(self, launchNamePrefix, launchScriptName):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:writeCondorFile")

        condorJobFile =  os.path.join(self.localStagingDir, launchNamePrefix+".condor")

        clist = []
        clist.append("universe=vanilla\n")
        clist.append("executable=%s/%s\n" % (launchNamePrefix, launchScriptName))
        clist.append("transfer_executable=false\n")
        clist.append("output=%s_Condor.out\n" % launchNamePrefix)
        clist.append("error=%s_Condor.err\n" % launchNamePrefix)
        clist.append("log=%s_Condor.log\n" % launchNamePrefix)
        clist.append("should_transfer_files = NO\n")
        clist.append("remote_initialdir="+self.dirs.get("work")+"\n")
        clist.append("Requirements = (FileSystemDomain != \"dummy\") && (Arch != \"dummy\") && (OpSys != \"dummy\") && (Disk != -1) && (Memory != -1)\n")
        clist.append("queue\n")

        # Create a file object: in "write" mode
        condorFILE = open(condorJobFile,"w")
        condorFILE.writelines(clist)
        condorFILE.close()

        return condorJobFile

    def getWorkflowName(self):
        return self.shortName

    
    def stageLocally(self, localName, remoteName):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:stageLocally")

        print "local name  = "+localName
        print "remote name  = "+remoteName

        localStageName = os.path.join(self.localStagingDir, remoteName)

        print "localName = %s, localStageName = %s\n",(localName,localStageName)
        if os.path.exists(os.path.dirname(localStageName)) == False:
            os.makedirs(os.path.dirname(localStageName))
        shutil.copyfile(localName, localStageName)

    def copyToRemote(self, localName, remoteName):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:copyToRemote")
        
        localNameURL = "%s%s" % ("file://",localName)
        remoteNameURL = "%s%s" % (self.transferProtocolPrefix, remoteName)

        cmd = "globus-url-copy -r -vb -cd %s %s " % (localNameURL, remoteNameURL)
        
        print "cmd = ",cmd
        # perform this copy from the local machine to the remote machine
        pid = os.fork()
        if not pid:
            os.execvp("globus-url-copy",cmd.split())
        os.wait()[0]

    def remoteChmodX(self, remoteName):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:remoteChmodX")
        cmd = "gsissh %s chmod +x %s" % (self.remoteLoginName, remoteName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]


    def remoteMkdir(self, remoteDir):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:remoteMkdir")
        cmd = "gsissh %s mkdir -p %s" % (self.remoteLoginName, remoteDir)
        print "running: "+cmd
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]
    ##
    # @brief 
    #
    def deploySetup(self, provSetup, wfPolicy, platformPolicy, pipelinePolicyGroup):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:deploySetup")

        pipelinePolicy = pipelinePolicyGroup[0]
        shortName = pipelinePolicy.get("shortName")

        pipelinePolicyNumber = pipelinePolicyGroup[1]
        pipelineName = "%s_%d" % (shortName, pipelinePolicyNumber)

        globalPipelineOffset = pipelinePolicyGroup[2]
        # add things to the pipeline policy and write it out to "work"
        #self.rewritePipelinePolicy(pipelinePolicy)

        #workDir = self.dirs.get("work")
        workDir = os.path.join(self.localStagingDir, "work")

        # create the subdirectory for the pipeline specific files
        #logDir = os.path.join(self.dirs.get("work"), pipelineName)
        logDir = os.path.join(workDir, pipelineName)
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
        #self.writeNodeList(logDir)

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
        self.script = os.path.join(self.dirs.get("work"), os.path.basename(self.script))

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
        remoteLogDir = os.path.join(self.dirs.get("work"), pipelineName)
        launcher.write("eups list 2>/dev/null | grep Setup >%s/eups-env.txt\n" % remoteLogDir)

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

        
        launcher.write("nohup %s %s %s -L %s --logdir %s >%s/launch.log 2>&1 &\n" % (execCmd, filename, self.runid, self.wfVerbosity, remoteLogDir, remoteLogDir))
        launcher.close()

        return

    def rewritePipelinePolicy(self, pipelinePolicy):
        localWorkDir = os.path.join(self.localStagingDir, "work")
        
        filename = pipelinePolicy.getFile("definition").getPath()
        oldPolicy = pol.Policy.createPolicy(filename, False)

        if self.prodPolicy.exists("eventBrokerHost"):
            oldPolicy.set("execute.eventBrokerHost", self.prodPolicy.get("eventBrokerHost"))

        if self.prodPolicy.exists("shutdownTopic"):
            oldPolicy.set("execute.shutdownTopic", self.prodPolicy.get("shutdownTopic"))

        if self.prodPolicy.exists("logThreshold"):
            oldPolicy.set("execute.logThreshold", self.prodPolicy.get("logThreshold"))

        newPolicyFile = os.path.join(localWorkDir, filename)
        pw = pol.PAFWriter(newPolicyFile)
        pw.write(oldPolicy)
        pw.close()


    ##
    # @brief write a shell script to launch a workflow
    #
    def createLaunchScript(self):

        localWorkDir = os.path.join(self.localStagingDir, "work")
        print "localWorkDir =", localWorkDir
        
        if not os.path.exists(localWorkDir):
            os.makedirs(localWorkDir)

        tempName = os.path.join(localWorkDir, self.remoteScript)
        launcher = open(tempName, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("echo \"Running SimpleLaunch\"\n")
        launcher.write("echo \"running:\"\n")
        launcher.write("echo $@\n")
        launcher.write("$@\n")
        launcher.close()


        # make it executable
        os.chmod(tempName, stat.S_IRWXU)

        self.launcherScript = tempName
        return

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self, platformPolicy, pipelinePolicy):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:createDirs")

        dirPolicy = platformPolicy.getPolicy("dir")
        dirName = pipelinePolicy.get("shortName")
        self.directories = Directories(dirPolicy, dirName, self.runid)
        self.dirs = self.directories.getDirs()

        remoteRootDir = self.directories.getDefaultRootDir()
        remoteRunDir = self.directories.getDefaultRunDir()
        suffix = remoteRunDir.split(remoteRootDir)
        self.localStagingDir = os.path.join(self.localScratch, suffix[1][1:])
        print "self.localStagingDir = ",self.localStagingDir

        for name in self.dirs.names():
            localDirName = os.path.join(self.localStagingDir, name)
    
            if not os.path.exists(localDirName):
                os.makedirs(localDirName)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:setupDatabase")

