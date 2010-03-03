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
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:__init__")

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
        return self._configureSpecialized(self.wfPolicy)

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param policy the workflow policy to use for configuration
    # @param configurationDict a dictionary containing configuration info
    # @param provenanceDict a dictionary containing info to record provenance
    # @param repository policy file repository location
    #
    def _configureSpecialized(self, wfPolicy):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:configure")

        self.remoteLoginName = wfPolicy.get("configuration.loginNode")
        self.remoteFTPName = wfPolicy.get("configuration.ftpNode")
        self.transferProtocolPrefix = wfPolicy.get("configuration.transferProtocol")+"://"+self.remoteFTPName
        self.localScratch = wfPolicy.get("configuration.localScratch")

        if wfPolicy.getValueType("platform") == pol.Policy.FILE:
            filename = wfPolicy.getFile("platform").getPath()
            platformPolicy = pol.Policy.createPolicy(filename)
        else:
            platformPolicy = wfPolicy.getPolicy("platform")

        self.remoteScript = "orca_launch.sh"

        self.repository = self.prodPolicy.get("repositoryDirectory")

        self.workflow = self.wfPolicy.get("shortName")

        pipelinePolicies = wfPolicy.getPolicyArray("pipeline")
        for pipelinePolicy in pipelinePolicies:
            self.createDirs(platformPolicy, pipelinePolicy)
            self.createLaunchScript()
            launchCmd = self.deploySetup(pipelinePolicy)
            condorFile = self.writeCondorFile(pipelinePolicy)
            self.setupDatabase()

        
        workflowLauncher = VanillaCondorWorkflowLauncher("", self.workflow, self.logger)
        return workflowLauncher

    ##
    # @brief create the command which will launch the workflow
    # @return a string containing the shell commands to execute
    #
    def writeCondorFile(self, pipelinePolicy):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:writeCondorFile")

        filename = None
        if pipelinePolicy.getValueType("definition") == pol.Policy.FILE:
            filename = pipelinePolicy.getFile("definition").getPath()
            definitionPolicy = pol.Policy.createPolicy(filename, False)
        else:
            definitionPolicy = pipelinePolicy.getPolicy("definition")

        execPath = definitionPolicy.get("framework.exec")
        #launchcmd = EnvString.resolve(execPath)
        launchcmd = execPath

        setupScriptBasename = os.path.basename(orca.envscript)
        remoteSetupScriptName = os.path.join(self.dirs.get("work"), setupScriptBasename)
       
        shortName = pipelinePolicy.get("shortName")

        if filename == None:
            policyName = filename
        else:
            policyName = shortName
        launchArgs = "%s %s -L %s -S %s" % \
             (policyName+".paf", self.runid, self.wfVerbosity, remoteSetupScriptName)
        print "launchArgs = %s",launchArgs


        # get the name of this pipeline

        shortName = pipelinePolicy.get("shortName")
        # Write Condor file 
        condorJobfile =  os.path.join(self.localStagingDir, shortName+".condor")

        clist = []
        clist.append("universe=vanilla\n")
        clist.append("executable="+self.remoteScript+"\n")
        clist.append("arguments="+launchcmd+" "+launchArgs+"\n")
        clist.append("transfer_executable=false\n")
        clist.append("output="+shortName+"_Condor.out\n")
        clist.append("error="+shortName+"_Condor.err\n")
        clist.append("log="+shortName+"_Condor.log\n")
        clist.append("should_transfer_files = YES\n")
        clist.append("when_to_transfer_output = ON_EXIT\n")
        clist.append("remote_initialdir="+self.dirs.get("work")+"\n")
        clist.append("Requirements = (FileSystemDomain != \"dummy\") && (Arch != \"dummy\") && (OpSys != \"dummy\") && (Disk != -1) && (Memory != -1)\n")
        clist.append("queue\n")

        # Create a file object: in "write" mode
        condorFILE = open(condorJobfile,"w")
        condorFILE.writelines(clist)
        condorFILE.close()

        return

    def getWorkflowName(self):
        return self.workflow

    
    def stageLocally(self, localName, remoteName):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:stageLocally")

        print "local name  = "+localName
        print "remote name  = "+remoteName

        localStageName = os.path.join(self.localStagingDir, remoteName)

        print "localName = %s, localStageName = %s\n",(localName,localStageName)
        if os.path.exists(os.path.dirname(localStageName)) == False:
            os.makedirs(os.path.dirname(localStageName))
        shutil.copyfile(localName, localStageName)

    def copyToRemote(self, localName, remoteName):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:copyToRemote")
        
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
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:remoteChmodX")
        cmd = "gsissh %s chmod +x %s" % (self.remoteLoginName, remoteName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]


    def remoteMkdir(self, remoteDir):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:remoteMkdir")
        cmd = "gsissh %s mkdir -p %s" % (self.remoteLoginName, remoteDir)
        print "running: "+cmd
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]

    ##
    # @brief 
    #
    def deploySetup(self, pipelinePolicy):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:deploySetup")

        # copy /bin/sh script responsible for environment setting

        if pipelinePolicy.getValueType("definition") == pol.Policy.FILE:
            filename = pipelinePolicy.getFile("definition").getPath()
            definitionPolicy = pol.Policy.createPolicy(filename, False)
        else:
            definitionPolicy = pipelinePolicy.getPolicy("definition")

        setupPath = definitionPolicy.get("framework.environment")
        if setupPath == None:
             raise RuntimeError("couldn't find framework.environment")
        script = EnvString.resolve(setupPath)

        if orca.envscript == None:
            print "using default setup.sh"
        else:
            script = orca.envscript

        setupLineList = script.split("/")
        lengthLineList = len(setupLineList)
        setupShortName = setupLineList[lengthLineList-1]
        setupShortName = os.path.join("work",setupShortName)

        # stage the setup script into place on the remote resource
        self.stageLocally(script, setupShortName)

        # This policy has the override values, but must be written out and
        # recorded.
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()
        # 
        # copy the policies to the working directory
        
        self.rewritePipelinePolicy(pipelinePolicy)


        pipelinePolicySet = sets.Set()
        repos = self.prodPolicy.get("repositoryDirectory")
        PolicyUtils.getAllFilenames(repos, definitionPolicy, pipelinePolicySet)

        for filename in pipelinePolicySet:
            print "working on filename = "+filename
            print "working on repository = "+repos

            localStageNames = filename.split(repos)
            localStageName = "work"+localStageNames[1]
            localStageName = os.path.join(self.localStagingDir, localStageName)
            print "localStageName = "+localStageName
            self.stageLocally(filename, localStageName)

        shortName = pipelinePolicy.get("shortName")
        remoteDir = os.path.join(self.directories.getDefaultRootDir(), self.runid)
        remoteDir = os.path.join(remoteDir, shortName)

        # the extra "/" is required below to copy the entire directory
        self.copyToRemote(self.localStagingDir+"/", remoteDir+"/")

        # make the remote script executable;  this is required because the copy doesn't
        # retain the execution bit setting.
        filename = os.path.join(self.dirs.get("work"), self.remoteScript)
        self.remoteChmodX(filename)

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
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:createDirs")

        dirPolicy = platformPolicy.getPolicy("dir")
        dirName = pipelinePolicy.get("shortName")
        self.directories = Directories(dirPolicy, dirName, self.runid)
        self.dirs = self.directories.getDirs()

        remoteRootDir = self.directories.getDefaultRootDir()
        remoteRunDir = self.directories.getDefaultRunDir()
        suffix = remoteRunDir.split(remoteRootDir)
        self.localStagingDir = os.path.join(self.localScratch, suffix[1][1:])

        for name in self.dirs.names():
            localDirName = os.path.join(self.localStagingDir, name)
            if not os.path.exists(localDirName):
                os.makedirs(localDirName)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:setupDatabase")
