import sys, os, os.path, shutil, sets, stat, socket
from sets import Set
import getpass
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.pex.harness.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PolicyUtils import PolicyUtils
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.VanillaCondorWorkflowLauncher import VanillaCondorWorkflowLauncher
from lsst.ctrl.orca.TemplateWriter import TemplateWriter
from lsst.ctrl.orca.FileWaiter import FileWaiter

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
        self.logFileNames = []

        self.directoryList = {}
        self.initialWorkDir = None
        
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
        self.repository = EnvString.resolve(self.repository)

        self.shortName = self.wfPolicy.get("shortName")

        pipelinePolicies = wfPolicy.getPolicyArray("pipeline")
        expandedPipelinePolicies = self.expandPolicies(self.shortName, pipelinePolicies)

        # Collect the entire set of directories to create, and make them.
        # This avoids needlessly making connections to Abe for directories
        # that already exist.
        dirSet = Set()
        for pipelinePolicyGroup in expandedPipelinePolicies:
            pipelinePolicy = pipelinePolicyGroup.getPolicyName()
            num = pipelinePolicyGroup.getPolicyNumber()
            self.collectDirNames(dirSet, platformPolicy, pipelinePolicy, num)

        # create all the directories we've collected, plus any local
        # directories we need.
        self.createDirs(dirSet)

        jobs = []
        firstGroup = True
        glideinFileName = None
        remoteFileWaiterName = None
        linkScriptname = None

        self.localStagingDir = os.path.join(self.localScratch, self.runid)
        self.localWorkflowDir = os.path.join(self.localStagingDir, self.shortName)
        for pipelinePolicyGroup in expandedPipelinePolicies:
            pipelinePolicy = pipelinePolicyGroup.getPolicyName()
            num = pipelinePolicyGroup.getPolicyNumber()

            pipelineShortName = pipelinePolicy.get("shortName")

            # set this pipeline's self.directories and self.dirs
            indexName = "%s_%d" % (pipelineShortName, num)

            self.directories = self.directoryList[indexName]
            self.dirs = self.directories.getDirs()

            indexedNamedDir = os.path.join(self.localWorkflowDir, indexName)
            self.localWorkDir = os.path.join(indexedNamedDir, "work")

            if not os.path.exists(self.localWorkDir):
                os.makedirs(self.localWorkDir)

            self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator: indexName = %s" % indexName)
            #launchCmd = self.deploySetup(pipelinePolicy)
            self.deploySetup(provSetup, wfPolicy, platformPolicy, pipelinePolicyGroup)
            condorFile = self.writeCondorFile(indexName, "launch_%s.sh" % indexName)
            launchCmd = ["condor_submit", condorFile]
            jobs.append(condorFile)
            self.setupDatabase()

            # There are some things that are only added to the first work directory
            if firstGroup == True:
                self.initialWorkDir = self.localWorkDir
                self.createCondorDir(self.localWorkDir)

                # copy the $CTRL_ORCA_DIR/etc/condor_glidein_config to local
                condorGlideinConfig = EnvString.resolve("$CTRL_ORCA_DIR/etc/glidein_condor_config")
                condorDir = os.path.join(self.localWorkDir,"Condor_glidein")
                stagedGlideinConfigFile = os.path.join(condorDir, "glidein_condor_config")

                keyvalues = pol.Policy()
                keyvalues.set("ORCA_LOCAL_HOSTNAME", socket.gethostname())
            
                writer = TemplateWriter()
                writer.rewrite(condorGlideinConfig, stagedGlideinConfigFile, keyvalues)

                # write the glidein request script
                glideinFileName = self.writeGlideinRequest(wfPolicy.get("configuration"))

                # copy the file creation watching utility
                remoteFileWaiterName = self.copyFileWaiterUtility()

                # copy the link script once, to the first working directory
                linkScriptName = self.copyLinkScript(wfPolicy)

                firstGroup = False
            
            self.logger.log(Log.DEBUG, "launchCmd = %s" % launchCmd)

            # after all the pipeline files are placed, copy them to the remote location
            # all at once.
            remoteDir = self.dirs.get("work")
            # the extra "/" is required below to copy the entire directory
            #self.copyToRemote(self.localStagingDir+"/*", remoteDir+"/")
            self.copyToRemote(self.localWorkDir+"/*", remoteDir+"/")

            filename = "launch_%s_%d.sh" % (pipelineShortName, num)
            remoteDir = os.path.join(self.dirs.get("work"), "%s_%d" % (pipelineShortName, num))
            remoteFilename = os.path.join(remoteDir, filename)
            self.remoteChmodX(remoteFilename)
        

        # Now that all the input directories are made, run the link script
        if linkScriptName != None:
            self.runLinkScript(wfPolicy, linkScriptName)

        # create the FileWaiter
        fileWaiter = FileWaiter(self.remoteLoginName, remoteFileWaiterName, self.logFileNames, self.logger)

        # create the Launcher

        workflowLauncher = VanillaCondorWorkflowLauncher(jobs, self.initialWorkDir, glideinFileName,  self.prodPolicy, self.wfPolicy, self.runid, fileWaiter, self.logger)
        return workflowLauncher

    ##
    # @brief create the command which will launch the workflow
    # @return a string containing the shell commands to execute
    #
    def writeCondorFile(self, launchNamePrefix, launchScriptName):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:writeCondorFile")

        condorJobFile = os.path.join(self.localWorkDir, launchNamePrefix)
        condorJobFile = os.path.join(condorJobFile, launchNamePrefix+".condor")

        clist = []
        clist.append("universe=vanilla\n")
        clist.append("executable=%s/%s\n" % (launchNamePrefix, launchScriptName))
        clist.append("transfer_executable=false\n")
        clist.append("output=%s/%s/Condor.out\n" % (self.localWorkDir, launchNamePrefix))
        clist.append("error=%s/%s/Condor.err\n" % (self.localWorkDir, launchNamePrefix))
        clist.append("log=%s/%s/Condor.log\n" % (self.localWorkDir, launchNamePrefix))
        clist.append("should_transfer_files = YES\n")
        clist.append("when_to_transfer_output = ON_EXIT\n")
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
        print cmd
        
        # perform this copy from the local machine to the remote machine
        pid = os.fork()
        if not pid:
            # when forking stuff, gotta close *BOTH* the python and C level 
            # file descriptors. not strictly needed here, since we're just
            # shutting off stdout and stderr, but a good habit to be in.

            # TODO: Change this to add a check to not close file descriptors
            # if verbosity is set high enough, so you can see the output of the 
            # globus-url-copy
            sys.stdin.close()
            sys.stdout.close()
            sys.stderr.close()
            os.close(0)
            os.close(1)
            os.close(2)
            os.execvp("globus-url-copy",cmd.split())
        os.wait()[0]

    def remoteMakeDirs(self, remoteName):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:remoteMakeDirs")
        cmd = "gsissh %s mkdir -p %s" % (self.remoteLoginName, remoteName)
        print cmd
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]
        
    def remoteChmodX(self, remoteName):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:remoteChmodX")
        cmd = "gsissh %s chmod +x %s" % (self.remoteLoginName, remoteName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]

    def copyFileWaiterUtility(self):
        script = EnvString.resolve("$CTRL_ORCA_DIR/bin/filewaiter.py")
        remoteName = os.path.join(self.dirs.get("work"), os.path.basename(script))
        self.copyToRemote(script, remoteName)
        self.remoteChmodX(remoteName)
        return remoteName

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

        pipelinePolicy = pipelinePolicyGroup.getPolicyName()
        shortName = pipelinePolicy.get("shortName")

        pipelinePolicyNumber = pipelinePolicyGroup.getPolicyNumber()
        pipelineName = "%s_%d" % (shortName, pipelinePolicyNumber)

        globalPipelineOffset = pipelinePolicyGroup.getGlobalOffset()

        logDir = os.path.join(self.localWorkDir, pipelineName)
        # create the log directories under the local scratch work
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        # create the list of launch.log file's we'll watch for later.
        logFile = os.path.join(pipelineName, "launch.log")
        logFile = os.path.join(self.dirs.get("work"), logFile)
        self.logFileNames.append(logFile)

        
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
            newPolicyFile = os.path.join(self.localWorkDir, filename)
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(definitionPolicy)
            pw.close()

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
            shutil.copy(self.script, self.localWorkDir)

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
                destintationDir = self.localWorkDir
                for newDestinationDir in tokens[:len(tokens)-1]:
                    newDir = os.path.join(self.localWorkDir, newDestinationDir)
                    if os.path.exists(newDir) == False:
                        os.mkdir(newDir)
                    destinationDir = newDir
                shutil.copyfile(policyFile, os.path.join(destinationDir, destinationFile))

        # create the launch command
        execPath = definitionPolicy.get("framework.exec")
        execCmd = execPath

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

        
        # On condor, you have to launch the script, then wait until that
        # script exits.
        launcher.write("%s %s %s -L %s --logdir %s >%s/launch.log 2>&1 &\n" % (execCmd, filename, self.runid, self.wfVerbosity, remoteLogDir, remoteLogDir))
        launcher.write("wait\n")
        launcher.write('echo "from launcher"\n')
        launcher.write("ps -ef\n")

        launcher.close()

        return

    def rewritePipelinePolicy(self, pipelinePolicy):
        filename = pipelinePolicy.getFile("definition").getPath()
        oldPolicy = pol.Policy.createPolicy(filename, False)

        if self.prodPolicy.exists("eventBrokerHost"):
            oldPolicy.set("execute.eventBrokerHost", self.prodPolicy.get("eventBrokerHost"))

        if self.prodPolicy.exists("shutdownTopic"):
            oldPolicy.set("execute.shutdownTopic", self.prodPolicy.get("shutdownTopic"))

        if self.prodPolicy.exists("logThreshold"):
            oldPolicy.set("execute.logThreshold", self.prodPolicy.get("logThreshold"))

        newPolicyFile = os.path.join(self.localWorkDir, filename)
        pw = pol.PAFWriter(newPolicyFile)
        pw.write(oldPolicy)
        pw.close()

    def collectDirNames(self, dirSet, platformPolicy, pipelinePolicy, num):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:collectDirNames")
        
        dirPolicy = platformPolicy.getPolicy("dir")
        dirName = pipelinePolicy.get("shortName")

        directories = Directories(dirPolicy, dirName, self.runid)
        dirs = directories.getDirs()
        for name in dirs.names():
            remoteName = dirs.get(name)
            dirSet.add(remoteName)
        indexedDirName = "%s_%d" % (dirName, num)
        self.directoryList[indexedDirName] = directories
        return

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self, dirSet):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:createDirs")
        for remoteName in dirSet:
            self.remoteMakeDirs(remoteName)


    def createCondorDir(self, workDir):
        # create Condor_glidein/local directory under "work"
        condorDir = os.path.join(workDir,"Condor_glidein")
        condorLocalDir = os.path.join(condorDir, "local")
        if not os.path.exists(condorLocalDir):
            os.makedirs(condorLocalDir)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowConfigurator:setupDatabase")


    def copyLinkScript(self, wfPolicy):
        self.logger.log(Log.DEBUG, "VanillaPipelineWorkflowConfigurator:copyLinkScript")

        if wfPolicy.exists("configuration") == False:
            return None
        configuration = wfPolicy.get("configuration")
        if configuration.exists("deployData") == False:
            return None
        deployPolicy = configuration.get("deployData")
        dataRepository = deployPolicy.get("dataRepository")
        deployScript = deployPolicy.get("script")
        deployScript = EnvString.resolve(deployScript)
        collection = deployPolicy.get("collection")

        if os.path.isfile(deployScript) == True:
            # copy the script to the remote side
            remoteName = os.path.join(self.dirs.get("work"), os.path.basename(deployScript))
            self.copyToRemote(deployScript, remoteName)
            self.remoteChmodX(remoteName)
            return remoteName
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:deployData: warning: script '%s' doesn't exist" % deployScript)
        return None

    def runLinkScript(self, wfPolicy, remoteName):
        self.logger.log(Log.DEBUG, "VanillaPipelineWorkflowConfigurator:runLinkScript")
        if wfPolicy.exists("configuration") == False:
            return
        configuration = wfPolicy.get("configuration")
        if configuration.exists("deployData") == False:
            return

        deployPolicy = configuration.get("deployData")
        dataRepository = deployPolicy.get("dataRepository")
        collection = deployPolicy.get("collection")

        runDir = self.directories.getDefaultRunDir()
        # run the linking script
        deployCmd = ["gsissh", self.remoteLoginName, remoteName, runDir, dataRepository, collection]
        pid = os.fork()
        if not pid:
            os.execvp(deployCmd[0], deployCmd)
        os.wait()[0]
        return

    def writeGlideinRequest(self, configPolicy):
        self.logger.log(Log.DEBUG, "VanillaPipelineWorkflowConfigurator:writeGlideinRequest")
        glideinRequest = configPolicy.get("glideinRequest")
        templateFileName = glideinRequest.get("templateFileName")
        templateFileName = EnvString.resolve(templateFileName)
        outputFileName = glideinRequest.get("outputFileName")
        keyValuePairs = glideinRequest.get("keyValuePairs")

        realFileName = os.path.join(self.localWorkDir, outputFileName)

        # for glidein request, we add this additional keyword.
        keyValuePairs.set("ORCA_REMOTE_WORKDIR", self.dirs.get("work"))
        if keyValuePairs.exists("START_OWNER") == False:
            keyValuePairs.set("START_OWNER", getpass.getuser())

        writer = TemplateWriter()
        writer.rewrite(templateFileName, realFileName, keyValuePairs)

        return realFileName
