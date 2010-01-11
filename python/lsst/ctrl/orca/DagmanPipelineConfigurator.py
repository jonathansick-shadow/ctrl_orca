import os, os.path, shutil, sets, stat
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.ctrl.orca.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PipelineConfigurator import PipelineConfigurator
from lsst.ctrl.orca.DagmanPipelineLauncher import DagmanPipelineLauncher

##
#
# DagmanPipelineConfigurator 
#
class DagmanPipelineConfigurator(PipelineConfigurator):
    def __init__(self, runid, logger, verbosity):
        # TODO: these should be in the .paf file
        #self.abeLoginName = "login-abe.ncsa.teragrid.org"
        #self.abeFTPName = "gridftp-abe.ncsa.teragrid.org"
        #self.abePrefix = "gsiftp://"+self.abeFTPName
        self.runid = runid
        self.logger = logger
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:__init__")
        self.verbosity = verbosity

        self.nodes = None
        self.numNodes = None
        self.dirs = None
        self.policySet = sets.Set()

        #self.tmpdir = os.path.join("/tmp",self.runid)
        #if not os.path.exists(self.tmpdir):
        #    os.makedir(self.tmpdir)


    ##
    # @brief Setup as much as possible in preparation to execute the pipeline
    #            and return a PipelineLauncher object that will launch the
    #            configured pipeline.
    # @param policy the pipeline policy to use for configuration
    # @param configurationDict a dictionary containing configuration info
    # @param provenanceDict a dictionary containing info to record provenance
    # @param repository policy file repository location
    #
    def configure(self, policy, configurationDict, provenanceDict, repository):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:configure")
        self.policy = policy

        self.abeLoginName = self.policy.get("configurator.loginNode")
        self.abeFTPName = self.policy.get("configurator.ftpNode")
        self.abePrefix = self.policy.get("configurator.transferProtocol")+"://"+self.abeFTPName
        self.localScratch = self.policy.get("configurator.localScratch")

        self.tmpdir = os.path.join(self.localScratch,self.runid)
        if not os.path.exists(self.tmpdir):
            os.makedir(self.tmpdir)

        self.remoteScript = "orca_launch.sh"

        self.configurationDict = configurationDict
        self.provenanceDict = provenanceDict
        self.repository = repository
        self.pipeline = self.policy.get("shortName")
        # unused in the condor vanilla universe
        #self.nodes = self.createNodeList()
        self.prepPlatform()
        self.createLaunchScript()
        self.deploySetup()
        self.setupDatabase()

        condorFile = self.writeCondorFile()
        
        pipelineLauncher = DagmanPipelineLauncher("", self.pipeline, self.logger)
        return pipelineLauncher

    ##
    # @brief create the command which will launch the pipeline
    # @return a string containing the shell commands to execute
    #
    def writeCondorFile(self):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:writeCondorFile")

        execPath = self.policy.get("configuration.framework.exec")
        #launchcmd = EnvString.resolve(execPath)
        launchcmd = execPath

        setupScriptBasename = os.path.basename(self.script)
        remoteSetupScriptName = os.path.join(self.dirs.get("work"), setupScriptBasename)
       
        launchArgs = "%s %s -L %s -S %s" % \
             (self.pipeline+".paf", self.runid, self.verbosity, remoteSetupScriptName)
        print "launchArgs = %s",launchArgs


        # we need a count of the number of nodes for this condor file, so
        # count 'em up.
        nodelist = self.policy.getArray("platform.deploy.nodes")
        nodecount = 0
        for node in nodelist:
            nodecount = nodecount + 1
        
        # Write Condor file 
        condorJobfile =  os.path.join(self.tmpdir, self.pipeline+".condor")

        clist = []
        clist.append("universe=vanilla\n")
        clist.append("executable="+self.remoteScript+"\n")
        clist.append("arguments="+launchcmd+" "+launchArgs+"\n")
        clist.append("transfer_executable=false\n")
        clist.append("output="+self.pipeline+"_Condor.out\n")
        clist.append("error="+self.pipeline+"_Condor.err\n")
        clist.append("log="+self.pipeline+"_Condor.log\n")
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

    def getPipelineName(self):
        return self.pipeline

    
    def getNodeCount(self):
        # unused in this case
        return -1

    ##
    # @brief creates a list of nodes from platform.deploy.nodes
    # @return the list of nodes
    #
    def createNodeList(self):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:createNodeList")
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

    ##
    # @brief prepare the platform by creating directories and writing the node list
    #
    def prepPlatform(self):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:prepPlatform")
        self.createDirs()

    def copyToRemote(self, localName, remoteName):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:copyToRemote")
        
        localNameURL = "%s%s" % ("file://",localName)
        remoteFullName = os.path.join(self.dirs.get("work"),remoteName)
        remoteNameURL = "%s%s" % (self.abePrefix, remoteFullName)

        cmd = "globus-url-copy -vb -cd %s %s " % (localNameURL, remoteNameURL)
        
        # perform this copy from the local machine to the remote machine
        pid = os.fork()
        if not pid:
            os.execvp("globus-url-copy",cmd.split())
        os.wait()[0]

    def remoteChmodX(self, remoteName):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:remoteChmodX")
        cmd = "gsissh %s chmod +x %s" % (self.abeLoginName, remoteName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]


    def remoteMkdir(self, remoteDir):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:remoteMkdir")
        cmd = "gsissh %s mkdir -p %s" % (self.abeLoginName, remoteDir)
        print "running: "+cmd
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]

    ##
    # @brief write the node list to a local directory
    #
    def writeNodeList(self):

        
        # write this only for debug
        nodelistBaseName = os.path.join(self.tmpdir, "nodelist.scr."+self.runid)
        nodelist = open(nodelistBaseName, 'w')
        for node in self.nodes:
            print >> nodelist, node
        nodelist.close()

        # write a local policy file
        p = pol.Policy()
        x = 0
        for node in self.nodes:
            p.set("node%d" % x, node)
            x = x + 1
        localPAFName = os.path.join(self.tmpdir, "nodelist.paf."+self.runid)
        pw = pol.PAFWriter(localPAFName)
        pw.write(p)
        pw.close()
        self.numNodes = x

        self.copyToRemote(nodelistBaseName, "nodelist.scr")
        self.copyToRemote(localPAFName, "nodelist.paf")
        os.unlink(nodelistBaseName)
        os.unlink(localPAFName)

    ##
    # @brief 
    #
    def deploySetup(self):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:deploySetup")

        # write the nodelist to "work"
        # unused in the condor vanilla universe
        #self.writeNodeList()

        # copy /bin/sh script responsible for environment setting

        setupPath = self.policy.get("configuration.framework.environment")
        if setupPath == None:
             raise RuntimeError("couldn't find configuration.framework.environment")
        self.script = EnvString.resolve(setupPath)

        if orca.envscript == None:
            print "using default setup.sh"
        else:
            self.script = orca.envscript

        setupLineList = self.script.split("/")
        lengthLineList = len(setupLineList)
        setupShortName = setupLineList[lengthLineList-1]

        # stage the setup script into place on the remote resource
        self.copyToRemote(self.script, setupShortName)

        # This policy has the override values, but must be written out and
        # recorded.
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()
        # 
        # copy the policies to the working directory
        
        configurationFileName = self.configurationDict["filename"]
        
        configurationPolicy = self.configurationDict["policy"]
        # This is commmented out, because the work directory doesn't exist on this side.
        #newPolicyFile = os.path.join(self.dirs.get("work"), configurationFileName+".tmp")

        # XXX - write this in the current directory;  we should probably really specify
        # a "scratch" area to write this to instead.
        # XXX - 01/04/10 - srp - use pipeline name to avoid conflicts with other original pipeline names
        #newPolicyFile = os.path.join(self.tmpdir, configurationFileName+".tmp."+self.runid)
        newPolicyFile = os.path.join(self.tmpdir, self.pipeline+".paf.tmp")
        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN, "Working directory already contains %s" % newPolicyFile)
        else:
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(configurationPolicy)
            pw.close()
        # XXX - srp - 01/04/10 - copy to pipeline name instead
        #self.copyToRemote(newPolicyFile,configurationFileName)
        self.copyToRemote(newPolicyFile,self.pipeline+".paf")
        # TODO: uncomment this and perform the remove of the temp file.
        # os.unlink(newPolicyFile)

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
            for filename  in pipelinePolicySet:
                destinationDir = self.dirs.get("work")
                destName = filename.replace(self.repository+"/","")
                tokens = destName.split('/')
                tokensLength = len(tokens)
                # if the destination directory heirarchy doesn't exist, create all          
                # the missing directories
                destinationFile = tokens[len(tokens)-1]
                if tokensLength > 1:
                    for newDestinationDir in tokens[:len(tokens)-1]:
                        newDir = os.path.join(destinationDir, newDestinationDir)
                        destinationDir = newDir
                    self.copyToRemote(filename, os.path.join(destinationDir, destinationFile))
                if tokensLength == 1:
                    self.copyToRemote(filename, os.path.join(destinationDir, destinationFile))

        #
        # copy the launcher script to the remote directory, and make it executable
        #
        name = os.path.join(self.dirs.get("work"), self.remoteScript)
        self.copyToRemote(self.launcherScript, name)
        self.remoteChmodX(name)

        #
        # copy the provenance python files
        filelist = [
                 ('bin/ProvenanceRecorder.py','python/ProvenanceRecorder.py'),
                 ('python/lsst/ctrl/orca/provenance/BasicRecorder.py', 'python/lsst/ctrl/orca/provenance/BasicRecorder.py'),
                 ('python/lsst/ctrl/orca/provenance/Recorder.py', 'python/lsst/ctrl/orca/provenance/Recorder.py'),
                 ('python/lsst/ctrl/orca/provenance/Provenance.py', 'python/lsst/ctrl/orca/provenance/Provenance.py'),
                 ('python/lsst/ctrl/orca/provenance/__init__.py', 'python/lsst/ctrl/orca/provenance/__init__.py'),
                 ('python/lsst/ctrl/orca/NamedClassFactory.py', 'python/lsst/ctrl/orca/NamedClassFactory.py')
                ]
        ctrl_orca_dir = os.getenv('CTRL_ORCA_DIR', None)
        if ctrl_orca_dir == None:
            raise RuntimeError("couldn't find CTRL_ORCA_DIR environment variable")

        for files in filelist:
            localFile = os.path.join(ctrl_orca_dir,files[0])
            remoteFile = os.path.join(self.dirs.get("work"),files[0])
            self.copyToRemote(localFile, remoteFile)
            

    ##
    # @brief write a shell script to launch a pipeline
    #
    def createLaunchScript(self):
        # write out the script we use to kick things off
        
        user = self.provenanceDict["user"]
        runid = self.provenanceDict["runid"]
        dbrun = self.provenanceDict["dbrun"]
        dbglobal = self.provenanceDict["dbglobal"]
        repos = self.provenanceDict["repos"]

        filename = os.path.join(self.dirs.get("work"), self.configurationDict["filename"])
        remoterepos = os.path.join(self.dirs.get("work"),self.pipeline)

        provenanceCmd = "#ProvenanceRecorder.py --type=%s --user=%s --runid=%s --dbrun=%s --dbglobal=%s --filename=%s --repos=%s\n" % ("lsst.ctrl.orca.provenance.BasicRecorder", user, runid, dbrun, dbglobal, filename, remoterepos)

        # we can't write to the remove directory, so name it locally first.
        # 
        #tempName = name+".tmp"
        tempName = os.path.join(self.tmpdir, self.remoteScript+".tmp."+self.runid)
        launcher = open(tempName, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("PYTHONPATH=$PWD/python:$PYTHONPATH\n")
        launcher.write(provenanceCmd)
        launcher.write("echo \"Running SimpleLaunch\"\n")
        launcher.write("echo \"would have run:\"\n")
        launcher.write("echo $@\n")
        launcher.close()


        # make it executable
        os.chmod(tempName, stat.S_IRWXU)

        self.launcherScript = tempName
        return

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:createDirs")

        dirPolicy = self.policy.getPolicy("platform.dir")
        directories = Directories(dirPolicy, self.pipeline, self.runid)
        self.dirs = directories.getDirs()

        for name in self.dirs.names():
            #if not os.path.exists(self.dirs.get(name)): os.makedirs(self.dirs.get(name))
            self.remoteMkdir(self.dirs.get(name))

        # mkdir under "work" for the provenance directory
        self.remoteMkdir(os.path.join(self.dirs.get("work"),"python"))

    ##
    # @brief set up this pipeline's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "DagmanPipelineConfigurator:setupDatabase")

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
        
    ##
    # @brief given a policy, recursively add all child policies to a policy set
    # 
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
