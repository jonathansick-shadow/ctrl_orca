import os, os.path, shutil, sets, stat
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.ctrl.orca.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.DagmanWorkflowLauncher import DagmanWorkflowLauncher

##
#
# VanillaCondorWorkflowConfigurator 
#
class VanillaCondorWorkflowConfigurator(WorkflowConfigurator):
    def __init__(self, runid, logger, verbosity):
        self.runid = runid
        self.logger = logger
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:__init__")
        self.verbosity = verbosity

        self.directories = None
        self.nodes = None
        self.numNodes = None
        self.dirs = None
        self.policySet = sets.Set()

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param policy the workflow policy to use for configuration
    # @param configurationDict a dictionary containing configuration info
    # @param provenanceDict a dictionary containing info to record provenance
    # @param repository policy file repository location
    #
    def configure(self, policy, configurationDict, provenanceDict, repository):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:configure")
        self.policy = policy

        self.remoteLoginName = self.policy.get("configurator.loginNode")
        self.remoteFTPName = self.policy.get("configurator.ftpNode")
        self.transferProtocolPrefix = self.policy.get("configurator.transferProtocol")+"://"+self.remoteFTPName
        self.localScratch = self.policy.get("configurator.localScratch")

        self.tmpdir = os.path.join(self.localScratch,self.runid)
        if not os.path.exists(self.tmpdir):
            os.makedir(self.tmpdir)

        self.remoteScript = "orca_launch.sh"

        self.configurationDict = configurationDict
        self.provenanceDict = provenanceDict
        self.repository = repository
        self.workflow = self.policy.get("shortName")
        # unused in the condor vanilla universe
        #self.nodes = self.createNodeList()
        self.prepPlatform()
        self.createLaunchScript()
        self.deploySetup()
        self.setupDatabase()

        condorFile = self.writeCondorFile()
        
        workflowLauncher = DagmanWorkflowLauncher("", self.workflow, self.logger)
        return workflowLauncher

    ##
    # @brief create the command which will launch the workflow
    # @return a string containing the shell commands to execute
    #
    def writeCondorFile(self):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:writeCondorFile")

        execPath = self.policy.get("configuration.framework.exec")
        #launchcmd = EnvString.resolve(execPath)
        launchcmd = execPath

        setupScriptBasename = os.path.basename(self.script)
        remoteSetupScriptName = os.path.join(self.dirs.get("work"), setupScriptBasename)
       
        launchArgs = "%s %s -L %s -S %s" % \
             (self.workflow+".paf", self.runid, self.verbosity, remoteSetupScriptName)
        print "launchArgs = %s",launchArgs


        # we need a count of the number of nodes for this condor file, so
        # count 'em up.
        nodelist = self.policy.getArray("platform.deploy.nodes")
        nodecount = 0
        for node in nodelist:
            nodecount = nodecount + 1
        
        # Write Condor file 
        condorJobfile =  os.path.join(self.tmpdir, self.workflow+".condor")

        clist = []
        clist.append("universe=vanilla\n")
        clist.append("executable="+self.remoteScript+"\n")
        clist.append("arguments="+launchcmd+" "+launchArgs+"\n")
        clist.append("transfer_executable=false\n")
        clist.append("output="+self.workflow+"_Condor.out\n")
        clist.append("error="+self.workflow+"_Condor.err\n")
        clist.append("log="+self.workflow+"_Condor.log\n")
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

    
    def getNodeCount(self):
        # unused in this case
        return -1

    ##
    # @brief creates a list of nodes from platform.deploy.nodes
    # @return the list of nodes
    #
    def createNodeList(self):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:createNodeList")
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
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:prepPlatform")
        self.createDirs()

    def stageLocally(self, localName, remoteName):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:stageLocally")

        print "local name  = "+localName
        print "remote name  = "+remoteName
        localStageDir = os.path.join(self.tmpdir, self.workflow)

        print "localStageDir = "+localStageDir

        localStageName = os.path.join(localStageDir, remoteName)

        print "localName = %s, localStageName = %s\n",(localName,localStageName)
        if os.path.exists(os.path.dirname(localStageName)) == False:
            os.makedirs(os.path.dirname(localStageName))
        shutil.copyfile(localName, localStageName)

    def copyToRemote(self, localName, remoteName):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:copyToRemote")
        
        localNameURL = "%s%s" % ("file://",localName)
        remoteNameURL = "%s%s" % (self.transferProtocolPrefix, remoteName)

        cmd = "globus-url-copy -r -vb -cd %s %s " % (localNameURL, remoteNameURL)
        
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

        # srp
        #self.copyToRemote(nodelistBaseName, "nodelist.scr")
        #self.copyToRemote(localPAFName, "nodelist.paf")
        #os.unlink(nodelistBaseName)
        #os.unlink(localPAFName)
        self.stageLocally(nodelistBaseName, "nodelist.scr")
        self.stageLocally(localPAFName, "nodelist.paf")

    ##
    # @brief 
    #
    def deploySetup(self):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:deploySetup")

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
        setupShortName = os.path.join("work",setupShortName)

        # stage the setup script into place on the remote resource
        # srp
        #self.copyToRemote(self.script, setupShortName)
        self.stageLocally(self.script, setupShortName)

        # This policy has the override values, but must be written out and
        # recorded.
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()
        # 
        # copy the policies to the working directory
        
        configurationPolicy = self.configurationDict["policy"]

        # write out the pipeline .paf file to the local work area.
        newPolicyDir = os.path.join(self.tmpdir, self.workflow)
        newPolicyDir = os.path.join(newPolicyDir, "work")
        newPolicyFile = os.path.join(newPolicyDir, self.workflow+".paf")
        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN, "Working directory already contains %s" % newPolicyFile)
        else:
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(configurationPolicy)
            pw.close()

        # TODO: Provenance script needs to write out newPolicyFile
        #self.provenance.recordPolicy(newPolicyFile)
        self.policySet.add(newPolicyFile)

        # TODO: cont'd - also needs to writeout child policies
        newPolicyObj = pol.Policy.createPolicy(newPolicyFile, False)
        workflowPolicySet = sets.Set()
        self.extractChildPolicies(self.repository, newPolicyObj, workflowPolicySet)

        if os.path.exists(os.path.join(self.dirs.get("work"), self.workflow)):
            self.logger.log(Log.WARN,
              "Working directory already contains %s directory; won't overwrite" % self.workflow)
        else:
            for filename  in workflowPolicySet:
                print "working on filename = "+filename
                print "working on repository = "+self.repository

                localStageNames = filename.split(self.repository)
                localStageName = "work"+localStageNames[1]
                print "localStageName = "+localStageName
                self.stageLocally(filename, localStageName)

        #
        # copy the provenance python files
        filelist = [
                 ('bin/ProvenanceRecorder.py','work/python/ProvenanceRecorder.py'),
                 ('python/lsst/ctrl/orca/provenance/BasicRecorder.py', 'work/python/lsst/ctrl/orca/provenance/BasicRecorder.py'),
                 ('python/lsst/ctrl/orca/provenance/Recorder.py', 'work/python/lsst/ctrl/orca/provenance/Recorder.py'),
                 ('python/lsst/ctrl/orca/provenance/Provenance.py', 'work/python/lsst/ctrl/orca/provenance/Provenance.py'),
                 ('python/lsst/ctrl/orca/provenance/__init__.py', 'work/python/lsst/ctrl/orca/provenance/__init__.py'),
                 ('python/lsst/ctrl/orca/NamedClassFactory.py', 'work/python/lsst/ctrl/orca/NamedClassFactory.py')
                ]
        ctrl_orca_dir = os.getenv('CTRL_ORCA_DIR', None)
        if ctrl_orca_dir == None:
            raise RuntimeError("couldn't find CTRL_ORCA_DIR environment variable")

        for files in filelist:
            localFile = os.path.join(ctrl_orca_dir,files[0])
            remoteFile = files[1]
            self.stageLocally(localFile, remoteFile)
        #print "copy from: "+self.tmpdir
        #print "copy to: "+os.path.join(self.directories.getDefaultRootDir(), self.runid)

        #  the postfix "/" is required by globus-url-copy to copy the whole directory
        self.copyToRemote(self.tmpdir+"/", os.path.join(self.directories.getDefaultRootDir(), self.runid)+"/")
        filename = os.path.join(self.dirs.get("work"), self.remoteScript)

        # make the remote script executable;  this is required because the copy doesn't
        # retain the execution bit setting.
        self.remoteChmodX(filename)

    ##
    # @brief write a shell script to launch a workflow
    #
    def createLaunchScript(self):
        # write out the script we use to kick things off
        
        user = self.provenanceDict["user"]
        runid = self.provenanceDict["runid"]
        dbrun = self.provenanceDict["dbrun"]
        dbglobal = self.provenanceDict["dbglobal"]
        repos = self.provenanceDict["repos"]

        filename = os.path.join(self.dirs.get("work"), self.configurationDict["filename"])
        remoterepos = os.path.join(self.dirs.get("work"),self.workflow)

        provenanceCmd = "#ProvenanceRecorder.py --type=%s --user=%s --runid=%s --dbrun=%s --dbglobal=%s --filename=%s --repos=%s\n" % ("lsst.ctrl.orca.provenance.BasicRecorder", user, runid, dbrun, dbglobal, filename, remoterepos)

        # we can't write to the remove directory, so name it locally first.
        # 
        #tempName = name+".tmp"
        localWorkDir = os.path.join(self.tmpdir, self.workflow)
        localWorkDir = os.path.join(localWorkDir, "work")
        if os.path.exists(localWorkDir) == False:
            os.path.makedirs(localWorkDir)
        tempName = os.path.join(localWorkDir, self.remoteScript)
        launcher = open(tempName, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("PYTHONPATH=$PWD/python:$PYTHONPATH\n")
        launcher.write(provenanceCmd)
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
    def createDirs(self):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:createDirs")

        dirPolicy = self.policy.getPolicy("platform.dir")
        self.directories = Directories(dirPolicy, self.workflow, self.runid)
        self.dirs = self.directories.getDirs()

        localStageDir = os.path.join(self.tmpdir,self.workflow)
        for name in self.dirs.names():
            #if not os.path.exists(self.dirs.get(name)): os.makedirs(self.dirs.get(name))
            #self.remoteMkdir(self.dirs.get(name))
            print "making "+os.path.join(localStageDir, name)
            os.makedirs(os.path.join(localStageDir, name))

        # mkdir under "work" for the provenance directory
        #self.remoteMkdir(os.path.join(self.dirs.get("work"),"python"))
        work  = os.path.join(localStageDir, "work")
        os.makedirs(os.path.join(work,"python"))

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "VanillaWorkflowConfigurator:setupDatabase")

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
    def extractChildPolicies(self, repos, policy, workflowPolicySet):
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
                        if (filename in workflowPolicySet) == False:
                            workflowPolicySet.add(filename)
                        newPolicy = pol.Policy.createPolicy(filename, False)
                        self.extractChildPolicies(repos, newPolicy, workflowPolicySet)
            else:
                field = name
                if policy.getValueType(field) == pol.Policy.FILE:
                    filename = policy.getFile(field).getPath()
                    filename = policy.getFile(field).getPath()
                    filename = os.path.join(repos, filename)
                    if (filename in self.policySet) == False:
                        #self.provenance.recordPolicy(filename)
                        self.policySet.add(filename)
                    if (filename in workflowPolicySet) == False:
                        workflowPolicySet.add(filename)
                    newPolicy = pol.Policy.createPolicy(filename, False)
                    self.extractChildPolicies(repos, newPolicy, workflowPolicySet)
