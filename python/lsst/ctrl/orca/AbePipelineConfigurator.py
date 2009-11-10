import os, os.path, shutil, sets, stat
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.ctrl.orca.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PipelineConfigurator import PipelineConfigurator
from lsst.ctrl.orca.BasicPipelineLauncher import BasicPipelineLauncher

##
#
# AbePipelineConfigurator 
#
class AbePipelineConfigurator(PipelineConfigurator):
    def __init__(self, runid, logger, verbosity):
        self.abePrefix = "gsiftp://gridftp-abe.ncsa.teragrid.org"
        self.runid = runid
        self.logger = logger
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:__init__")
        self.verbosity = verbosity

        self.nodes = None
        self.numNodes = None
        self.dirs = None
        self.policySet = sets.Set()


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
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:configure")
        self.policy = policy
        self.configurationDict = configurationDict
        self.provenanceDict = provenanceDict
        self.repository = repository
        self.pipeline = self.policy.get("shortname")
        self.nodes = self.createNodeList()
        self.prepPlatform()
        self.deploySetup()
        self.setupDatabase()
        cmd = self.createLaunchCommand()
        pipelineLauncher = BasicPipelineLauncher(cmd, self.pipeline, self.logger)
        return pipelineLauncher

    ##
    # @brief create the command which will launch the pipeline
    # @return a string containing the shell commands to execute
    #
    def createLaunchCommand(self):
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:createLaunchCommand")

        execPath = self.policy.get("configuration.framework.exec")
       
        filename = self.configurationDict["filename"]
        
        launchcmd =  os.path.join(self.dirs.get("work"), "orca_launch.sh")

        #cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -L %s" % (self.dirs.get("work"), self.script, launchcmd, filename, self.runid, self.verbosity) ]
        #return cmd
        launchArgs = "%s %s -L %s -S %s" % \
             (self.pipeline+".paf", self.runId, self.pipelineVerbosity, self.remoteScript)  

        # Write Condor file 
        # print "launchPipeline: Write Condor job file here"
        condorJobfile =  self.pipeline+".condor"
        # Let's create some data:
        clist = []
        clist.append("universe=globus\n")
        clist.append("executable="+launchcmd+"\n")
        clist.append("globusrsl = (jobtype=single)(hostcount="+str(self.numNodes)+")(maxWallTime=30)\n")
        clist.append("arguments="+launchArgs+"\n")
        clist.append("transfer_executable=false\n")
        clist.append("globusscheduler=grid-abe.ncsa.teragrid.org:2119/jobmanager-pbs\n")
        clist.append("output="+self.pipeline+"Condor.out\n")
        clist.append("error="+self.pipeline+"Condor.err\n")
        clist.append("log="+self.pipeline+"Condor.log\n")
        clist.append("remote_initialdir="+self.dirs.get("work")+"\n")
        clist.append("queue\n")

        # Create a file object: in "write" mode
        condorFILE = open(condorJobfile,"w")
        condorFILE.writelines(clist)
        condorFILE.close()

        # kick off the run
        cmdCondor = "condor_submit %s" % condorJobfile

        self.logger.log(Log.DEBUG, "cmdCondor = "+ cmdCondor)
        for item in clist:
            self.logger.log(Log.DEBUG, "Condor submit file line> "+ item)

        return cmdCondor


    ##
    # @brief creates a list of nodes from platform.deploy.nodes
    # @return the list of nodes
    #
    def createNodeList(self):
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:createNodeList")
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
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:prepPlatform")
        self.createDirs()

    def copyToRemote(self, localName, remoteName):
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:copyToRemote")
        
        localNameURL = "%s%s" % ("file://",localName)
        remoteFullName = os.path.join(self.dirs.get("work"),remoteName)
        remoteNameURL = "%s%s" % (self.abePrefix, remoteFullName)

        cmd = "globus-url-copy -vb -cd %s %s " % (localNameURL, remoteNameURL)
        
        # perform this copy from the local machine to the remote machine
        pid = os.fork()
        if not pid:
            os.execvp("globus-url-copy",cmd.split())
        os.wait()[0]

    ##
    # @brief write the node list to a local directory
    #
    def writeNodeList(self):

        
        # write this only for debug
        nodelistBaseName = os.join.path(self.repository, "nodelist.scr")
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
        pw = pol.PAFWriter(os.join.path(self.repository,"nodelist.paf"))
        pw.write(p)
        pw.close()
        self.numNodes = x

        self.copyToRemote(nodelistBaseName, "nodelist.scr")
        self.copyToRemote(os.join.path(self.repository,"nodelist.paf"),"nodelist.paf")

    ##
    # @brief 
    #
    def deploySetup(self):
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:deploySetup")

        # write the nodelist to "work"
        self.writeNodeList()

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
        setupShortname = setupLineList[lengthLineList-1]

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
        newPolicyFile = os.path.join(self.dirs.get("work"), configurationFileName+".tmp")
        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN, "Working directory already contains %s")
        else:
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(configurationPolicy)
            pw.close()

        self.copyToRemote(newPolicyFile,configurationFileName)

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
                if tokenslength == 1:
                    self.copyToRemote(filename, os.path.join(destinationDir, destinationFile))
                    

        self.writeLaunchScript()

    ##
    # @brief write a shell script to launch a pipeline
    #
    def writeLaunchScript(self):
        # write out the script we use to kick things off
        name = os.path.join(self.dirs.get("work"), "orca_launch.sh")

        user = self.provenanceDict["user"]
        runid = self.provenanceDict["runid"]
        dbrun = self.provenanceDict["dbrun"]
        dbglobal = self.provenanceDict["dbglobal"]
        repos = self.provenanceDict["repos"]

        filename = os.path.join(self.dirs.get("work"), self.configurationDict["filename"])

        s = "ProvenanceRecorder.py --type=%s --user=%s --runid=%s --dbrun=%s --dbglobal=%s --filename=%s --repos=%s\n" % ("lsst.ctrl.orca.provenance.BasicRecorder", user, runid, dbrun, dbglobal, filename, repos)

        tempName = name+".tmp"
        launcher = open(tempName, 'w')
        launcher.write("#!/bin/sh\n")

        launcher.write("echo $PATH >path.txt\n")
        launcher.write("eups list 2>/dev/null | grep Setup >eups-env.txt\n")
        launcher.write("pipeline=`echo ${1} | sed -e 's/\..*$//'`\n")
        launcher.write(s)
        launcher.write("#$CTRL_ORCA_DIR/bin/writeNodeList.py %s nodelist.paf\n" % self.dirs.get("work"))
        launcher.write("nohup $PEX_HARNESS_DIR/bin/launchPipeline.py $* > ${pipeline}-${2}.log 2>&1  &\n")
        launcher.close()
        # make it executable
        os.chmod(name, stat.S_IRWXU)

        self.copyToRemote(tempName, name)
        return

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self):
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:createDirs")

        dirPolicy = self.policy.getPolicy("platform.dir")
        directories = Directories(dirPolicy, self.pipeline, self.runid)
        self.dirs = directories.getDirs()

        for name in self.dirs.names():
            if not os.path.exists(self.dirs.get(name)): os.makedirs(self.dirs.get(name))

    ##
    # @brief set up this pipeline's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "AbePipelineConfigurator:setupDatabase")

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
