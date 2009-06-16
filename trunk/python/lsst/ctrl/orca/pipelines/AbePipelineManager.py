from __future__ import with_statement
import re, sys, os, os.path, fnmatch, shutil, subprocess, string
import traceback, time
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
import lsst.pex.policy as pol
import lsst.ctrl.orca as orca
import sets

from lsst.ctrl.orca.pipelines.PipelineManager import PipelineManager

from lsst.pex.harness.Directories import Directories

class AbePipelineManager(PipelineManager):

    def __init__(self, pipeVerb=None, logger=None):
        """
        create a AbePipelineManager
        @param pipeVerb  the verbosity level to pass onto the pipelines for 
                            pipeline messages
        @param logger    the caller's Log instance from which this manager can.
                            create a child Log
        """
        PipelineManager.__init__(self, pipeVerb, logger)
        self.logger.log(Log.DEBUG, "AbePipelineManager:__init__:done")

    def runProcess(self, program, *args):
        pid = os.fork()
        if not pid:
            os.execvp(program, (program,) +  args)
        return os.wait()[0]


    def configureDatabase(self):
        self.logger.log(Log.DEBUG, "AbePipelineManager:configureDatabase")

    def createDirectories(self):
        self.logger.log(Log.DEBUG, "AbePipelineManager:createDirectories")


        dirPolicy = self.policy.getPolicy("platform.dir")
        directories = Directories(dirPolicy, self.pipeline, self.runId)
        self.dirs = directories.getDirs()
        
        for name in self.dirs.names():
            print "createDirectories - would create ",self.dirs.get(name)

        # Do nothing here for Abe 
        # for name in self.dirs.names():
        #     if orca.dryrun == True:
        #         print "would create ",self.dirs.get(name)
        #     else:
        #         if not os.path.exists(self.dirs.get(name)): os.makedirs(self.dirs.get(name))

    #
    # look in the policy for the named directory alias, and create it
    # if that alias exists.
    #
    def createDirectory(self, pdir, name):
        dirName = self.policy.get(name)
        if dirName == None:
            return None

        self.logger.log(Log.DEBUG, "self.rootDir = "+ self.rootDir)
        self.logger.log(Log.DEBUG, "self.rundId = "+ self.runId)
        self.logger.log(Log.DEBUG, "self.pdir = "+ pdir)
        wdir = os.path.join(self.rootDir, self.runId, pdir)
        dir = os.path.join(wdir, dirName)
        if not os.path.exists(dir): os.makedirs(dir)
        return dir

    def deploySetup(self):
        self.logger.log(Log.DEBUG, "AbePipelineManager:deploySetup")

        # copy /bin/sh script responsible for environment setting

        setupPath = self.policy.get("configuration.framework.environment")
        if setupPath == None:
             raise RuntimeError("couldn't find configuration.framework.environment")
        self.script = EnvString.resolve(setupPath)

        setupLineList = self.script.split("/") 
        lengthLineList = len(setupLineList)
        setupShortname = setupLineList[lengthLineList-1]
        print "lengthLineList ", lengthLineList, "\n" 
        print "setupLineList ", setupShortname, "\n" 

        ## TODO: We did this same thing in DC2. We shouldn't be
        ## depending the system we launch on to determine which version
        ## of the setup.*sh script to run.   The remote systems aren't
        ## guaranteed to be running the same shell as the interactive
        ## shell from which orca was launched.

        if orca.envscript == None:
            print "using default setup.sh"
        else:
            self.script = orca.envscript

        # Stage the setup script  into place on the remote resource 
        localSetupName = "%s%s" % ("file://", self.script);
        print "localSetupName ", localSetupName, "\n"; 
        abePrefix =  "gsiftp://gridftp-abe.ncsa.teragrid.org";  

        self.remoteScript = self.dirs.get("work") + "/" + setupShortname
        remoteSetupName = "%s%s" % (abePrefix, self.remoteScript);
        print "remoteSetupName ", remoteSetupName, "\n"; 
        # Copy setup script 
        print "setup script ", self.script, self.remoteScript, "\n";

        # build the copy command
        cmdSetup = "globus-url-copy -vb -cd %s %s " % \
           (localSetupName, remoteSetupName)

        print "Before Transfer Setup \n"; 
        pid = os.fork()
        if not pid:
            os.execvp("globus-url-copy", cmdSetup.split())
        os.wait()[0]
        print "After Transfer Setup \n" 

        # The local copy of the nodelist file
        nodelistBase = self.repository + "/nodelist.scr"
        # count the number of nodes for Condor job
        nodeFile = open(nodelistBase) 
        nodeLines = (nodeFile.readlines())
        self.numNodes = len(nodeLines)
        print "numNodes is ", self.numNodes
        nodeFile.close() 

        # write gridftp suitable local and remote names  
        print "nodelistBase", nodelistBase
        localNodelistName = "%s%s" % ("file://", nodelistBase)
        print "localNodelistName ", localNodelistName, "\n"

        remoteNodelist = self.dirs.get("work") + "/nodelist.scr"
        remoteNodelistName = "%s%s" % (abePrefix, remoteNodelist)
        print "remoteNodelistName ", remoteNodelistName, "\n"

        # build the copy command for nodelist transfer
        cmdNodelist = "globus-url-copy -vb -cd %s %s " % \
           (localNodelistName, remoteNodelistName)
         
        print "Before Transfer Nodelist \n"
        pid = os.fork()
        if not pid:
            os.execvp("globus-url-copy", cmdNodelist.split())
        os.wait()[0]
        print "After Transfer Nodelist \n" 

        # 
        #  read in default policy
        #  read in given policy
        #  in given policy:
        #     set: execute.eventBrokerHost
        #     set: execute.dir
        #     set: execute.database.url
        #  write new policy file with overridden values
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()
        # 
        # copy the policies to the working directory
        polfile = os.path.join(self.repository, self.pipeline+".paf")
        print "polfile ", polfile, "\n";

        newPolicy = pol.Policy.createPolicy(polfile, False)

        if self.prodPolicyOverrides is not None:
            for name in self.prodPolicyOverrides.paramNames():
                newPolicy.set(name, self.prodPolicyOverrides.get(name))

        executeDir = self.policy.get("platform.dir")
        newPolicy.set("execute.dir", executeDir)

        print "executeDir ", executeDir, "\n";

        newPolicy.set("execute.database.url",self.dbRunURL)
        print "dbRunURL ", self.dbRunURL, "\n";

        polbasefile = os.path.basename(polfile)
        print "polbasefile ", polbasefile, "\n";

        # do not write new Policy in remote location
        # we have to write it here  
        # newPolicyFile = os.path.join(self.dirs.get("work"), self.pipeline+".paf")
        newPolicyFile = os.path.join(self.repository, self.pipeline+".paf.tmp"); 

        print "self.pipeline ", self.pipeline, "\n";
        print "newPolicyFile ", newPolicyFile, "\n";

        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN, 
                       "Working directory already contains %s; won't overwrite" % \
                           polbasefile)
        else:
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(newPolicy)
            pw.close()


        # Stage the newPolicy File into place on the remote resource 
        localGridName0 = "%s%s" % ("file://", newPolicyFile);
        print "localGridName0 ", localGridName0, "\n"; 

        remoteFile = os.path.join(self.dirs.get("work"), self.pipeline+".paf");
        # print "remoteFile ", remoteFile, "\n"; 

        remoteGridName0 = "%s%s" % (abePrefix, remoteFile);
        print "remoteGridName0 ", remoteGridName0, "\n"; 

        # build the copy command
        cmd0 = "globus-url-copy -vb -cd %s %s " % \
           (localGridName0, remoteGridName0)

        print "Before Transfer 1 \n"; 
        pid = os.fork()
        if not pid:
            os.execvp("globus-url-copy", cmd0.split())
        os.wait()[0]
        print "After Transfer 1 \n"; 

        self.provenance.recordPolicy(newPolicyFile)
        self.policySet.add(newPolicyFile)

        # XXX - reuse "newPolicy"?
        # Turn off provenance
        newPolicyObj = pol.Policy.createPolicy(newPolicyFile, False)
        pipelinePolicySet = sets.Set()
        self.recordChildPolicies(self.repository, newPolicyObj, pipelinePolicySet)
        
        ## if os.path.exists(os.path.join(self.dirs.get("work"), self.pipeline)):
        ##    self.logger.log(Log.WARN, 
        ##      "Working directory already contains %s directory; won't overwrite" % \
        ##                   self.pipeline)
        ## else:
            #shutil.copytree(os.path.join(self.repository, self.pipeline), os.path.join(self.dirs.get("work"),self.pipeline))
            #
            # instead of blindly copying the whole directory, take the set
            # if files from policySet and copy those.
            #
            # This is slightly tricky, because we want to copy from the policy file 
            # repository directory to the "work" directory, but we also want to keep
            # that partial directory hierarchy we're copying to as well.
            #

        # changing indent : always stage policy file 
        for filename  in pipelinePolicySet:
            destinationDir = self.dirs.get("work")
            destName = filename.replace(self.repository+"/","")
            tokens = destName.split('/')
            tokensLength = len(tokens)
            # if the destination directory heirarchy doesn't exist, create all
            # the missing directories
            destinationFile = tokens[len(tokens)-1]
            for newDestinationDir in tokens[:len(tokens)-1]:
                print "newDestinationDir ", newDestinationDir, "\n"; 
                newDir = os.path.join(destinationDir, newDestinationDir)
                # do not make the newDir; it is remote 
                # if os.path.exists(newDir) == False:
                #     os.mkdir(newDir)
                destinationDir = newDir
                print "destinationDir ", destinationDir, "\n"; 
                # Copy sub pipeline policy    
                dest1 = os.path.join(destinationDir, destinationFile);  
                print "dest1 ", dest1;

            print "filename ", filename, "\n"; 

            localGridName = "%s%s" % ("file://", filename);
            print "localGridName ", localGridName, "\n"; 

            remoteGridName =  "%s%s" % (abePrefix, dest1);
            print "remoteGridName ", remoteGridName, "\n"; 

            # build the copy command
            cmd = "globus-url-copy -vb -cd %s %s " % \
                 (localGridName, remoteGridName)

            print "Before sub Transfer \n"; 
            pid = os.fork()
            if not pid:
                os.execvp("globus-url-copy", cmd.split())
            os.wait()[0]
            print "After sub Transfer \n"; 

            # Typical globus-url-copy urls 
            # file:///lsst/home/daues/globus/lsst4/work/set.tar 
            #    gsiftp://gridftp-abe.ncsa.teragrid.org/u/ac/daues/set.tar

    def launchPipeline(self):

        self.logger.log(Log.DEBUG, "AbePipelineManager:launchPipeline")

        # Get command to execute
        execPath = self.policy.get("configuration.framework.exec")
        # In General one cannot get this from the local environemnt; it is remote 
        # launchcmd = EnvString.resolve(execPath)
        # HardCode for now
        launchcmd =  execPath 

        print "execPath ", execPath, "\n";
        print "launchcmd ", launchcmd, "\n";

        launchArgs = "%s %s -L %s -S %s" % (self.pipeline+".paf", self.runId, self.pipelineVerbosity, self.remoteScript)  

        # Write Condor file 
        print "launchPipeline: Write Condor job file here"
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
        print "cmdCondor \n"
        print cmdCondor

        print "Condor submit file \n"
        print clist
        print "\n"

        print "Submitting Condor job ... \n";
        pid = os.fork()
        if not pid:
            os.execvp("condor_submit", cmdCondor.split())
        os.wait()[0]
        print "Condor job submitted. \n";


    def getAllNames(self, dirName, pattern):
        
        nameList = []
    
        try:
            names = os.listdir(dirName)
        except os.error:
            return nameList
    
        patternList = string.splitfields( pattern, ';' )
        
        for name in names:
            fullname = os.path.normpath(os.path.join(dirName, name))
    
            for pat in patternList:
                if fnmatch.fnmatch(name, pat):
                    if os.path.isfile(fullname) or (os.path.isdir(fullname) == False):
                        nameList.append(fullname)
                    continue
                    
            if os.path.isdir(fullname) and not os.path.islink(fullname):
                nameList = nameList + self.getAllNames(fullname, pattern)
                
        return nameList

    def createNodeList(self):
        self.logger.log(Log.DEBUG, "AbePipelineManager:createNodeList")

        print "createNodelist AbePipelineManager:createNodeList  "

        node = self.policy.getArray("platform.deploy.nodes")

        # if node[0] == "remote":
        #     return None;

        nodes = map(self.expandNodeHost, node)
        # by convention, the master node is the first node in the list
        # we use this later to launch things, so strip out the info past ":", if it's there.
        self.masterNode = nodes[0]
        colon = self.masterNode.find(':')
        if colon > 1:
            self.masterNode = self.masterNode[0:colon]
        if orca.dryrun == False:
            nodelist = open("nodelist.scr", 'w')
            for node in nodes:
                print >> nodelist, node
            nodelist.close()

        return nodes

