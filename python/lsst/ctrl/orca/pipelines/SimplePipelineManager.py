from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import traceback, time
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
import lsst.pex.policy as pol
import lsst.ctrl.orca as orca

from lsst.ctrl.orca.pipelines.PipelineManager import PipelineManager

from lsst.pex.harness.Directories import Directories

class SimplePipelineManager(PipelineManager):

    def __init__(self):
        self.logger = Log(Log.getDefaultLog(), "dc3")
        self.logger.log(Log.DEBUG, "SimplePipelineManager:__init__")
        PipelineManager.__init__(self)
        self.logger.log(Log.DEBUG, "SimplePipelineManager:__init__:done")

    def createDirectories(self):
        self.logger.log(Log.DEBUG, "SimplePipelineManager:createDirectories")


        # ensure the existence of the working directory
        # pdir = self.policy.get("shortName", self.pipeline)
        # 
        # self.inputRootDir = self.createDirectory(pdir, "inputRootDir")
        # self.outputRootDir = self.createDirectory(pdir, "outputRootDir")
        # self.updateRootDir = self.createDirectory(pdir, "updateRootDir")
        # self.scratchRootDir = self.createDirectory(pdir, "scratchRootDir")
        # self.workingDirectory = self.createDirectory(pdir, "workRootDir")
        # print "SELF -> workingDirectory: ",self.workingDirectory
        dirPolicy = self.policy.getPolicy("platform.dir")
        directories = Directories(dirPolicy, self.runId)
        self.dirs = directories.getDirs()
        print self.dirs.names()
        for name in self.dirs.names():
            if orca.dryrun == True:
                print "would create ",self.dirs.get(name)
            else:
                if not os.path.exists(self.dirs.get(name)): os.makedirs(self.dirs.get(name))

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
        self.logger.log(Log.DEBUG, "SimplePipelineManager:deploySetup")

        # copy /bin/sh script responsible for environment setting

        print "simplepipelinemanager, policy:"
        print self.policy
        setupPath = self.policy.get("configuration.framework.environment")
        if setupPath == None:
             raise RuntimeError("couldn't find configuration.framework.environment")
        self.script = EnvString.resolve(setupPath)

        ## TODO: We did this same thing in DC2. We shouldn't be
        ## depending the system we launch on to determine which version
        ## of the setup.*sh script to run.   The remote systems aren't
        ## guaranteed to be running the same shell as the interactive
        ## shell from which orca was launched.

        print "orca.envscript = ",orca.envscript
        if orca.envscript == None:
            print "using default setup.sh"
        else:
            self.script = orca.envscript

        shutil.copy(self.script, self.dirs.get("work"))
         
        
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

        newPolicy = pol.Policy.createPolicy(polfile)

        eventBrokerHost = self.policy.get("configuration.execute.eventBrokerHost")
        newPolicy.set("execute.eventBrokerHost", eventBrokerHost)

        executeDir = self.policy.get("platform.dir")
        newPolicy.set("execute.dir", executeDir)

        #baseURL = self.dbConfigurator.getHostURL()

        #fullURL = baseURL+"/"+ dbNames[0]
        newPolicy.set("execute.database.url",self.dbRunURL)


        polbasefile = os.path.basename(polfile)
        newPolicyFile = os.path.join(self.dirs.get("work"), self.pipeline+".paf")
        print "newPolicyFile = "+newPolicyFile
        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN, 
                       "Working directory already contains %s; won't overwrite" % \
                           polbasefile)
        else:
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(newPolicy)
            pw.close()

            self.provenance.recordPolicy(newPolicyFile)
        
        if os.path.exists(os.path.join(self.dirs.get("work"), self.pipeline)):
            self.logger.log(Log.WARN, 
              "Working directory already contains %s directory; won't overwrite" % \
                           self.pipeline)
        else:
            shutil.copytree(os.path.join(self.repository, self.pipeline), os.path.join(self.dirs.get("work"),self.pipeline))

    def launchPipeline(self):

        self.logger.log(Log.DEBUG, "SimplePipelineManager:launchPipeline")

        execPath = self.policy.get("configuration.framework.exec")
        launchcmd = EnvString.resolve(execPath)
        # kick off the run

        cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -V %s" % (self.dirs.get("work"), self.script, launchcmd, self.pipeline+".paf", self.runId, orca.verbosity) ]
        if orca.dryrun == True:
            print "dryrun: would execute"
            print cmd
        else:
            self.logger.log(Log.DEBUG, "launching pipeline")

            # by convention the first node in the list is the "master" node
            print "launching %s on %s" % (self.pipeline, self.masterNode) 
            print "executing: ",cmd
                       
            self.logger.log(Log.INFO, "launching %s on %s" % (self.pipeline, self.masterNode) )
            self.logger.log(Log.DEBUG, "executing: " + " ".join(cmd))

            if subprocess.call(cmd) != 0:
                raise RuntimeError("Failed to launch " + self.pipeline)
