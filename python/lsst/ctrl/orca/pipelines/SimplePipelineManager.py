from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import traceback, time
from lsst.pex.logging import Log
import lsst.pex.policy as pol
import lsst.ctrl.orca as orca

from lsst.ctrl.orca.pipelines.PipelineManager import PipelineManager

from lsst.ctrl.orca.Directories import Directories

class SimplePipelineManager(PipelineManager):

    def __init__(self):
        self.logger = Log(Log.getDefaultLog(), "dc3pipe")
        self.logger.log(Log.DEBUG, "SimplePipelineMnager:__init__")
        PipelineManager.__init__(self)
        self.logger.log(Log.DEBUG, "SimplePipelineMnager:__init__:done")

    def configure(self, pipeline, policy, runId):
        PipelineManager.configure(self, pipeline, policy, runId)

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
        print "createDirectories: self.dirs['work']",self.dirs["work"]
        for name in self.dirs:
            if orca.dryrun == True:
                print "would create ",self.dirs[name]
            else:
                if not os.path.exists(self.dirs[name]): os.makedirs(self.dirs[name])

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


    def deploySetup(self, dbNames, repository):
        self.logger.log(Log.DEBUG, "SimplePipelineManager:deploySetup")

        # copy /bin/sh script responsible for environment setting
        self.script = "setup.sh"
        self.script = os.path.join(os.environ["DC3PIPE_DIR"], "etc", self.script)
        shutil.copy(self.script, self.dirs["work"])
        
        # 
        # TODO: 
        #  read in default policy
        #  read in given policy
        #  in given policy:
        #     set: execute.eventBrokerHost
        #     set: execute.dir
        #     construct proper database url
        #     set: execute.database.url
        #  write new policy file with overridden values
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()
        # 
        # copy the policies to the working directory
        polfile = os.path.join(repository, self.pipeline+".paf")

        newPolicy = pol.Policy.createPolicy(polfile)

        eventBrokerHost = self.policy.get("configuration.execute.eventBrokerHost")
        newPolicy.set("configuration.execute.eventBrokerHost", eventBrokerHost)

        executeDir = self.policy.get("configuration.execute.dir")
        newPolicy.set("configuration.execute.dir", executeDir)

        baseURL = self.dbConfigurator.getHostURL()

        fullURL = baseURL+"/"+ dbNames[0]
        newPolicy.set("configuration.execute.database.url",fullURL)


        polbasefile = os.path.basename(polfile)
        filePath = os.path.join(self.dirs["work"], self.pipeline+".paf")
        if os.path.exists(filePath):
            self.logger.log(Log.WARN, 
                       "Working directory already contains %s; won't overwrite" % \
                           polbasefile)
        else:
            newPolicyFile = open(filePath, 'w')
            newPolicyFile.write(newPolicy.toString())
            newPolicyFile.close()
            # shutil.copy(polfile, self.dirs["work"])
        
        if os.path.exists(os.path.join(self.dirs["work"], self.pipeline)):
            self.logger.log(Log.WARN, 
              "Working directory already contains %s directory; won't overwrite" % \
                           self.pipeline)
        else:
            shutil.copytree(os.path.join(repository, self.pipeline), os.path.join(self.dirs["work"],self.pipeline))

    def launchPipeline(self):

        self.logger.log(Log.DEBUG, "SimplePipelineManager:launchPipeline")

        # kick off the run
        launchcmd = os.path.join(os.environ["DC3PIPE_DIR"], "bin", "launchPipeline.sh")

        if orca.dryrun == True:
            print "dryrun: would execute"
            cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -V %s" % (self.dirs["work"], self.script, launchcmd, self.pipeline+".paf", self.runId, orca.verbosity) ]
            print cmd
        else:
            self.logger.log(Log.DEBUG, "launching pipeline")

            launchcmd = os.path.join(os.environ["DC3PIPE_DIR"], "bin", "launchPipeline.sh")

            # by convention the first node in the list is the "master" node
            cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -V %s" % (self.dirs["work"], self.script, launchcmd, self.pipeline+".paf", self.runId, orca.verbosity) ]
            print "launching %s on %s" % (self.pipeline, self.masterNode) 
            print "executing: ",cmd
                       
            self.logger.log(Log.INFO, "launching %s on %s" % (self.pipeline, self.masterNode) )
            self.logger.log(Log.DEBUG, "executing: " + " ".join(cmd))

            if subprocess.call(cmd) != 0:
                raise RuntimeError("Failed to launch " + self.pipeline)
