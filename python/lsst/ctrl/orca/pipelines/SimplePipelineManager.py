from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import traceback, time
from lsst.pex.logging import Log
from lsst.ctrl.orca.DryRun import DryRun

from pipelines.PipelineManager import PipelineManager

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
        pdir = self.policy.get("shortName", self.pipeline)
    

        self.inputRootDir = self.createDirectory(pdir, "inputRootDir")
        self.outputRootDir = self.createDirectory(pdir, "outputRootDir")
        self.updateRootDir = self.createDirectory(pdir, "updateRootDir")
        self.scratchRootDir = self.createDirectory(pdir, "scratchRootDir")
        self.workingDirectory = self.createDirectory(pdir, "workRootDir")

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

        # copy the policies to the working directory

    def launchPipeline(self):

        self.logger.log(Log.DEBUG, "SimplePipelineManager:launchPipeline")

        # kick off the run
        singleton = DryRun()
        launchcmd = os.path.join(os.environ["DC3PIPE_DIR"], "bin", "launchPipeline.sh")

        if singleton.value == True:
            print "dryrun: would execute"
            cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -V %s" % (self.workingDirectory, script, launchcmd, self.pipeline+".paf", runid, Verbosity().value) ]
            print cmd
        else:
            self.logger.log(Log.DEBUG, "launching pipeline")

            launchcmd = os.path.join(os.environ["DC3PIPE_DIR"], "bin", "launchPipeline.sh")
            cmd = ["ssh", node, "cd %s; source %s; %s %s %s -V %s" % (wdir, script, launchcmd, pname+".paf", runid, cl.opts.verbosity) ]
                       
            logger.log(Log.INFO, "launching %s on %s" % (pname, node) )
            logger.log(Log.DEBUG, "executing: " + " ".join(cmd))

            if subprocess.call(cmd) != 0:
                raise RuntimeError("Failed to launch " + pname)

