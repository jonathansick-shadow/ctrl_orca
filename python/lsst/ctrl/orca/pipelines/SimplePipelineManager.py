from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import traceback, time

from pipelines.PipelineManager import PipelineManager

class SimplePipelineManager(PipelineManager):

    def __init__(self):
        print "SimplePipelineMnager:__init__"
        PipelineManager.__init__(self)
        print "SimplePipelineMnager:__init__:done"

    def configure(self, pipeline, policy, runId):
        PipelineManager.configure(self, pipeline, policy, runId)

    def createDirectories(self):
        print "SimplePipelineManager:createDirectories"

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

        print "self.rootDir = ", self.rootDir
        print "self.rundId = ", self.runId
        print "self.pdir = ", pdir
        wdir = os.path.join(self.rootDir, self.runId, pdir)
        dir = os.path.join(wdir, dirName)
        if not os.path.exists(dir): os.makedirs(dir)
        return dir


    def deploySetup(self):
        print "SimplePipelineManager:deploySetup"

        # copy /bin/sh script responsible for environment setting

        # copy the policies to the working directory

    def launchPipeline(self):
        print "SimplePipelineManager:launchPipeline"

        # kick off the run
        print "launching pipeline"
