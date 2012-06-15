# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import stat, sys, os, os.path, shutil, sets, stat, socket
from sets import Set
import getpass
import lsst.ctrl.orca as orca
import lsst.pex.config as pexConfig

from lsst.ctrl.orca.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.EnvString import EnvString
#from lsst.ctrl.orca.ConfigUtils import ConfigUtils
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.CondorWorkflowLauncher import CondorWorkflowLauncher
from lsst.ctrl.orca.config.PlatformConfig import PlatformConfig
from lsst.ctrl.orca.TemplateWriter import TemplateWriter
from lsst.ctrl.orca.FileWaiter import FileWaiter

##
#
# CondorWorkflowConfigurator 
#
class CondorWorkflowConfigurator(WorkflowConfigurator):
    def __init__(self, runid, repository, prodConfig, wfConfig, wfName, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:__init__")

        self.runid = runid
        self.repository = repository
        self.prodConfig = prodConfig
        self.wfConfig = wfConfig
        self.wfName = wfName

        self.wfVerbosity = None

        self.dirs = None
        self.directories = None
        self.nodes = None
        self.numNodes = None
        self.logFileNames = []
        self.pipelineNames = []

        self.directoryList = {}
        self.initialWorkDir = None
        self.firstRemoteWorkDir = None
        self.defaultRoot = wfConfig.platform.dir.defaultRoot
        
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
        return self._configureSpecialized(provSetup, self.wfConfig)

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param provSetup
    # @param wfConfig
    #
    def _configureSpecialized(self, provSetup, wfConfig):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:configure")

        localConfig = wfConfig.configuration["condor"]
        self.localScratch = localConfig.condorData.localScratch

        platformConfig = wfConfig.platform
        taskConfigs = wfConfig.task


        self.localStagingDir = os.path.join(self.localScratch, self.runid)
        os.makedirs(self.localStagingDir)

        # write the glidein file
        startDir = os.getcwd()
        os.chdir(self.localStagingDir)

        if localConfig.glidein.template.inputFile is not None:
            self.writeGlideinFile(localConfig.glidein)
        else:
            print "not writing glidein file"
        os.chdir(startDir)

        # TODO - fix this loop for multiple condor submits; still working
        # out what this might mean.
        for taskName in taskConfigs:
            task = taskConfigs[taskName]
            self.scriptDir = task.scriptDir
            
            # save initial directory we were called from so we can get back
            # to it
            startDir = os.getcwd()

            # switch to staging directory
            os.chdir(self.localStagingDir)

            # switch to tasks directory in staging directory
            taskOutputDir = os.path.join(self.localStagingDir, task.scriptDir)
            os.makedirs(taskOutputDir)
            os.chdir(taskOutputDir)

            # generate pre job 
            preJobScript = EnvString.resolve(task.preJob.script.outputFile)
            preJobScriptTemplate = EnvString.resolve(task.preJob.script.template)
            self.writeJobTemplate(preJobScript, preJobScriptTemplate)
            preJobTemplate = EnvString.resolve(task.preJob.template)
            self.writeJobTemplate(task.preJob.outputFile, preJobTemplate, preJobScript)
            
        
            # generate post job
            postJobScript = EnvString.resolve(task.postJob.script.outputFile)
            postJobScriptTemplate = EnvString.resolve(task.postJob.script.template)
            self.writeJobTemplate(postJobScript, postJobScriptTemplate)
            postJobTemplate = EnvString.resolve(task.postJob.template)
            self.writeJobTemplate(task.postJob.outputFile, postJobTemplate, postJobScript)

            # generate worker job
            workerJobScript = EnvString.resolve(task.workerJob.script.outputFile)
            workerJobScriptTemplate = EnvString.resolve(task.workerJob.script.template)
            self.writeJobTemplate(workerJobScript, workerJobScriptTemplate)
            workerJobTemplate = EnvString.resolve(task.workerJob.template)
            self.writeJobTemplate(task.workerJob.outputFile, workerJobTemplate, workerJobScript)

            # switch to staging directory
            os.chdir(self.localStagingDir)

            # generate pre script

            if task.preScript.outputFile is not None:
                preScriptTemplate = EnvString.resolve(task.preScript.template)
                self.writePreScript(task.preScript.outputFile, preScriptTemplate)
                os.chmod(task.preScript.outputFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
            
            # generate dag
            dagGenerator = EnvString.resolve(task.dagGenerator.script)
            dagGeneratorInput = EnvString.resolve(task.dagGenerator.input)
            dagCreatorCmd = [dagGenerator, "-s", dagGeneratorInput, "-w", task.scriptDir, "-t", task.workerJob.outputFile]
            if task.preScript.outputFile is not None:
                dagCreatorCmd.append("-p")
                dagCreatorCmd.append(task.preScript.outputFile)
            pid = os.fork()
            if not pid:
                os.execvp(dagCreatorCmd[0], dagCreatorCmd)
            os.wait()[0]

            # create dag logs directories
            fileObj = open(dagGeneratorInput,'r')
            visitSet = set()
            count = 0
            # this info from gd:
            # Searching for a space detects 
            # extended input like :  visit=887136081 raft=2,2 sensor=0,1
            # No space is something simple like a skytile id  
            for aline in fileObj:
                count += 1
                myData = aline.rstrip()
                if " " in myData:
                    myList = myData.split(' ')
                    visit = myList[0].split('=')[1]
                else:
                    visit = myData
                visitSet.add(visit)
            logDirName = os.path.join(self.localStagingDir, "logs")
            for visit in visitSet:
                dirName = os.path.join(logDirName, str(visit))
                os.makedirs(dirName)
            
            # change back to initial directory
            os.chdir(startDir)

        # create the Launcher

        #workflowLauncher = CondorWorkflowLauncher(jobs, self.initialWorkDir,  self.prodConfig, self.wfConfig, self.runid, self.pipelineNames, self.logger)
        workflowLauncher = CondorWorkflowLauncher(self.prodConfig, self.wfConfig, self.runid, self.localStagingDir, task.dagGenerator.dagName+".diamond.dag", self.logger)
        return workflowLauncher

    # TODO - XXX - these probably can be combined
    def writePreScript(self, outputFileName, template):
        pairs = {}
        pairs["RUNID"] = self.runid
        pairs["DEFAULTROOT"] = self.defaultRoot
        writer = TemplateWriter()
        writer.rewrite(template, outputFileName, pairs)

    def writeJobTemplate(self, outputFileName, template, scriptName = None):
        pairs = {}
        if scriptName is not None:
            pairs["SCRIPT"] = self.scriptDir+"/"+scriptName
        pairs["RUNID"] = self.runid
        pairs["DEFAULTROOT"] = self.defaultRoot
        writer = TemplateWriter()
        writer.rewrite(template, outputFileName, pairs)

    def writeGlideinFile(self, glideinConfig):
        template = glideinConfig.template
        inputFile = EnvString.resolve(template.inputFile)

        # copy the keywords so we can add a couple more
        pairs = {}
        for value in template.keywords:
            val = template.keywords[value]
            pairs[value] = val
        pairs["ORCA_REMOTE_WORKDIR"] = self.defaultRoot+"/"+self.runid
        if pairs.has_key("START_OWNER") == False:
            pairs["START_OWNER"] = getpass.getuser()

        writer = TemplateWriter()
        writer.rewrite(inputFile, template.outputFile, pairs)
          

    def getWorkflowName(self):
        return self.wfName


    ##
    # @brief 
    #
    def deploySetup(self, provSetup, wfConfig, platformConfig, pipelineConfigGroup):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:deploySetup")


    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self, localStagingDir, platformDirConfig):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:createDirs")


    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:setupDatabase")

