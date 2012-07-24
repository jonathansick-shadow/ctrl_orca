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

        localConfig = wfConfig.configuration["local"]
        self.remoteLoginName = localConfig.condorData.loginNode
        self.localScratch = localConfig.condorData.localScratch

        platformConfig = wfConfig.platform
        taskConfigs = wfConfig.task


        # TODO - fix this loop for multiple condor submits; still working
        # out what this might mean.
        for taskName in taskConfigs:
            task = taskConfigs[taskName]
            print "self.localScratch = ",self.localScratch
            print "self.runid = ",self.runid
            self.localStagingDir = os.path.join(self.localScratch, self.runid)
            os.makedirs(self.localStagingDir)
            
            # save initial directory we were called from so we can get back
            # to it
            startDir = os.getcwd()


            # switch to tasks directory in staging directory
            taskOutputDir = os.path.join(self.localStagingDir, task.scriptDir)
            os.makedirs(taskOutputDir)
            os.chdir(taskOutputDir)

            # generate pre job 
            preJob = EnvString.resolve(task.preJob.script)
            self.writeJobTemplate(task.preJob.outputFile, task.preJob.template, preJob)
        
            # generate post job
            postJob = EnvString.resolve(task.postJob.script)
            self.writeJobTemplate(task.postJob.outputFile, task.postJob.template, postJob)

            # generate worker job
            workerJob = EnvString.resolve(task.workerJob.script)
            self.writeJobTemplate(task.workerJob.outputFile, task.workerJob.template, workerJob)

            # switch to staging directory
            os.chdir(self.localStagingDir)

            # generate pre script
            self.writePreScriptTemplate(task.preScript.outputFile, task.preScript.template)
            os.chmod(task.preScript.outputFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
            
            # generate dag
            dagGenerator = EnvString.resolve(task.dagGenerator.script)
            dagGeneratorInput = EnvString.resolve(task.dagGenerator.input)
            dagCreatorCmd = [dagGenerator, "-s", dagGeneratorInput, "-w", task.scriptDir, "-t", task.workerJob.outputFile, "-p", task.preScript.outputFile]
            print "dagCreatorCmd = ",dagCreatorCmd
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

    def writePreScriptTemplate(self, outputFileName, template):
        pairs = {}
        pairs["runid"] = self.runid
        pairs["defaultRoot"] = self.defaultRoot
        writer = TemplateWriter()
        writer.rewrite(template, outputFileName, pairs)

    def writeJobTemplate(self, outputFileName, template, scriptName):
        pairs = {}
        pairs["SCRIPT"] = scriptName
        writer = TemplateWriter()
        writer.rewrite(template, outputFileName, pairs)

    ##
    # @brief create the command which will launch the workflow
    # @return a string containing the shell commands to execute
    #
    def writeCondorFile(self, launchNamePrefix, launchScriptName):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:writeCondorFile")

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
        clist.append("remote_initialdir="+self.dirs.get("workDir")+"\n")
        clist.append("Requirements = (FileSystemDomain != \"dummy\") && (Arch != \"dummy\") && (OpSys != \"dummy\") && (Disk != -1) && (Memory != -1)\n")
        clist.append("queue\n")

        # Create a file object: in "write" mode
        condorFILE = open(condorJobFile,"w")
        condorFILE.writelines(clist)
        condorFILE.close()

        return condorJobFile

    def getWorkflowName(self):
        return self.wfName

    

    ##
    # @brief 
    #
    def deploySetup(self, provSetup, wfConfig, platformConfig, pipelineConfigGroup):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:deploySetup")

        pipelineConfig = pipelineConfigGroup.getConfigName()
        shortName = pipelineConfig

        #shortName = pipelineConfig.shortName

        pipelineConfigNumber = pipelineConfigGroup.getConfigNumber()
        pipelineName = "%s_%d" % (shortName, pipelineConfigNumber)

        globalPipelineOffset = pipelineConfigGroup.getGlobalOffset()

        logDir = os.path.join(self.localWorkDir, pipelineName)
        # create the log directories under the local scratch work
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        self.pipelineNames.append(pipelineName)

        # create the list of launch.log file's we'll watch for later.
        logFile = os.path.join(pipelineName, "launch.log")
        logFile = os.path.join(self.dirs.get("workDir"), logFile)
        self.logFileNames.append(logFile)

        eventBrokerHost =self.prodConfig.production.eventBrokerHost
        if eventBrokerHost == None:
           print "warning: eventBrokerHost is not set"
        
        # copy /bin/sh script responsible for environment setting

        print "shortName = ",shortName
        pipelineDefinitionConfig = wfConfig.pipeline[shortName].definition
        setupPath = pipelineDefinitionConfig.framework.environment
        print "setupPath = ",setupPath
        if setupPath:
            setupPath = EnvString.resolve(setupPath)        
        self.script = setupPath

        if orca.envscript is None:
            self.logger.log(self.logger.INFO-1, "Using configured setup.sh")
        else:
            self.script = orca.envscript
        if not self.script:
             raise RuntimeError("couldn't find framework.environment")

        # only copy the setup script once
        if pipelineConfigNumber == 1:
            shutil.copy(self.script, self.localWorkDir)

        # now point at the new location for the setup script
        self.script = os.path.join(self.dirs.get("workDir"), os.path.basename(self.script))

        # create the launch command
        
        execPath = pipelineDefinitionConfig.framework.script
        execCmd = execPath

        # write out the script we use to kick things off
        launchName = "launch_%s.sh" % pipelineName

        name = os.path.join(logDir, launchName)


        remoteLogDir = os.path.join(self.dirs.get("workDir"), pipelineName)

        launcher = open(name, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("export SHELL=/bin/sh\n")
        launcher.write("cd %s\n" % self.dirs.get("workDir"))
        launcher.write("/bin/rm -f %s/launch.log\n" % remoteLogDir)

        launcher.write("source %s\n" % self.script)

        launcher.write("eups list --setup 2>/dev/null >%s/eups-env.txt\n" % remoteLogDir)


# TODO - rework this when provenance is back in.
#        cmds = provSetup.getCmds()
#        workflowConfigs = self.prodConfig.workflow

#        # append the other information we previously didn't have access to, but need for recording.
#        for cmd in cmds:
#            wfShortName = wfConfig.shortName
#            cmd.append("--activityname=%s_%s" % (wfShortName, pipelineName))
#            cmd.append("--platform=%s" % self.getPath(wfConfig.platform))
#            cmd.append("--localrepos=%s" % self.dirs.get("workDir"))
#            workflowIndex = 1
#            workflowNames = self.prodConfig.workflowNames
#            for workflowName in workflowNames:
#                wfConfig = workflowConfigs[workflowName]
#                if wfConfig.shortName == wfShortName:
#                    #cmd.append("--activoffset=%s" % workflowIndex)
#                    cmd.append("--activoffset=%s" % globalPipelineOffset)
#                    break
#                workflowIndex = workflowIndex + 1
#            launchCmd = ' '.join(cmd)
#
#            # extract the pipeline config and all the files it includes, and add it to the command
#            filelist = provSetup.extractSinglePipelineFileNames(pipelineConfig, repository, self.logger)
#            fileargs = ' '.join(filelist)
#            launcher.write("%s %s\n" % (launchCmd, fileargs))
        # TODO - re-add provenance command
        launcher.write("# provenance command was here...removed for now.");

        # TODO - "filename" needs to be a save() of pipelineDefinitionConfig
        filename = pipelineName+"_replace_this_with_a_config_file.py"
        # On condor, you have to launch the script, then wait until that
        # script exits.
        # TODO: remove the line below, and delete it when ticket 1398 is
        # verified to be working.
        #launcher.write("%s %s %s -L %s --logdir %s >%s/launch.log 2>&1 &\n" % (execCmd, filename, self.runid, self.wfVerbosity, remoteLogDir, remoteLogDir))
        launcher.write("%s %s %s -L %s --logdir %s --workerid %s >%s/launch.log 2>&1 &\n" % (execCmd, filename, self.runid, self.wfVerbosity, remoteLogDir, pipelineName, remoteLogDir))
        launcher.write("wait\n")
        launcher.write("./workerdone.py %s %s %s\n" % (eventBrokerHost, self.runid, pipelineName))
        #launcher.write('echo "from launcher"\n')
        #launcher.write("ps -ef\n")

        launcher.close()

        return

    def collectDirNames(self, dirSet, platformConfig, pipelineConfig, num):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:collectDirNames")
        
        dirConfig = platformConfig.dir
        dirName = pipelineConfig
        print "collectDirNames dirConfig = ", dirConfig
        print "collectDirNames dirName = ", dirName

        directories = Directories(dirConfig, dirName, self.runid)
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
    def createDirs(self, localStagingDir, platformDirConfig):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:createDirs")
        os.makedirs(os.path.join(localStagingDir, platformDirConfig.workDir))
        os.makedirs(os.path.join(localStagingDir, platformDirConfig.inputDir))
        os.makedirs(os.path.join(localStagingDir, platformDirConfig.outputDir))
        os.makedirs(os.path.join(localStagingDir, platformDirConfig.updateDir))
        os.makedirs(os.path.join(localStagingDir, platformDirConfig.scratchDir))


    def createCondorDir(self, workDir):
        # create Condor_glidein/local directory under "workDir"
        condorDir = os.path.join(workDir,"Condor_glidein")
        condorLocalDir = os.path.join(condorDir, "local")
        if not os.path.exists(condorLocalDir):
            os.makedirs(condorLocalDir)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:setupDatabase")


    def copyLinkScript(self, wfConfig):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:copyLinkScript")

        if wfConfig.configuration["local"] == None:
            return None
        configuration = wfConfig.configuration["local"]
        if configuration.deployData == None:
            return None
        deployConfig = configuration.deployData
        dataRepository = deployConfig.dataRepository
        deployScript = deployConfig.script
        print "deployScript = ",deployScript
        deployScript = EnvString.resolve(deployScript)
        collection = deployConfig.collection

        if os.path.isfile(deployScript) == True:
            # copy the script to the remote side
            remoteName = os.path.join(self.dirs.get("workDir"), os.path.basename(deployScript))
            self.copyToRemote(deployScript, remoteName)
            self.localChmodX(remoteName)
            return remoteName
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:deployData: warning: script '%s' doesn't exist" % deployScript)
        return None

    def runLinkScript(self, wfConfig, remoteName):
        self.logger.log(Log.DEBUG, "CondorWorkflowConfigurator:runLinkScript")
        configuration = wfConfig.configuration["local"]
        if configuration == None:
            return

        if configuration.deployData == None:
            return

        deployConfig = configuration.deployData
        dataRepository = deployConfig.dataRepository
        collection = deployConfig.collection

        runDir = self.directories.getDefaultRunDir()
        # run the linking script
        deployCmd = ["ssh", self.remoteLoginName, remoteName, runDir, dataRepository, collection]
        print "deployCmd = ",deployCmd
        pid = os.fork()
        if not pid:
            os.execvp(deployCmd[0], deployCmd)
        os.wait()[0]
        return
