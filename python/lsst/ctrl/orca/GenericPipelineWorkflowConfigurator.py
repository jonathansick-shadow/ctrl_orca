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

import sys,os, os.path, shutil, sets, stat
import lsst.ctrl.orca as orca
import lsst.pex.policy as pol

from lsst.pex.harness.Directories import Directories
from lsst.pex.logging import Log

from lsst.ctrl.orca.PolicyUtils import PolicyUtils
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.GenericPipelineWorkflowLauncher import GenericPipelineWorkflowLauncher

##
#
# GenericPipelineWorkflowConfigurator 
#
class GenericPipelineWorkflowConfigurator(WorkflowConfigurator):
    def __init__(self, runid, repository, prodPolicy, wfPolicy, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:__init__")
        self.runid = runid
        self.prodPolicy = prodPolicy
        self.wfPolicy = wfPolicy
        self.repository = repository

        self.wfVerbosity = None

        self.nodes = None
        self.directories = None
        self.dirs = None
        self.defaultRunDir = None

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
        return self._configureSpecialized(provSetup, self.wfPolicy)
    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param policy the workflow policy to use for configuration
    # @param configurationDict a dictionary containing configuration info
    # @param provenanceDict a dictionary containing info to record provenance
    # @param repository policy file repository location
    #
    
    def _configureSpecialized(self, provSetup, wfPolicy):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:configure")
        self.shortName = wfPolicy.get("shortName")
        if wfPolicy.getValueType("platform") == pol.Policy.FILE:
            filename = wfPolicy.getFile("platform").getPath()
            fullpath = None
            if os.path.isabs(filename):
                fullpath = filename
            else:
                fullpath = os.path.join(self.repository, filename)
            platformPolicy = pol.Policy.createPolicy(fullpath)
        else:
            platformPolicy = wfPolicy.getPolicy("platform")

        self.defaultDomain = platformPolicy.get("deploy.defaultDomain")
        pipelinePolicies = wfPolicy.getPolicyArray("pipeline")
        expandedPipelinePolicies = self.expandPolicies(self.shortName, pipelinePolicies)
        #launchCmd = {}
        launchCmd = []
        for pipelinePolicyGroup in expandedPipelinePolicies:
            pipelinePolicy = pipelinePolicyGroup.getPolicyName()
            num = pipelinePolicyGroup.getPolicyNumber()

            self.nodes = self.createNodeList(pipelinePolicy)
            self.createDirs(platformPolicy, pipelinePolicy)
            pipelineShortName = pipelinePolicy.get("shortName")
            launchName = "%s_%d" % (pipelineShortName, num)
            self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator: launchName = %s" % launchName)
            val = self.deploySetup(provSetup, wfPolicy, platformPolicy, pipelinePolicyGroup)
            launchCmd.append(val)
            self.logger.log(Log.DEBUG, "launchCmd = %s" % launchCmd)
        self.deployData(wfPolicy)
        workflowLauncher = GenericPipelineWorkflowLauncher(launchCmd, self.prodPolicy, wfPolicy, self.runid, self.logger)
        return workflowLauncher

    ##
    # @brief creates a list of nodes from platform.deploy.nodes
    # @return the list of nodes
    #
    def createNodeList(self,  pipelinePolicy):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:createNodeList")
        node = pipelinePolicy.getArray("deploy.processesOnNode")

        nodes = map(self.expandNodeHost, node)
        # by convention, the master node is the first node in the list
        # we use this later to launch things, so strip out the info past ":", if it's there.
        self.masterNode = nodes[0]
        colon = self.masterNode.find(':')
        if colon > 1:
            self.masterNode = self.masterNode[0:colon]
        return nodes

    def getWorkflowName(self):
        return self.workflow
    
    def getNodeCount(self):
        return len(self.nodes)

    ##
    # @brief write the node list to the "work" directory
    #
    def writeNodeList(self, logDir):
        
        # write this only for debug
        nodelist = open(os.path.join(logDir, "nodelist.scr"), 'w')
        #nodelist = open(os.path.join(self.dirs.get("work"), "nodelist.scr"), 'w')
        for node in self.nodes:
            print >> nodelist, node
        nodelist.close()

        p = pol.Policy()
        x = 0
        for node in self.nodes:
            p.set("node%d" % x, node)
            x = x + 1
        pw = pol.PAFWriter(os.path.join(logDir, "nodelist.paf"))
        pw.write(p)
        pw.close()


    def deployData(self, wfPolicy):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:deployData")

        # add data deploy here

        if wfPolicy.exists("configuration"):
            configuration = wfPolicy.get("configuration")
            if configuration.exists("deployData"):
                deployPolicy = configuration.get("deployData")
                dataRepository = deployPolicy.get("dataRepository")
                dataRepository = EnvString.resolve(dataRepository)
                deployScript = deployPolicy.get("script")
                deployScript = EnvString.resolve(deployScript)
                collection = deployPolicy.get("collection")
                
                if os.path.isfile(deployScript) == True:
                    runDir = self.directories.getDefaultRunDir()
                    deployCmd = [deployScript, runDir, dataRepository, collection]
                    print ">>> ",deployCmd
                    pid = os.fork()
                    if not pid:
                        os.execvp(deployCmd[0], deployCmd)
                    os.wait()[0]
                else:
                    self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:deployData: warning: script '%s' doesn't exist" % deployScript)
        # end data deploy here


    ##
    # @brief 
    #
    def deploySetup(self, provSetup, wfPolicy, platformPolicy, pipelinePolicyGroup):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:deploySetup")

        pipelinePolicy = pipelinePolicyGroup.getPolicyName()
        shortName = pipelinePolicy.get("shortName")

        pipelinePolicyNumber = pipelinePolicyGroup.getPolicyNumber()
        pipelineName = "%s_%d" % (shortName, pipelinePolicyNumber)

        globalPipelineOffset = pipelinePolicyGroup.getGlobalOffset()

        workDir = self.dirs.get("work")

        # create the subdirectory for the pipeline specific files
        logDir = os.path.join(self.dirs.get("work"), pipelineName)
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        
        # only write out the policyfile once
        filename = pipelinePolicy.getFile("definition").getPath()
        fullpath = None
        if os.path.isabs(filename):
            fullpath = filename
        else:
            fullpath = os.path.join(self.repository, filename)
        definitionPolicy = pol.Policy.createPolicy(fullpath, False)
        if pipelinePolicyNumber == 1:
            if platformPolicy.exists("dir"):
                definitionPolicy.set("execute.dir", platformPolicy.get("dir"))
            if self.prodPolicy.exists("eventBrokerHost"):
                definitionPolicy.set("execute.eventBrokerHost", self.prodPolicy.get("eventBrokerHost"))
    
            if self.wfPolicy.exists("shutdownTopic"):
                definitionPolicy.set("execute.shutdownTopic", self.wfPolicy.get("shutdownTopic"))
            if self.prodPolicy.exists("logThreshold"):
                definitionPolicy.set("execute.logThreshold", self.prodPolicy.get("logThreshold"))
            newPolicyFile = os.path.join(workDir, filename)
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(definitionPolicy)
            pw.close()

        # write the nodelist to "work"
        self.writeNodeList(logDir)

        # copy /bin/sh script responsible for environment setting

        setupPath = definitionPolicy.get("framework.environment")
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
        if pipelinePolicyNumber == 1:
            shutil.copy(self.script, workDir)

        # now point at the new location for the setup script
        self.script = os.path.join(workDir, os.path.basename(self.script))

        #
        # Write all policy files out to the work directory, 
        # but only do it once.
        
        if pipelinePolicyNumber == 1:
        
            # first, grab all the file names, and throw them into a Set() to 
            # avoid duplication
            pipelinePolicySet = sets.Set()

            PolicyUtils.getAllFilenames(self.repository, definitionPolicy, pipelinePolicySet)

            # Cycle through the file names, creating subdirectories as required,
            # and copy them to the destination directory
            for policyFile in pipelinePolicySet:
                destName = policyFile.replace(self.repository+"/","")
                tokens = destName.split('/')
                tokensLength = len(tokens)
                destinationFile = tokens[len(tokens)-1]
                
                for newDestinationDir in tokens[:len(tokens)-1]:
                    newDir = os.path.join(workDir, newDestinationDir)
                    if os.path.exists(newDir) == False:
                        os.mkdir(newDir)
                    destinationDir = newDir
                shutil.copyfile(policyFile, os.path.join(destinationDir, destinationFile))

        # create the launch command
        execPath = definitionPolicy.get("framework.exec")
        #execCmd = EnvString.resolve(execPath)
        execCmd = execPath

        #cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -L %s" % (self.dirs.get("work"), self.script, execCmd, filename, self.runid, self.wfVerbosity) ]

        # write out the launch script
        # write out the script we use to kick things off
        launchName = "launch_%s.sh" % pipelineName

        name = os.path.join(logDir, launchName)


        launcher = open(name, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("export SHELL=/bin/sh\n")
        launcher.write("cd %s\n" % self.dirs.get("work"))
        launcher.write("source %s\n" % self.script)
        launcher.write("eups list 2>/dev/null | grep Setup >%s/eups-env.txt\n" % logDir)

        cmds = provSetup.getCmds()
        workflowPolicies = self.prodPolicy.getArray("workflow")


        # append the other information we previously didn't have access to, but need for recording.
        for cmd in cmds:
            wfShortName = wfPolicy.get("shortName")
            cmd.append("--activityname=%s_%s" % (wfShortName, pipelineName))
            cmd.append("--platform=%s" % wfPolicy.get("platform").getPath())
            cmd.append("--localrepos=%s" % self.dirs.get("work"))
            workflowIndex = 1
            for wfPolicy in workflowPolicies:
                if wfPolicy.get("shortName") == wfShortName:
                    #cmd.append("--activoffset=%s" % workflowIndex)
                    cmd.append("--activoffset=%s" % globalPipelineOffset)
                    break
                workflowIndex = workflowIndex + 1
            launchCmd = ' '.join(cmd)

            # extract the pipeline policy and all the files it includes, and add it to the command
            filelist = provSetup.extractSinglePipelineFileNames(pipelinePolicy, self.repository, self.logger)
            fileargs = ' '.join(filelist)
            launcher.write("%s %s\n" % (launchCmd, fileargs))

        
        launcher.write("nohup %s %s %s -L %s --logdir %s >%s/launch.log 2>&1 &\n" % (execCmd, filename, self.runid, self.wfVerbosity, logDir, logDir))
        launcher.close()
        # make it executable
        os.chmod(name, stat.S_IRWXU)

        launchCmd = ["ssh", self.masterNode, name]

        # print "cmd to execute is: ",launchCmd
        return launchCmd

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self, platformPolicy, pipelinePolicy):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:createDirs")

        dirPolicy = platformPolicy.getPolicy("dir")
        dirName = pipelinePolicy.get("shortName")
        self.directories = Directories(dirPolicy, dirName, self.runid)
        self.dirs = self.directories.getDirs()
        self.defaultRootDir = self.directories.getDefaultRootDir()

        for name in self.dirs.names():
            localDirName = self.dirs.get(name)
            if not os.path.exists(localDirName):
                os.makedirs(localDirName)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowConfigurator:setupDatabase")

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
