import os, os.path, sets
import lsst.pex.policy as pol
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString

class ProductionRunManager:
    def __init__(self, runid, policyFileName, logger, pipelineVerbosity=None):
        self.logger = logger
        self.pipelineVerbosity = pipelineVerbosity
        self.logger.log(Log.DEBUG, "ProductionRunManager:__init__")
        self.runid = runid
        self.pipelineManagers = []

        self.fullPolicyFilePath = ""
        if os.path.isabs(policyFileName) == True:
            self.fullPolicyFilePath = policyFileName
        else:
            self.fullPolicyFilePath = os.path.join(os.path.realpath('.'), policyFileName)

        # create policy file - but don't dereference yet
        self.policy = pol.Policy.createPolicy(self.fullPolicyFilePath, False)

        # determine the repository

        reposValue = self.policy.get("repositoryDirectory")
        if reposValue == None:
            self.repository = "."
        else:
            self.repository = EnvString.resolve(reposValue)
            
        # do a little sanity checking on the repository before we continue.
        if not os.path.exists(self.repository):
            raise RuntimeError("specified repository " + self.repository + ": directory not found");        
        if not os.path.isdir(self.repository):
            raise RuntimeError("specified repository "+ self.repository + ": not a directory");



    def getRunId(self):
        return self.runid

    def getPolicy(self):
        return self.policy

    ##
    # @brief run the entire production
    #
    def runProduction(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:runProduction")

        # create configurator
        self.productionRunConfigurator = self.createConfigurator()

        # configure each pipeline
        for pipelineMgr in self.pipelineManagers:
            pipelineLauncher = pipelineMgr.configure()
            self.pipelineLaunchers.append(pipelineLauncher)

        # Check the configururation
        self.checkConfiguration()

        for pipelineMgr in self.pipelineManagers:
            pipelineManager.runPipeline()

    ##
    # @brief determine whether production is currently running
    #
    def isRunning(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isRunning")
        return False

    ##
    # @brief determine whether production has completed
    #
    def isDone(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isDone")
        return False

    ##
    # @brief determine whether production can be run
    #
    def isRunnable(self):
        self.logger.log(Log.DEBUG, "ProductionRunManager:isRunnable")
        return False

    def createConfigurator(self):
        # prodPolicy - the production run policy
        self.logger.log(Log.DEBUG, "ProductionRunManager:createConfigurator")

        # these are policy settings which can be overriden from what they
        # are in the pipeline policies. Save them for when we create the
        # PipelineManager, and let the PipelineManger use these overrides
        # as it sees fit to do so.
        policyOverrides = pol.Policy()
        if self.policy.exists("eventBrokerHost"):
            policyOverrides.set("execute.eventBrokerHost",
                              self.policy.get("eventBrokerHost"))
        if self.policy.exists("logThreshold"):
            policyOverrides.set("execute.logThreshold",
                              self.policy.get("logThreshold"))
        if self.policy.exists("shutdownTopic"):            
            policyOverrides.set("execute.shutdownTopic", self.policy.get("shutdownTopic"))

        productionRunConfiguratorName = self.policy.get("productionRunConfiguratorClass")
        print self.policy.toString()
        print "---"
        print productionRunConfiguratorName

        classFactory = NamedClassFactory()
        productionRunConfiguratorClass = classFactory.createClass(productionRunConfiguratorName)
        productionRunConfigurator = productionRunConfiguratorClass(self.runid, self.policy, self.logger, self.pipelineVerbosity)


        # get pipelines
        pipelinePolicies = self.policy.get("pipelines")
        pipelinePolicyNames = pipelinePolicies.policyNames(True)
        

        # create a pipelineManager for each pipeline, and save it.
        for policyName in pipelinePolicyNames:
            self.logger.log(Log.DEBUG, "policyName --> "+policyName)
            pipelinePolicy = pipelinePolicies.get(policyName)
            if pipelinePolicy.get("launch",1) != 0:
                shortName = pipelinePolicy.get("shortname", policyName)
                newPolicy = self.rewritePolicy(shortName, pipelinePolicy, policyOverrides)
                pipelineManager = productionRunConfigurator.createPipelineManager(shortName, newPolicy, self.pipelineVerbosity)
                self.pipelineManagers.append(pipelineManager)

        dbNames = productionRunConfigurator.configure()
        productionRunConfigurator.recordPolicy(fullyPolicyFilePath)

        return productionRunConfigurator

    def rewritePolicy(self, shortName, pipelinePolicy, policyOverrides):
        #  read in default policy        
        #  read in given policy
        #  in given policy:
        #     set: execute.eventBrokerHost
        #     set: execute.dir
        #     set: execute.database.url
        #  write new policy file with overridden values        
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()        # 
        # copy the policies to the working directory
        polfile = os.path.join(self.repository, shortName+".paf")

        newPolicy = pol.Policy.createPolicy(polfile, False)

        if policyOverrides is not None:
            for name in policyOverrides.paramNames():
                newPolicy.set(name, policyOverrides.get(name))

        executeDir = self.pipelinePolicy.get("platform.dir")
        newPolicy.set("execute.dir", executeDir)

        newPolicy.set("execute.database.url",self.dbRunURL)


        polbasefile = os.path.basename(polfile)
        newPolicyFile = os.path.join(self.dirs.get("work"), shortName+".paf")   
 
        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN, "Working directory already contains %s; won't overwrite" % polbasefile)        
        else:            
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(newPolicy)
            pw.close()
        return pol.Policy.loadPolicy(newPolicyFile)
        
        # provenance really should be recorded here, since orca is supposed
        # to be in control of everything.
        #self.provenance.recordPolicy(newPolicyFile)

        
    def checkConfiguration(self, care):
        # care - level of "care" in checking the configuration to take. In
        # general, the higher the number, the more checks that are made.
        self.logger.log(Log.DEBUG, "ProductionRunManager:checkConfiguration")
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.checkConfiguration()

    def stopProduction(self, urgency):
        # urgency - an indicator of how urgently to carry out the shutdown.  
        #
        # Recognized values are: 
        #   FINISH_PENDING_DATA - end after all currently available data has 
        #                         been processed 
        #   END_ITERATION       - end after the current data ooping iteration 
        #   CHECKPOINT          - end at next checkpoint opportunity 
        #                         (typically between stages) 
        #   NOW                 - end as soon as possible, forgoing any 
        #                         checkpointing
        self.logger.log(Log.DEBUG, "ProductionRunManager:stopProduction")
        for pipelineMgr in self.pipelineManagers:
            pipelineMgr.stopProduction(urgency)
            
