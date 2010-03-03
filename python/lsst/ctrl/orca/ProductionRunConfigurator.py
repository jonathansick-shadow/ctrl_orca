import os
from lsst.ctrl.orca.DatabaseConfigurator import DatabaseConfigurator
from lsst.ctrl.orca.provenance.Provenance import Provenance
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.WorkflowManager import WorkflowManager
from lsst.pex.logging import Log
from lsst.pex.policy import Policy, NameNotFound
from lsst.ctrl.provenance.ProvenanceSetup import ProvenanceSetup
import lsst.pex.exceptions as pexEx

class ProductionRunConfigurator:
    ##
    # @brief create a basic production run.
    # Note that all ProductionRunConfigurator subclasses must support this
    # constructor signature.
    def __init__(self, runid, policyFile, repository=None, logger=None, workflowVerbosity=None):

        # the logger used by this instance
        if not logger:
            logger = Log.getDefaultLogger()
        self.parentLogger = logger
        self.logger = Log(logger, "config")

        logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")

        self.runid = runid
        self._prodPolicyFile = policyFile
        self.prodPolicy = Policy.createPolicy(policyFile, False)

        self.repository = repository
        self.workflowVerbosity = workflowVerbosity
        self._provSetup = None

        self.provenanceDict = {}
        self._wfnames = None

        # cache the database configurators for checking the configuraiton.  
        self._databaseConfigurators = []

        # these are policy settings which can be overriden from what they
        # are in the workflow policies.
        self.policyOverrides = Policy() 
        if self.prodPolicy.exists("eventBrokerHost"):
            self.policyOverrides.set("execute.eventBrokerHost",
                              self.prodPolicy.get("eventBrokerHost"))
        if self.prodPolicy.exists("logThreshold"):
            self.policyOverrides.set("execute.logThreshold",
                              self.prodPolicy.get("logThreshold"))
        if self.prodPolicy.exists("shutdownTopic"):
            self.policyOverrides.set("execute.shutdownTopic",
                              self.prodPolicy.get("shutdownTopic"))

    ##
    # @brief create the WorkflowManager for the pipelien with the given shortName
    #
    def createWorkflowManager(self, wfPolicy, prodPolicy):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createWorkflowManager")

        wfManager = WorkflowManager(None, self.runid, wfPolicy, prodPolicy, self.logger)
        return wfManager

    ##
    # @brief return provenanceSetup
    #
    def getProvenanceSetup(self):
        return self._provSetup

    ##
    # @brief configure this production run
    #
    def configure(self, workflowVerbosity):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
        self.repository = self.prodPolicy.get("repositoryDirectory")

        self._provSetup = ProvenanceSetup()

        # cycle through the policies in the production policy file to get
        # a list of files to record.
        #
        # TODO here:  Don't blindly add files as below.  We have to cycle through
        # all of the files first, dereferencing each file at each level, add them
        # adding them to a Set() to avoid recording provenance on a file more than
        # once, and then add all of those files to provSetup.

        #names = self.prodPolicy.fileNames()
        #for name in names:
        #    if self.prodPolicy.getValueType(name) == Policy.FILE:
        #        # TODO: There's a bug in Policy that prevents retrieving all 
        #        # files -  we want all files, not just one.  
        #        filename = self.prodPolicy.getFile(name).getPath()
        #        self._provSetup.addProductionPolicyFile(name)

        print "self._prodPolicyFile = ",self._prodPolicyFile
        self._provSetup.addAllProductionPolicyFiles(self._prodPolicyFile)
            
        #
        # setup the database for each database listed in production policy.
        # cache the configurators in case we want to check the configuration
        # later. 
        #
        databasePolicies = None
        try :
            databasePolicies = self.prodPolicy.getArray("database")
        except pexExLsstCppException, e:
            pass
        for databasePolicy in databasePolicies:
            cfg = self.createDatabaseConfigurator(databasePolicy)
            cfg.setup(self._provSetup)
            self._databaseConfigurators.append(cfg)

        #
        # do specialized production level configuration, if it exists
        #
        if self.prodPolicy.exists("configuration"):
            specialConfigurationPolicy = self.prodPolicy.getPolicy("configuration")
            self.specializedConfigure(self.productionPolicy)
        

        workflowPolicies = self.prodPolicy.getArray("workflow")
        workflowManagers = []
        for wfPolicy in workflowPolicies:
            # copy in appropriate production level info into workflow Node  -- ?

            workflowManager = self.createWorkflowManager(wfPolicy, self.prodPolicy)
            workflowLauncher = workflowManager.configure(self._provSetup, workflowVerbosity)
            workflowManagers.append(workflowManager)

        return workflowManagers

    ##
    # @brief carry out production-wide configuration checks.
    # @param care      the thoroughness of the checks.
    # @param issueExc  an instance of MultiIssueConfigurationError to add 
    #                   problems to.  If not None, this function will not 
    #                   raise an exception when problems are encountered; they
    #                   will merely be added to the instance.  It is assumed
    #                   that the caller will raise that exception is necessary.
    #
    def checkConfiguration(self, care=1, issueExc=None):
        self.logger.log(Log.DEBUG, "checkConfiguration")
        myProblems = issueExc
        if myProblems is None:
            myProblems = MultiIssueConfigurationError("problems encountered while checking configuration")

        for dbconfig in self._databaseConfigurators:
            dbconfig.checkConfiguration(care, issueExc)
        
        if not issueExc and myProblems.hasProblems():
            raise myProblems

    ##
    # @brief lookup and create the configurator for database operations
    #
    def createDatabaseConfigurator(self, databasePolicy):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createDatabaseConfigurator")
        className = databasePolicy.get("configurationClass")
        classFactory = NamedClassFactory()
        configurationClass = classFactory.createClass(className)
        configurator = configurationClass(self.runid, databasePolicy, self.prodPolicy, None, self.logger)
        return configurator

    ##
    # @brief do any production-wide setup not covered by the setup of the
    # databases or the individual workflows.
    #
    # This implementation does nothing.  Subclasses may override this method
    # to provide specialized production-wide setup.  
    #
    def _specializedConfigure(self, specialConfigurationPolicy):
        pass

    ##
    # @brief return the workflow names to be used for this set of workflows
    #
    def getWorkflowNames(self):
        if not self._wfnames:
            self._wfnames = self.determineWorkflowNames()
        return list(self._wfnames)

    ##
    # @brief return a unique set of names for the workflows.
    #
    # The names are gotten from the "shortName" items from each "workflow"
    # policy item.  This method ensures uniqueness of those names by appending
    # suffixes if needed (e.g. "-#" where # is a number).
    #
    # This may be overridden by a subclass either to override the 
    # names in the policy or fine tune the uniquification.
    #
    def determineWorkflowNames(self):
        workflows = self.policy.getArray("workflow")
        names = []
        i = 1
        for wf in workflows:
            if wf.exists("shortName"):
                names.append(wf.get("shortName"))
            else:
                names.append("Workflow")

        return self._uniquifyWorkflowNames(names)

    def _uniquifyWorkflowNames(self, names):
        names = list(names)
        idxs = range(len(names))
        for i in idxs:
            nonunique = filter(lambda idx: name[i] == names[idx], idxs)
            while len(nonunique) > 1:
                for j in nonunique:
                    names[j] += "-%i" % (j+1)
                nonunique = filter(lambda idx: name[i] == names[idx], idxs)
        return names
