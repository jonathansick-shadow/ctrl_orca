from lsst.pex.logging import Log
import lsst.pex.exceptions as pexEx
import lsst.pex.policy as pexPol
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory

##
# @brief an abstract class for configuring a workflow
#
# This class should not be used directly but rather must be subclassed,
# providing an implementation for at least _configureWorkflowLauncher.
# Usually, _configureSpecialized() should also be overridden to carry
# out the non-database setup, including deploying the workflow onto the
# remote platform.
# 
class WorkflowConfigurator:


    class PolicyGroup:
        def __init__(self, name, number, offset):
            self.policyName = name
            self.policyNumber = number
            self.globalOffset = offset

        def getPolicyName(self):
            return self.policyName

        def getPolicyNumber(self):
            return self.policyNumber

        def getGlobalOffset(self):
            return self.globalOffset
            
    ##
    # @brief create the configurator
    #
    # This constructor should only be called from a subclass's
    # constructor, in which case the fromSub parameter must be
    # set to True.
    # 
    # @param runid       the run identifier for the production run
    # @param wfPolicy    the workflow policy that describes the workflow
    # @param logger      the logger used by the caller.  This class
    #                       will set this create a child log with the
    #                       subname "config".  A sub class may wish to
    #                       reset the child logger for a different subname.
    # @param fromSub     set this to True to indicate that it is being called
    #                       from a subclass constructor.  If False (default),
    #                       an exception will be raised under the assumption
    #                       that one is trying instantiate it directly.
    def __init__(self, runid, prodPolicy, wfPolicy, logger, fromSub=False):
        self.runid = runid

        # the logger used by this instance
        if not logger:
            logger = Log.getDefaultLogger()
        self.parentLogger = logger
        self.logger = Log(logger, "config")

        sel.logger.log(Log.DEBUG, "WorkflowConfigurator:__init__")

        self.prodPolicy = prodPolicy
        self.wfPolicy = wfPolicy
        self.repository = repository

        if fromSub:
            raise RuntimeError("Attempt to instantiate abstract class, " +
                               "WorkflowConfigurator; see class docs")

    ###
    # @brief Configure the databases, and call an specialization required
    #
    def configure(self, provSetup, workflowVerbosity=None):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:configure")
        self._configureDatabases(provSetup)
        return self._configureSpecialized(self.wfPolicy, workflowVerbosity)

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param policy the workflow policy to use for configuration
    # @param provSetup
    #
    def _configureDatabases(self, provSetup):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:_configureDatabases")

        #
        # setup the database for each database listed in workflow policy
        #
        if self.wfPolicy.exists("database"):
            databasePolicies = self.wfPolicy.getPolicyArray("database")

            for databasePolicy in databasePolicies:
                databaseConfigurator = self.createDatabaseConfigurator(databasePolicy)
                databaseConfigurator.setup(provSetup)
        return

    ##
    # @brief complete non-database setup, including deploying the workflow and its
    # piplines onto the remote platform.
    #
    # This normally should be overriden.
    #
    # @return workflowLauncher
    def _configureSpecialized(self, wfPolicy):
        workflowLauncher = self._createWorkflowLauncher()
        return workflowLauncher


    ##
    # @brief create the workflow launcher
    #
    # This "abstract" method must be overridden; otherwise an exception is raised
    # 
    # @return workflowLauncher
    def _createWorkflowLauncher(self):
        msg = 'called "abstract" WorkflowConfigurator._createWorkflowLauncher'
        self.logger.log(Log.FAIL, msg)
        raise RuntimeError(msg)

    ##
    # @brief lookup and create the configurator for database operations
    #
    def createDatabaseConfigurator(self, databasePolicy):
        self.logger.log(Log.DEBUG, "WorkflowConfigurator:createDatabaseConfigurator")
        className = databasePolicy.get("configurationClass")
        classFactory = NamedClassFactory()
        configurationClass = classFactory.createClass(className)
        configurator = configurationClass(self.runid, databasePolicy, self.logger) 
        return configurator

    ##
    # @brief given a list of pipelinePolicies, number the section we're 
    # interested in based on the order they are in, in the productionPolicy
    # We use this number Provenance to uniquely identify this set of pipelines
    #
    def expandPolicies(self, wfShortName, pipelinePolicies):
        # Pipeline provenance requires that "activoffset" be unique and 
        # sequential for each pipeline in the production.  Each workflow
        # in the production can have multiple pipelines, and even a call for
        # duplicates of the same pipeline within it.
        #
        # Since these aren't numbered within the production policy file itself,
        # we need to do this ourselves. This is slightly tricky, since each
        # workflow is handled individually by orca and had has no reference 
        # to the other workflows or the number of pipelines within 
        # those workflows.
        #
        # Therefore, what we have to do is go through and count all the 
        # pipelines in the other workflows so we can enumerate the pipelines
        # in this particular workflow correctly. This needs to be reworked.

        wfPolicies = self.prodPolicy.getArray("workflow")
        totalCount = 1
        for wfPolicy in wfPolicies:
            if wfPolicy.get("shortName") == wfShortName:
                # we're in the policy which needs to be numbered
               expanded = []
               for policy in pipelinePolicies:
                   # default to 1, if runCount doesn't exist
                   runCount = 1
                   if policy.exists("runCount"):
                       runCount = policy.get("runCount")
                   for i in range(0,runCount):
                       #expanded.append((policy,i+1, totalCount))
                       expanded.append(self.PolicyGroup(policy,i+1, totalCount))
                       totalCount = totalCount + 1
       
               return expanded
            else:
                policies = wfPolicy.getPolicyArray("pipeline")
                for policy in policies:
                    if policy.exists("runCount"):
                        totalCount = totalCount + policy.get("runCount")
                    else:
                        totalCount = totalCount + 1
        return None # should never reach here - this is an error
