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

