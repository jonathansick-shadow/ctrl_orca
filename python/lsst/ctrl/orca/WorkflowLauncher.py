import subprocess
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor

##
# @brief an abstract class for configuring a workflow
#
# This class should not be used directly but rather must be subclassed,
# providing an implementation for _configureSpecialized.
# 
class WorkflowLauncher:
    ##
    # @brief
    #
    # This constructor should only be called from a subclass's
    # constructor, in which case the fromSub parameter must be
    # set to True.
    # 
    # @param logger      the logger used by the caller.  This class
    #                       will set this create a child log with the
    #                       subname "config".  A sub class may wish to
    #                       reset the child logger for a different subname.
    # 
    def __init__(self, logger, workflowPolicy):
        if not logger:
            logger = Log.getDefaultLog()
        self.parentLogger = logger
        self.logger = Log(logger, "launch")
        self.logger.log(Log.DEBUG, "WorkflowLauncher:__init__")

        self.workflowPolicy = workflowPolicy

    ##
    # @brief launch this workflow
    #
    def launch(self):
        self.logger.log(Log.DEBUG, "WorkflowLauncher:launch")

        self.workflowMonitor = WorkflowMonitor(self.logger, workflowPolicy)
        return self.workflowMonitor # returns WorkflowMonitor

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "WorkflowLauncher:cleanUp")

    ##
    # @brief perform checks on validity of configuration of this workflow
    #
    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "WorkflowLauncher:checkConfiguration")
