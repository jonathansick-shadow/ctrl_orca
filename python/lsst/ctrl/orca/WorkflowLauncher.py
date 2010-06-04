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
    # @param wfPolcy     workflow policy
    # @param logger      the logger used by the caller.  This class
    #                       will set this create a child log with the
    #                       subname "config".  A sub class may wish to
    #                       reset the child logger for a different subname.
    # 
    def __init__(self, wfPolicy, logger = None):
        if not logger:
            logger = Log.getDefaultLog()
        self.parentLogger = logger
        self.logger = Log(logger, "launch")
        self.logger.log(Log.DEBUG, "WorkflowLauncher:__init__")

        self.wfPolicy = wfPolicy

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "WorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener):
        self.logger.log(Log.DEBUG, "WorkflowLauncher:launch")

        self.workflowMonitor = WorkflowMonitor(self.logger, self.wfPolicy)
        if statusListener != None:
            self.workflowMonitor.addStatusListener(statusListener)
        return self.workflowMonitor # returns WorkflowMonitor
