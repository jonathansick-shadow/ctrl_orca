import subprocess
from lsst.pex.logging import Log

##
# @brief an interface for getting notified about changes in the status
# of a workflow: when it has started and when it has finished.
#
class StatusListener:
    # 
    def __init__(self, logger):
        if not logger:
            logger = Log.getDefaultLog()
        self.logger = Log(logger, "launch")
        self.logger.log(Log.DEBUG, "StatusListener:__init__")

    ##
    #
    # @brief indicate taht a workflow has experience an as-yet unhandled
    # failure that prevents further processing
    #
    def workflowFailed(self, name, errorName, errmsg, event, pipelineName):
        return

    ##
    #
    # @brief the workflow has successfully shutdown and ready to be
    # cleaned up
    #
    def workflowShutdown(self, name):
        return

    ##
    #
    # @brief Called when a workflow has started up correctly and is
    # ready to process data.  Note that if a pipeline is waiting for
    # an event, the listener should be notified via a workflowWaiting()
    # message.
    def workflowStarted(self, name):
        return

    ##
    #
    # @brief Indicate that a workflow is waiting for an event to proceed.
    # This should only be called only for events that are expected from
    # outside the workflow.  Events that are meant to travel between Pipelines
    # within a workflow should not trigger this notification.
    #
    def workflowWaiting(self, name):
        return
