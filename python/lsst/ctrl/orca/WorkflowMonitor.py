from lsst.pex.logging import Log

class WorkflowMonitor:
    ##
    # @brief
    #
    def __init__(self, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "WorkflowMonitor:__init__")
        self.isActive = False

    ##
    # @brief return True if the workflow being monitored appears to still be
    #        running
    #
    def isRunning(self):
        self.logger.log(Log.DEBUG, "WorkflowMonitor:isRunnable")
        return self.isActive

    ##
    # @brief stop the workflow
    #
    def stopWorkflow(self, urgency):
        self.logger.log(Log.DEBUG, "WorkflowMonitor:stopWorkflow")

    ##
    # @brief handle an event
    #
    def handleEvent(self, event):
        # even - an event delivered to this workflow
        self.logger.log(Log.DEBUG, "WorkflowMonitor:handleEvent")

    ##
    # @brief handle a failure
    #
    def handleFailure(self):
        self.logger.log(Log.DEBUG, "WorkflowMonitor:handleFailure")
