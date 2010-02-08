from lsst.pex.logging import Log

class WorkflowMonitor:
    ##
    # @brief
    #
    def __init__(self, logger):

        # _locked: a container for data to be shared across threads that 
        # have access to this object.
        self._locked = threading.SharedData(False,
                                            {"running": False, "done": False})

        if not logger:
            logger = Log.getDefaultLog()
        self.logger = Log(logger, "monitor")
        self.logger.log(Log.DEBUG, "WorkflowMonitor:__init__")


    ##
    # @brief return True if the workflow being monitored appears to still be
    #        running
    #
    def isRunning(self):
        return self._locked.running

    ##
    # @brief determine whether workflow has completed
    #
    def isDone(self):
        return self._locked.done

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
