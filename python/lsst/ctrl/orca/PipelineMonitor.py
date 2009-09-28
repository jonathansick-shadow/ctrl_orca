from lsst.pex.logging import Log

class PipelineMonitor:
    ##
    # @brief
    #
    def __init__(self, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "PipelineMonitor:__init__")
        self.isActive = False

    ##
    # @brief return True if the pipeline being monitored appears to still be
    #        running
    #
    def isRunning(self):
        self.logger.log(Log.DEBUG, "PipelineMonitor:isRunnable")
        return self.isActive

    ##
    # @brief stop the pipeline
    #
    def stopPipeline(self, timeout):
        self.logger.log(Log.DEBUG, "PipelineMonitor:stopPipeline")

    ##
    # @brief handle an event
    #
    def handleEvent(self, event):
        # even - an event delivered to this pipeline
        self.logger.log(Log.DEBUG, "PipelineMonitor:handleEvent")

    ##
    # @brief handle a failure
    #
    def handleFailure(self):
        self.logger.log(Log.DEBUG, "PipelineMonitor:handleFailure")
