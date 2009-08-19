from lsst.pex.logging import Log

class PipelineMonitor:
    def __init__(self, logger):
        self.logger = logger
        self.logger.log(Log.DEBUG, "PipelineMonitor:__init__")

    def isRunning(self):
        self.logger.log(Log.DEBUG, "PipelineMonitor:isRunnable")
        return True

    def stopPipeline(self, timeout):
        self.logger.log(Log.DEBUG, "PipelineMonitor:stopPipeline")

    def handleEvent(self, event):
        # even - an event delivered to this pipeline
        self.logger.log(Log.DEBUG, "PipelineMonitor:handleEvent")

    def handleFailure(self):
        self.logger.log(Log.DEBUG, "PipelineMonitor:handleFailure")
