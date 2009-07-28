class PipelineLauncher:
    def __init__(self):
        self.logger.log(Log.DEBUG, "PipelineLauncher:__init__")

    def launch(self):
        self.logger.log(Log.DEBUG, "PipelineLauncher:launch")
        return 0 # returns PipelineMonitor

    def cleanUp(self):
        self.logger.log(Log.DEBUG, "PipelineLauncher:cleanUp")

    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "PipelineLauncher:checkConfiguration")
