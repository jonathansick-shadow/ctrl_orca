class PipelineLauncher:
    def __init__(self):
        self.logger.log(Log.DEBUG, "PipelineLauncher:__init__")

    def launch(self):
        self.logger.log(Log.DEBUG, "PipelineLauncher:launch")
        return 0 # returns PipelineMonitor

    def cleanUp(self):
        self.logger.log(Log.DEBUG, "PipelineLauncher:cleanUp")

    def checkConfiguration(self, level):
        self.logger.log(Log.DEBUG, "PipelineLauncher:checkConfiguration")
