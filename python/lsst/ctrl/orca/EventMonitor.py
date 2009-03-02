from lsst.pex.logging import Log

class EventMonitor:

    def __init__(self, monitorFile):
        self.logger = Log(Log.getDefaultLog(), "d3pipe")

        self.logger.log(Log.DEBUG,"EventMonitor:init")
        # create and initialize Event Monitor

    def start(self):
        self.logger.log(Log.DEBUG, "EventMonitor:start")

    def stop(self):
        self.logger.log(Log.DEBUG, "EventMonitor:stop")
