import lsst.ctrl.orca as orca
from lsst.pex.logging import Log

class EventMonitor:

    def __init__(self, monitorFile, logger=None):
        if logger is None:  logger = orca.logger
        self.logger = Log(logger, "eventMonitor")

        self.logger.log(Log.DEBUG,"EventMonitor:init")
        # create and initialize Event Monitor

    def start(self):
        self.logger.log(Log.DEBUG, "EventMonitor:start")

    def stop(self):
        self.logger.log(Log.DEBUG, "EventMonitor:stop")
