import lsst.ctrl.orca as orca
from lsst.pex.logging import Log

class EventMonitor:

    def __init__(self, monitorFile):
        if orca.logger == None:
            orca.logger = Log(Log.getDefaultLog(), "d3pipe")

        orca.logger.log(Log.DEBUG,"EventMonitor:init")
        # create and initialize Event Monitor

    def start(self):
        orca.logger.log(Log.DEBUG, "EventMonitor:start")

    def stop(self):
        orca.logger.log(Log.DEBUG, "EventMonitor:stop")
