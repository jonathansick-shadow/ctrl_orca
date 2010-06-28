import os
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString

class DataAnnouncer:

    def __init__(self, runid, prodPolicy, wfPolicy, logger = None):
        self.logger = logger
        if self.logger is not None:
            self.logger.log(Log.DEBUG, "DataAnnouncer: __init__")
        self.runid = runid
        self.prodPolicy = prodPolicy
        self.wfPolicy = wfPolicy

    def announce(self):
        if self.logger is not None:
            self.logger.log(Log.DEBUG, "DataAnnouncer: announce")
        broker = self.prodPolicy.get("eventBrokerHost")
        config = None
        if self.wfPolicy.exists("configuration"):
            config = self.wfPolicy.get("configuration")
        else:
            print "configuration for workflow was not found"
            return False

        if config.exists("announceData"):
            annData = config.get("announceData")
            script = annData.get("script")
            script = EnvString.resolve(script)
            topic = annData.get("topic")
            inputdata = annData.get("inputdata")
            inputdata = EnvString.resolve(inputdata)
            cmd = "%s -r %s -b %s -t %s %s" % (script, self.runid, broker, topic, inputdata)
            print cmd
            cmdSplit = cmd.split()
            pid = os.fork()
            if not pid:
                os.execvp(cmdSplit[0], cmdSplit)
            os.wait()[0]
            return True
        else:
            return False


