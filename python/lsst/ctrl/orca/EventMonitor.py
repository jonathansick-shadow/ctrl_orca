import os
import signal
import subprocess
import lsst.ctrl.orca as orca
from lsst.pex.logging import Log

class EventMonitor:

    def __init__(self, runId, monitorFile, logger=None):
        # create and initialize Event Monitor
        self.runId = runId
        self.monitorFile = monitorFile
        self.proc = None
        

    def start(self):
        print "Starting "+self.monitorFile
        evmon_bindir = os.path.join(os.environ["CTRL_EVMON_DIR"], "bin")
        evmon_exec = os.path.join(evmon_bindir, "evmon")
        evmon_scriptdir = os.path.join(os.environ["CTRL_EVMON_DIR"], "scripts")
        evmon_script = os.path.join(evmon_scriptdir, self.monitorFile)
        str = "nohup "+evmon_exec+" "+evmon_script+" "+self.runId+"&"
        print str
        self.proc = subprocess.call("nohup "+evmon_exec+" "+evmon_script+" "+self.runId+"&", shell=True)

    def stop(self):
        print "Stopping "+self.monitorFile
        os.kill(self.proc.pid, signal.SIGHUP)
