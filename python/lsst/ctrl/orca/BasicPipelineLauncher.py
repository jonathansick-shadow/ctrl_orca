import subprocess
import lsst.ctrl.orca as orca

from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.PipelineMonitor import PipelineMonitor

class BasicPipelineLauncher:
    def __init__(self, runid, policy, pipeline, masterNode, dirs, script, logger, verbosity):
        self.logger = logger
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:__init__")
        self.runid = runid
        self.pipeline = pipeline
        self.policy = policy
        self.masterNode = masterNode
        self.dirs = dirs
        self.script = script
        self.pipelineVerbosity = verbosity

    def launch(self):
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:launch")
        self.launchPipeline()

        self.pipelineMonitor = PipelineMonitor(self.logger)
        return self.pipelineMonitor # returns PipelineMonitor

    def cleanUp(self):
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:cleanUp")

    def checkConfiguration(self, care):
        # the level of care taken in the checks.  In general, the higher
        # the number of checks that will be done.
        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:checkConfiguration")

    def launchPipeline(self):

        self.logger.log(Log.DEBUG, "BasicPipelineLauncher:launchPipeline")

        execPath = self.policy.get("configuration.framework.exec")
        launchcmd = EnvString.resolve(execPath)
        # kick off the run

        cmd = ["ssh", self.masterNode, "cd %s; source %s; %s %s %s -L %s" % (self.dirs.get("work"), self.script, launchcmd, self.pipeline+".paf", self.runid, self.pipelineVerbosity) ]
        if orca.dryrun == True:
            print "dryrun: would execute"
            print cmd
        else:
            self.logger.log(Log.DEBUG, "launching pipeline")

            # by convention the first node in the list is the "master" node
                       
            self.logger.log(Log.INFO, "launching %s on %s" % (self.pipeline, self.masterNode) )
            self.logger.log(Log.DEBUG, "executing: " + " ".join(cmd))

            if subprocess.call(cmd) != 0:
                raise RuntimeError("Failed to launch " + self.pipeline)
