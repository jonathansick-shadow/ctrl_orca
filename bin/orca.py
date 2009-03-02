#!/usr/bin/env python

from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import optparse, traceback, time
import lsst.ctrl.orca as orca
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.ProductionRunManager import ProductionRunManager 

usage = """usage: %%prog [-n] pipelinePolicyFile runId"""



parser = optparse.OptionParser(usage)
# TODO: handle "--dryrun"
parser.add_option("-n", "--dryrun", action="store_true", dest="dryrun", default=False, help="print messages, but don't execute anything")
parser.add_option("-V", "--verbosity", type="int", action="store", dest="verbosity", default=0, metavar="int", help="verbosity level (0=normal, 1=debug, -1=quiet, -3=silent)")


parser.opts = {}
parser.args = []

# parse and check command line arguments
(parser.opts, parser.args) = parser.parse_args()
if len(parser.args) < 2:
    print usage
    raise RuntimeError("Missing args: pipelinePolicyFile runId")

pipelinePolicyFile = parser.args[0]
runId = parser.args[1]

orca.dryrun = parser.opts.dryrun

logger = Log(Log.getDefaultLog(), "d3pipe")
orca.verbosity = parser.opts.verbosity
logger.setThreshold(-10 * parser.opts.verbosity)

# set the dryrun singleton to the value set on the command line.
# we reference this in other classes
orca.dryrun = parser.opts.dryrun


logger.log(Log.DEBUG,"pipelinePolicyFile = "+pipelinePolicyFile)
logger.log(Log.DEBUG, "runId = "+runId)

policy = Policy.createPolicy(pipelinePolicyFile)

# check environment to make sure we're ready to go
packageDirEnv = policy.get("packageDirectoryEnv")
if not os.environ.has_key(packageDirEnv):
    raise RuntimeError(packageDirEnv+" environment variable not set")


# create the ProductionRunManager, configure it, and launch it
productionRunManager = ProductionRunManager(policy)
productionRunManager.configure(runId)
productionRunManager.checkConfiguration()
productionRunManager.launch()

#
# Check any runtime prequisites (environment variables, files that need
# to be set up, etc)
