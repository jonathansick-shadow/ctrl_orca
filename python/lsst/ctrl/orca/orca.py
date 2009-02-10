#!/usr/bin/env python

from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import optparse, traceback, time
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from ProductionRunManager import ProductionRunManager 

usage = """usage: %%prog [-n] pipelinePolicyFile runId"""


opts = {}
args = []

parser = optparse.OptionParser(usage)
# TODO: handle "--dryrun"
parser.add_option("-n", "--dryrun", type="string", action="store", dest="dryrun", default=None, help="print messages, but don't execute anything")

# parse and check command line arguments
(opts, args) = parser.parse_args()
if len(args) < 2:
    print usage
    raise RuntimeError("Missing args: pipelinePolicyFile runId")

pipelinePolicyFile = args[0]
runId = args[1]

# TODO: add log messages for these
print "pipelinePolicyFile = "+pipelinePolicyFile
print "runId = "+runId

policy = Policy.createPolicy(pipelinePolicyFile)

# check environment to make sure we're ready to go
packageDirEnv = policy.get("packageDirectoryEnv")
if not os.environ.has_key(packageDirEnv):
    raise RuntimeError(packageDirEnv+" environment variable not set")


# create the ProductionRunManager, configure it, and launch it
productionRunManager = ProductionRunManager(policy)
productionRunManager.configure(runId)
productionRunManager.launch()

#
# Check any runtime prequisites (environment variables, files that need
# to be set up, etc)
