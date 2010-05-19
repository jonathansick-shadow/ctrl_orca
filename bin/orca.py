#!/usr/bin/env python

from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import optparse, traceback, time
import lsst.ctrl.orca as orca
import lsst.pex.harness.run as run
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.ProductionRunManager import ProductionRunManager 

usage = """usage: %prog [-ndvqsc] [-r dir] [-e script] [-V int][-L lev] pipelinePolicyFile runId"""

parser = optparse.OptionParser(usage)
# TODO: handle "--dryrun"
parser.add_option("-n", "--dryrun", action="store_true", dest="dryrun", default=False, help="print messages, but don't execute anything")

parser.add_option("-c", "--configcheck", action="store_true", dest="skipconfigcheck", default=False, help="skip configuration check")

parser.add_option("-V", "--verbosity", type="int", action="store", dest="verbosity", default=0, metavar="int", help="orca verbosity level (0=normal, 1=debug, -1=quiet, -3=silent)")
parser.add_option("-r", "--policyRepository", type="string", action="store",
              dest="repository", default=None, metavar="dir",
              help="directory containing policy files")
parser.add_option("-e", "--envscript", action="store", dest="envscript",
              default=None, metavar="script",
              help="an environment-setting script to source on pipeline platform")
parser.add_option("-d", "--debug", action="store_const", const=1, 
                  dest="verbosity", help="print maximum amount of messages")
parser.add_option("-v", "--verbose", action="store_const", const=1,
                  dest="verbosity", help="same as -d")
parser.add_option("-q", "--quiet", action="store_const", const=-1,
                  dest="verbosity", help="print only warning & error messages")
parser.add_option("-s", "--silent", action="store_const", const=-3,
                  dest="verbosity", help="print nothing (if possible)")
parser.add_option("-P", "--pipeverb", type="int", action="store", dest="pipeverb", default=0, metavar="int", help="pipeline verbosity level (0=normal, 1=debug, -1=quiet, -3=silent)")
run.addVerbosityOption(parser, dest="pipeverb")

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
orca.repository = parser.opts.repository
orca.envscript = parser.opts.envscript

# This is handled via lsst.ctrl.orca (i.e. lsst/ctrl/orca/__init__.py):
#
# orca.logger = Log(Log.getDefaultLog(), "orca")

orca.verbosity = parser.opts.verbosity
orca.logger.setThreshold(-10 * parser.opts.verbosity)

# set the dryrun singleton to the value set on the command line.
# we reference this in other classes
orca.dryrun = parser.opts.dryrun


orca.logger.log(Log.DEBUG,"pipelinePolicyFile = "+pipelinePolicyFile)
orca.logger.log(Log.DEBUG, "runId = "+runId)

# create the ProductionRunManager, configure it, and launch it
#productionRunManager = ProductionRunManager(runId, pipelinePolicyFile, orca.logger, pipelineVerbosity=parser.opts.pipeverb)
productionRunManager = ProductionRunManager(runId, pipelinePolicyFile, orca.logger, orca.repository)


productionRunManager.runProduction(skipConfigCheck=parser.opts.skipconfigcheck, workflowVerbosity=parser.opts.pipeverb)
productionRunManager.joinShutdownThread()
