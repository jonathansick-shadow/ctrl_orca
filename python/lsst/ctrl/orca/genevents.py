#! /usr/bin/env python

from __future__ import with_statement
import optparse, os, os.path, subprocess, shutil, time
from lsst.pex.logging import Log

pkgdirvar = "DC2PIPE_DIR"
eventgenerator = "eventgenerator.py lsst8"

usage = """usage: %%prog exposureList...

Send events to an event broker, to kick off pipeline processing of exposures.
"""

logger = Log(Log.getDefaultLog(), "dc2pipe")

cl = optparse.OptionParser(usage)

cl.opts = {}
cl.args = []

(cl.opts, cl.args) = cl.parse_args();

exposureFiles = cl.args[0:]

# send input data events
for efile in exposureFiles:
    if not os.path.isabs(efile):
        if os.path.exists(efile) and not efile.startswith("."):
            efile = os.path.join(".", efile)
        elif os.path.exists(os.path.join(os.environ[pkgdirvar],
                                         "exposureLists",efile)):
            efile = os.path.join(os.environ[pkgdirvar],
                                "exposureLists",efile)
    if not os.path.exists(efile):
        logger.log(Log.WARN, "Exposure list file not found: " + efile)

    print "Pausing for 15s, waiting for setup..."
    logger.log(Log.DEBUG, "Pausing for 15s, waiting for setup...")
    # time.sleep(15)

    logger.log(Log.INFO, "Sending exposure data from " + efile)
    logger.log(Log.DEBUG,
               "executing: %s < %s"  % (eventgenerator, efile))
    print "executing: %s < %s"  % (eventgenerator, efile)

    with file(efile) as expfile:
        if subprocess.call(eventgenerator.split(), stdin=expfile) != 0:
            raise RuntimeError("Failed to execute eventgenerator")
