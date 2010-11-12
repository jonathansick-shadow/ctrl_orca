#! /usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#


from __future__ import with_statement
import optparse, os, os.path, subprocess, shutil, time
from lsst.pex.logging import Log

pkgdirvar = "CTRL_DC3PIPE_DIR"
eventgenerator = "eventgenerator.py lsst8"

usage = """usage: %%prog exposureList...

Send events to an event broker, to kick off pipeline processing of exposures.
"""

logger = Log(Log.getDefaultLog(), "dc3")

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

    #print "Pausing for 15s, waiting for setup..."
    #logger.log(Log.DEBUG, "Pausing for 15s, waiting for setup...")
    #time.sleep(15)

    logger.log(Log.INFO, "Sending exposure data from " + efile)
    logger.log(Log.DEBUG,
               "executing: %s < %s"  % (eventgenerator, efile))
    print "executing: %s < %s"  % (eventgenerator, efile)

    with file(efile) as expfile:
        if subprocess.call(eventgenerator.split(), stdin=expfile) != 0:
            raise RuntimeError("Failed to execute eventgenerator")
