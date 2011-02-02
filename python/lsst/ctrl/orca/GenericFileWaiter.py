#!/usr/bin/env python

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


import os
import sys
import time
from lsst.pex.logging import Log


#
#
class GenericFileWaiter:
    def __init__(self, fileNames, logger = None):
        self.logger = logger
        if self.logger != None:
            self.logger.log(Log.DEBUG, "GenericFileWaiter:__init__")
        self.fileNames = fileNames

    def waitForFirstFile(self):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "GenericFileWaiter:waitForFirstFile")
        print "waiting for log file to be created to confirm launch."

        while os.path.exists(self.fileNames[0]) == False:
            time.sleep(1)
        return

    def waitForAllFiles(self):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "GenericFileWaiter:waitForAllFiles")

        print "waiting for all log files to be created to confirm launch"

        list = self.fileNames
        while len(list):
            newlist = [ item for item in list if (os.path.exists(item) == False)]
            list = newlist
            time.sleep(1)
        return
