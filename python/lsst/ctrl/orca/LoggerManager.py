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
import re
import time
import signal
from subprocess import Popen
from lsst.pex.logging import Log


#
# This class is highly dependent on the output of the condor commands 
# condor_submit and condor_q
#
class LoggerManager:
    def __init__(self, logger, broker, dbHost, dbPort, runid, dbName):
        self.logger = logger
        self.logger.log(Log.DEBUG, "LoggerManager:__init__")
        self.broker = broker
        self.dbHost = dbHost
        self.dbPort = dbPort
        self.runid = runid
        self.dbName = dbName
        self.process = None
        return


    def getPID(self):
        return self.process.pid

    def start(self):
        self.logger.log(Log.DEBUG, "LoggerManager:start")
        if self.process != None:
            return

        directory = os.getenv("CTRL_ORCA_DIR")
        self.process = Popen("%s/bin/Logger.py %s %s %s %s %s" % (directory, self.broker, self.dbHost, self.dbPort, self.runid, self.dbName), shell=True)
        return

    def stop(self):
        self.logger.log(Log.DEBUG, "LoggerManager:stop")
        if self.process == None:
            return

        try :
            os.kill(self.process.pid, signal.SIGKILL)
            os.waitpid(self.process.pid,0)
            self.process = None
            self.logger.log(Log.DEBUG, "LoggerManager:stop: killed Logger process")
        except Exception,e:
            self.logger.log(Log.DEBUG, "LoggerManager:stop: tried to kill Logger process, but it didn't exist")
            
        return
