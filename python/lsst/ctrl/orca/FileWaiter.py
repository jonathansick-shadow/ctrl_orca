#!/usr/bin/env python

import os
import sys
import time
from lsst.pex.logging import Log


#
#
class FileWaiter:
    def __init__(self, remoteNode, remoteFileWaiter, fileListName, logger = None):
        self.logger = logger
        if self.logger != None:
            self.logger.log(Log.DEBUG, "FileWaiter:__init__")
        self.remoteNode = remoteNode
        self.fileListName = fileListName
        self.remoteFileWaiter = remoteFileWaiter

    def waitForFirstFile(self):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "FileWaiter:waitForFirstFile")
        print "waiting for log file to be created to confirm launch."
        cmd = "gsissh %s %s -f %s" % (self.remoteNode, self.remoteFileWaiter, self.fileListName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]

    def waitForAllFiles(self):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "FileWaiter:waitForAllFiles")

        print "waiting for all log files to be created to confirm launch"
        cmd = "gsissh %s %s -l %s" % (self.remoteNode, self.remoteFileWaiter, self.fileListName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]

