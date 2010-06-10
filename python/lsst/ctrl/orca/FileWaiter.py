#!/usr/bin/env python

import os
import sys
import time
from lsst.pex.logging import Log


#
#
class FileWaiter:
    def __init__(self, remoteNode, remoteFileWaiter, fileList, logger = None):
        self.logger = logger
        if self.logger != None:
            self.logger.log(Log.DEBUG, "FileWaiter:__init__")
        self.remoteNode = remoteNode
        self.remoteFileWaiter = remoteFileWaiter
        self.fileList = fileList

    def waitForFirstFile(self):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "FileWaiter:waitForFirstFile")
        item = self.fileList[0]
        if self.logger != None:
            self.logger.log(Log.DEBUG, "FileWaiter:waiting for this file to be created %s" % item)
        cmd = "gsissh %s %s %s" % (self.remoteNode, self.remoteFileWaiter, item)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]

    def waitForAllFiles(self):
        if self.logger != None:
            self.logger.log(Log.DEBUG, "FileWaiter:waitForAllFiles")
        list = ""
        for item in self.fileList:
            list = list+" "+item
        if self.logger != None:
            self.logger.log(Log.DEBUG, "FileWaiter:waiting for these files to be created %s" % list)
        cmd = "gsissh %s %s %s" % (self.remoteNode, self.remoteFileWaiter, list)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh",cmd.split())
        os.wait()[0]

