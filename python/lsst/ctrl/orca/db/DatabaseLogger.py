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


from lsst.cat.MySQLBase import MySQLBase
import MySQLdb
import os
import sys
import subprocess

class DatabaseLogger(MySQLBase):

    def __init__(self, dbHostName, portNumber):
        MySQLBase.__init__(self, dbHostName, portNumber)

        self.keywords = ['HOSTID', 'RUNID', 'SLICEID', 'LEVEL', 'LOG', 'DATE', 'node', 'TIMESTAMP', 'COMMENT', 'STATUS', 'PIPELINE', 'EVENTTIME', 'PUBTIME', 'TYPE', 'STAGEID', 'LOOPNUM', 'workerid', 'usertime', 'systemtime', 'stagename']
        self.keywordSet = set(self.keywords)
        self.highwater = 10

    def insertRecords(self, dbTable, msgs):
        cnt = len(msgs)
        while cnt > 0:
            if cnt > self.highwater:
                ins = ""
                for i in range(0,self.highwater+1):
                    event = msgs.pop(0)
                    ins = ins + self.createInsertString(dbTable, event.getPropertySet())
                self.execCommand0(ins)
            elif cnt > 0:
                ins = ""
                for i in range(0,cnt):
                    event = msgs.pop(0)
                    ins = ins + self.createInsertString(dbTable, event.getPropertySet())
                self.execCommand0(ins)
            cnt = len(msgs)

    def insertRecord(self, dbTable, ps):
        ins = self.createInsertString(dbTable,ps)
        self.execCommand0(ins)


    def createInsertString(self, dbTable, ps):
        hostId = ps.get("HOSTID")
        hostId = MySQLdb.escape_string(hostId)

        runId = ps.get("RUNID")
        runId = MySQLdb.escape_string(runId)

        sliceId = ps.get("SLICEID")
        level = ps.get("LEVEL")

        log = ps.get("LOG")
        log = MySQLdb.escape_string(log)

        date = ps.get("DATE")
        date = MySQLdb.escape_string(date)
        
        ts = ps.get("TIMESTAMP")
        eventtime = ps.get("EVENTTIME")
        pubtime = ps.get("PUBTIME")
        eventtype = ps.get("TYPE")
        eventtype = MySQLdb.escape_string(eventtype)

        if ps.exists("node"):
            node = ps.get("node")
        else:
            node = -1

        timestamp = ts.nsecs()

        commentList = ps.get("COMMENT")
        comment = ""
        
        if ps.valueCount("COMMENT") == 1:
            comment = commentList
        else:
            for i in commentList:
                if comment == "":
                    comment = i
                else:
                    comment = comment+";"+i


        if (ps.exists("TOPIC")):
            ps.remove("TOPIC")

        if ps.exists("STATUS"):
            status = ps.get("STATUS")
            status = MySQLdb.escape_string(status)
        else:
            status = "NULL"

        if ps.exists("PIPELINE"):
            pipeline = ps.get("PIPELINE")
            pipeline = MySQLdb.escape_string(pipeline)
        else:
            pipeline = "NULL"

        if ps.exists("STAGEID"):
            stageid = ps.get("STAGEID")
        else:
            stageid = "-1"

        if ps.exists("LOOPNUM"):
            loopnum = ps.get("LOOPNUM")
        else:
            loopnum = "-1"

        if ps.exists("workerid"):
            workerid = ps.get("workerid")
            workerid = MySQLdb.escape_string(workerid)
        else:
            workerid = "NULL"


        if ps.exists("usertime"):
            usertime = ps.get("usertime")
        else:
            usertime = 0

        if ps.exists("systemtime"):
            systemtime = ps.get("systemtime")
        else:
            systemtime = 0

        names = ps.names()
        namesSet = set(names)

        diff = namesSet.difference(self.keywordSet)

        custom = ""
        for name in diff:
            if custom == "":
                custom = "%s : %s;" % (name,ps.get(name))
            else:
                custom = custom+ "%s : %s;" % (name,ps.get(name))
        if custom == "":
            custom = "NULL"
        custom = MySQLdb.escape_string(custom[0:4096])
        comment = MySQLdb.escape_string(comment[0:2048])

        cmd = """INSERT INTO %s(HOSTID, RUNID, SLICEID, STATUS, LEVEL, LOG, DATE, node, TIMESTAMP, custom, COMMENT, PIPELINE, EVENTTIME, PUBTIME, TYPE, STAGEID, LOOPNUM, workerid, usertime, systemtime, stagename) values("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s"); """ % (dbTable, hostId, runId, sliceId, status, level, log, date, node, timestamp, custom, comment, pipeline, eventtime, pubtime, eventtype, stageid, loopnum, workerid, usertime, systemtime, stagename)

        return cmd
