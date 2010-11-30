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


import getpass
import os
import sys
import subprocess
import lsst.ctrl.events as events

from lsst.ctrl.orca.db.DatabaseLogger import DatabaseLogger
from lsst.daf.persistence import DbAuth
from lsst.pex.policy import Policy
from threading import Thread
from threading import Condition


#
# retrieve command line arguments: [broker] [host] [port]
#
broker = sys.argv[1]
host = sys.argv[2]
port = sys.argv[3]
runid = sys.argv[4]
dbname = sys.argv[5]



#
# get database authorization info
#
home = os.getenv("HOME")
pol = Policy(home+"/.lsst/db-auth.paf")

dbAuth = DbAuth()
dbAuth.setPolicy(pol)

user = dbAuth.username(host,port)
password = dbAuth.password(host,port)

#
# create the logger for the database and connect to it
#
dbLogger = DatabaseLogger(host, int(port))

dbLogger.connect(user, password, dbname)

# incoming messages list
msgs = []

# set the highwater mark for the number of messages retrieved before attempting to drain them.
highwatermark = 40000

# set to the name of the file to write to
tmpFilename = "/dev/shm/logdb_"+getpass.getuser()+".txt"

# create an event receiver
receiver = events.EventReceiver(broker, events.EventLog.LOGGING_TOPIC, "RUNID='%s'" % runid)

# initialize the message counter
cnt = 0

#
# main loop - attempt to get messages until either no messages are retrieved, or until
# the highwater mark is reached.  If either of these happens, insert all current events
# in the incoming message list into the database
#
while True:
    event = receiver.receiveEvent(10)
    if event != None:
       msgs.append(event)
       cnt += 1
       if cnt >= highwatermark:
           dbLogger.insertRecords("%s.Logs" % dbname, msgs, tmpFilename)
           cnt = 0
    elif event == None:
        if len(msgs) > 0:
            dbLogger.insertRecords("%s.Logs" % dbname, msgs, tmpFilename)
            cnt = 0
            
dbLogger.disconnect()
