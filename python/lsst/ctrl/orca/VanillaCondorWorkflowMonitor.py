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
import os, sys, subprocess, threading, time
import lsst.ctrl.events as events
import lsst.pex.logging as logging

from lsst.daf.base import PropertySet
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.multithreading import SharedData

class VanillaCondorWorkflowMonitor(WorkflowMonitor):
    ##
    # @brief in charge of monitoring and/or controlling the progress of a
    #        running workflow.
    #
    def __init__(self, eventBrokerHost, shutdownTopic, runid, pipelineNames, loggerManagers, logger):

        #self.__init__(logger)

        # _locked: a container for data to be shared across threads that 
        # have access to this object.
        self._locked = SharedData(False,
                                            {"running": False, "done": False})

        if not logger:
            logger = Log.getDefaultLog()
        self.logger = Log(logger, "monitor")
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowMonitor:__init__")
        self._statusListeners = []
        # make a copy of this liste, since we'll be removing things.
        self.pipelineNames = pipelineNames[:] 

        self.loggerPIDs = []
        for lm in loggerManagers:
            self.loggerPIDs.append(lm.getPID())
        self.loggerManagers = loggerManagers

        self._eventBrokerHost = eventBrokerHost
        self._shutdownTopic = shutdownTopic
        self.orcaTopic = "orca.monitor"
        self.runid = runid

        self._wfMonitorThread = None
        self.eventSystem = events.EventSystem.getDefaultEventSystem()
        self.originatorId = self.eventSystem.createOriginatorId()
        self.bSentLastLoggerEvent = False
        self.bSentJobOfficeEvent = False

        with self._locked:
            self._wfMonitorThread = VanillaCondorWorkflowMonitor._WorkflowMonitorThread(self, self._eventBrokerHost, self._shutdownTopic, self.orcaTopic, runid)


    class _WorkflowMonitorThread(threading.Thread):
        def __init__(self, parent, eventBrokerHost, shutdownTopic, eventTopic, runid):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self._parent = parent
            self._eventBrokerHost = eventBrokerHost
            self._shutdownTopic = shutdownTopic
            self._eventTopic = eventTopic
            selector = "RUNID = '%s'" % runid
            self._receiver = events.EventReceiver(self._eventBrokerHost, self._eventTopic, selector)
            self._Logreceiver = events.EventReceiver(self._eventBrokerHost, "LoggerStatus", selector)
            self._jobOfficeReceiver = events.EventReceiver(self._eventBrokerHost, "JobOfficeStatus", selector)

        def run(self):
            self._parent.logger.log(Log.DEBUG, "VanillaCondorWorkflowMonitor Thread started")
            sleepInterval = 5
            # we don't decide when we finish, someone else does.
            while True:
                # TODO:  this timeout value should go away when the GIL lock relinquish is implemented in events.
                if sleepInterval != 0:
                    time.sleep(sleepInterval)
                event = self._receiver.receiveEvent(1)

                logEvent = self._Logreceiver.receiveEvent(1)

                jobOfficeEvent = self._jobOfficeReceiver.receiveEvent(1)

                if jobOfficeEvent is not None:
                    val = self._parent.handleJobOfficeEvent(jobOfficeEvent)
                if event is not None:
                    val = self._parent.handleEvent(event)
                    if self._parent._locked.running == False:
                        print "and...done!"
                        return
                elif logEvent is not None:
                    val = self._parent.handleEvent(logEvent)
                    if self._parent._locked.running == False:
                        print "logger handled... and... done!"
                        return
                if (jobOfficeEvent is not None) or (jobOfficeEvent is not None) or (jobOfficeEvent is not None):
                    sleepInterval = 0
                else:
                    sleepInterval = 5


    def startMonitorThread(self, runid):
        with self._locked:
            self._wfMonitorThread.start()
            self._locked.running = True

    def handleJobOfficeEvent(self, event):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowMonitor:handleJobOfficeEvent")
        if event.getType() == events.EventTypes.STATUS:
            ps = event.getPropertySet()
            status = ps.get("STATUS")
            print "STATUS = "+status
            if status == "joboffice:done": 
                self.logger.log(Log.DEBUG, "VanillaCondorWorkflowMonitor:handleJobOfficeEvent joboffice:done received")
                self.stopWorkflow(1)
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowMonitor:handleJobOfficeEvent done")
        return


    def handleEvent(self, event):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowMonitor:handleEvent called")

        # make sure this is really for us.

        ps = event.getPropertySet()
        #print ps.toString()
        #print "==="

        if event.getType() == events.EventTypes.STATUS:
            ps = event.getPropertySet()
            #print ps.toString()
    
            if ps.exists("pipeline"):
                pipeline = ps.get("pipeline")
                print "pipeline-->",pipeline
                if pipeline in self.pipelineNames:
                    self.pipelineNames.remove(pipeline)
            elif ps.exists("logger.status"):
                loggerStatus = ps.get("logger.status")
                pid = ps.getInt("logger.pid")
                if pid in self.loggerPIDs:
                    self.loggerPIDs.remove(pid)

            cnt = len(self.pipelineNames)
            print "pipelineNames: "
            print self.pipelineNames
            # TODO: clean up to not specifically name "joboffices_1"
            if cnt == 1 and self.pipelineNames[0] == "joboffices_1" and self.bSentJobOfficeEvent == False:
                self.stopWorkflow(1)
                self.bSentJobOfficeEvent = True
            
            if (cnt == 0) and (self.bSentLastLoggerEvent == False):
                self.eventSystem.createTransmitter(self._eventBrokerHost, events.EventLog.LOGGING_TOPIC)
                evtlog = events.EventLog(self.runid, -1)
                tlog = logging.Log(evtlog, "orca.control")
                logging.LogRec(tlog, 1) << logging.Prop("STATUS", "eol") << logging.LogRec.endr
                self.bSentLastLoggerEvent = True

            # if both lists are empty we're finished.
            if (len(self.pipelineNames) == 0) and (len(self.loggerPIDs) == 0):
               with self._locked:
                   self._locked.running = False
        elif event.getType() == events.EventTypes.COMMAND:
            with self._locked:
                self._locked.running = False
        else:
            print "didn't handle anything"
    ##
    # @brief stop the workflow
    #
    def stopWorkflow(self, urgency):
        self.logger.log(Log.DEBUG, "VanillaCondorWorkflowMonitor:stopWorkflow")
        transmit = events.EventTransmitter(self._eventBrokerHost, self._shutdownTopic)
        
        root = PropertySet()
        root.setInt("level",urgency)
        root.setString("STATUS","shut things down")

        event = events.CommandEvent(self.runid, self.originatorId, 0, root)
        transmit.publishEvent(event)
