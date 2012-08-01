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
from lsst.ctrl.orca.CondorJobs import CondorJobs


class CondorWorkflowMonitor(WorkflowMonitor):
    ##
    # @brief in charge of monitoring and/or controlling the progress of a
    #        running workflow.
    #
    def __init__(self, eventBrokerHost, shutdownTopic, runid, condorDagId, loggerManagers, monitorConfig, logger):

        #self.__init__(logger)

        # _locked: a container for data to be shared across threads that 
        # have access to this object.
        self._locked = SharedData(False,
                                            {"running": False, "done": False})

        if not logger:
            logger = Log.getDefaultLog()
        self.logger = Log(logger, "monitor")
        self.logger.log(Log.DEBUG, "CondorWorkflowMonitor:__init__")
        self._statusListeners = []
        # make a copy of this liste, since we'll be removing things.

        self.loggerPIDs = []
        for lm in loggerManagers:
            self.loggerPIDs.append(lm.getPID())
        self.loggerManagers = loggerManagers

        self._eventBrokerHost = eventBrokerHost
        self._shutdownTopic = shutdownTopic
        self.orcaTopic = "orca.monitor"
        self.runid = runid
        self.condorDagId = condorDagId
        self.monitorConfig = monitorConfig

        self._wfMonitorThread = None
        self.eventSystem = events.EventSystem.getDefaultEventSystem()
        self.originatorId = self.eventSystem.createOriginatorId()
        self.bSentLastLoggerEvent = False

        with self._locked:
            self._wfMonitorThread = CondorWorkflowMonitor._WorkflowMonitorThread(self, self._eventBrokerHost, self._shutdownTopic, self.orcaTopic, runid, self.condorDagId, self.monitorConfig)


    class _WorkflowMonitorThread(threading.Thread):
        def __init__(self, parent, eventBrokerHost, shutdownTopic, eventTopic, runid, condorDagId, monitorConfig):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self._parent = parent
            self._eventBrokerHost = eventBrokerHost
            self._shutdownTopic = shutdownTopic
            self._eventTopic = eventTopic

            selector = "RUNID = '%s'" % runid
            self._receiver = events.EventReceiver(self._eventBrokerHost, self._eventTopic, selector)
            self._Logreceiver = events.EventReceiver(self._eventBrokerHost, "LoggerStatus", selector)
            self.condorDagId = condorDagId
            self.monitorConfig = monitorConfig

        def run(self):
            cj = CondorJobs(self._parent.logger)
            self._parent.logger.log(Log.DEBUG, "CondorWorkflowMonitor Thread started")
            statusCheckInterval = int(self.monitorConfig.statusCheckInterval)
            sleepInterval = statusCheckInterval
            # we don't decide when we finish, someone else does.
            while True:
                # TODO:  this timeout value should go away when the GIL lock relinquish is implemented in events.
                if sleepInterval != 0:
                    #print "sleep interval =",sleepInterval
                    time.sleep(sleepInterval)
                event = self._receiver.receiveEvent(1)

                logEvent = self._Logreceiver.receiveEvent(1)

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
                if (event is not None) or (logEvent is not None):
                    sleepInterval = 0
                else:
                    sleepInterval = statusCheckInterval
                # if the dag is no longer running, send the logger an event
                # telling it to clean up.
                if cj.isJobAlive(self.condorDagId) == False:
                    self._parent.sendLastLoggerEvent()
                
                

    def startMonitorThread(self, runid):
        with self._locked:
            self._wfMonitorThread.start()
            self._locked.running = True

    def handleEvent(self, event):
        self.logger.log(Log.DEBUG, "CondorWorkflowMonitor:handleEvent called")

        # make sure this is really for us.

        ps = event.getPropertySet()
        #print ps.toString()
        #print "==="

        # check for Logger event status
        if event.getType() == events.EventTypes.STATUS:
            ps = event.getPropertySet()
            #print ps.toString()
    
            if ps.exists("logger.status"):
                loggerStatus = ps.get("logger.status")
                pid = ps.getInt("logger.pid")
                print "logger.pid = ",pid
                if pid in self.loggerPIDs:
                    self.loggerPIDs.remove(pid)

            # if the logger list is empty, we're finished.
            if len(self.loggerPIDs) == 0:
               with self._locked:
                   self._locked.running = False
        elif event.getType() == events.EventTypes.COMMAND:
            # TODO: stop this thing right now.
            # that means the logger and the dag.
            with self._locked:
                self._locked.running = False
        else:
            print "didn't handle anything"

    def sendLastLoggerEvent(self):
        # send a message to the logger that we're done
        if self.bSentLastLoggerEvent == False:
            print "sending last Logger Event"
            self.eventSystem.createTransmitter(self._eventBrokerHost, events.EventLog.LOGGING_TOPIC)
            evtlog = events.EventLog(self.runid, -1)
            tlog = logging.Log(evtlog, "orca.control")
            logging.LogRec(tlog, 1) << logging.Prop("STATUS", "eol") << logging.LogRec.endr
            self.bSentLastLoggerEvent = True

    ##
    # @brief stop the workflow
    #
    def stopWorkflow(self, urgency):
        self.logger.log(Log.DEBUG, "CondorWorkflowMonitor:stopWorkflow")

        # do a condor_rm on the cluster id for the dag we submitted.
        cj = CondorJobs(self.logger)
        cj.killCondorId(self.condorDagId)

        self.sendLastLoggerEvent()
    

