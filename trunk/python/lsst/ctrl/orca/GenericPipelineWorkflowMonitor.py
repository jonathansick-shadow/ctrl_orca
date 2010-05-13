from __future__ import with_statement
import os, sys, subprocess, threading, time
import lsst.ctrl.events as events

from lsst.daf.base import PropertySet
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.multithreading import SharedData

class GenericPipelineWorkflowMonitor(WorkflowMonitor):
    ##
    # @brief in charge of monitoring and/or controlling the progress of a
    #        running workflow.
    #
    def __init__(self, eventBrokerHost, shutdownTopic, runid, logger):

        #self.__init__(logger)

        # _locked: a container for data to be shared across threads that 
        # have access to this object.
        self._locked = SharedData(False,
                                            {"running": False, "done": False})

        if not logger:
            logger = Log.getDefaultLog()
        self.logger = Log(logger, "monitor")
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowMonitor:__init__")
        self._statusListeners = []

        self._eventBrokerHost = eventBrokerHost
        self._shutdownTopic = shutdownTopic
        self.runid = runid

        self._wfMonitorThread = None
        eventSystem = events.EventSystem.getDefaultEventSystem()
        self.originatorId = eventSystem.createOriginatorId()


    class _WorkflowMonitorThread(threading.Thread):
        def __init__(self, parent, eventBrokerHost, eventTopic, runid):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self._parent = parent
            self._eventBrokerHost = eventBrokerHost
            self._eventTopic = eventTopic
            selector = "RUNID = '%s'" % runid
            self._receiver = events.EventReceiver(self._eventBrokerHost, self._eventTopic, selector)

        def run(self):
            self._parent.logger.log(Log.DEBUG, "GenericPipelineWorkflowMonitor Thread started")
            # we don't decide when we finish, someone else does.
            while True:
                # TODO:  this timeout value should go away when the GIL lock relinquish is implemented in events.
                time.sleep(1)
                event = self._receiver.receiveEvent(1)
                if event is not None:
                    self._parent.handleEvent(event)

    def startMonitorThread(self, runid):
        with self._locked:
            self._wfMonitorThread = GenericPipelineWorkflowMonitor._WorkflowMonitorThread(self, self._eventBrokerHost, self._shutdownTopic, runid)
            self._wfMonitorThread.start()
            self._locked.running = True

    def handleEvent(self, event):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowMonitor:handleEventCalled")

        # make sure this is really for us.

        # TODO: Temporarily set to false no matter what event we get.  This needs to differentiate which events it receives.
        with self._locked:
            self._locked.running = False

    ##
    # @brief stop the workflow
    #
    def stopWorkflow(self, urgency):
        self.logger.log(Log.DEBUG, "GenericPipelineWorkflowMonitor:stopWorkflow")
        transmit = events.EventTransmitter(self._eventBrokerHost, self._shutdownTopic)
        
        root = PropertySet()
        root.setInt("level",urgency)
        root.setString("STATUS","shut things down")

        event = events.CommandEvent(self.runid, self.originatorId, 0, root)
        transmit.publishEvent(event)
