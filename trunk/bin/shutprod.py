#!/usr/bin/env python
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
import sys

# usage:  python bin/shutprod.py urgency_level runid
if __name__ == "__main__":
    host = "lsst8.ncsa.uiuc.edu"
    
    topic = "productionShutdown"

    level = int(sys.argv[1])
    runid = sys.argv[2]

    trans = events.EventTransmitter(host, topic)
    eventSystem = events.EventSystem.getDefaultEventSystem()
    
    root = PropertySet()
    root.setInt("level",level)
    
    id = eventSystem.createOriginatorId()
    event = events.StatusEvent(runid, id, root)
    trans.publishEvent(event)
