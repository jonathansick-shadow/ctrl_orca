import threading
import lsst.ctrl.events as events

class EventListener(threading.Thread):

    def __init__(self, topic, resolver):
        self.host = host
        self.topic = topic
        self.resolver = resolver

    def run(self):
            eventSystem = events.EventSystem.getDefaultEventSystem()
            # block on receive
            while True:
                propertySet = eventSystem.receive(self.topic)
                self.resolver.execute(propertySet)
