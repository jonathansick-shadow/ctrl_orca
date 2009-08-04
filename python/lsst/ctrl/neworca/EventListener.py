import threading

class EventListener(threading.Thread):

    def __init__(self, topic, executor):
        self.host = host
        self.topic = topic
        self.executor = executor

    def run(self):
            eventSystem = events.EventSystem.getDefaultEventSystem()
            # block on receive
            propertySet = eventSystem.receive(self.topic)
            executor.execute(propertySet)
