import threading
import lsst.ctrl.events as events

##
# @brief object that listens for incoming events, and delegates the event to be resolved
#
# This class listens on an LSST event "topic" and when a message is received, it hands
# off that message to a "resolver" object which interprets the message
#

class EventListener(threading.Thread):

    ##
    # @brief listen for events, and act on them
    # @param topic the event topic to listen to
    # @param resolver the object used to decide what to do with the incoming message
    #
    def __init__(self, topic, resolver):
        self.topic = topic
        self.resolver = resolver

    def run(self):
            eventSystem = events.EventSystem.getDefaultEventSystem()
            # block on receive
            while True:
                propertySet = eventSystem.receive(self.topic)
                self.resolver.execute(propertySet)
