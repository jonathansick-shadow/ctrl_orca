##
# @brief skeleton template class to be subclassed to use with EventListener
#            to resolve incoming messages
#
class EventResolver:
    def __init__(self):
        return

    ##
    # @brief process event message
    def execute(self,x):
        print x
