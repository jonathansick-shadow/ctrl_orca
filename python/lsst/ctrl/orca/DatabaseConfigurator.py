##
#
#
#
#
# NOTE: This is here as a placeholder, and should be integrated with 
# db/Dc3aDatabaseConfigurator
#
#
#
#
# @brief
#
class DatabaseConfigurator:
    def __init__(self, runid, policy, logger):
        self.runid = runid
        self.policy = policy
        self.logger = logger
        return

    ##
    # @brief
    #
    def setDatabase(self, provSetup):

        # setup the database - using Dc3aDatabaseConfigurator as a placeholder
        dbConfigurator = Dc3aDatabaseConfigurator(self.runid, self.policy, self.logger)
        dbConfigurator.setup()

        if provSetup is not None:
            # may call provSetup.addProductionRecorder(ProvenanceRecorder)
            # may call provSetup.addWorkflowRecordCmd(string)
            return
