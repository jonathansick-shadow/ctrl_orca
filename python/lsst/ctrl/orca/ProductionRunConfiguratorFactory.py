class ProductionRunConfiguratorFactory:
    def createProductionRunConfigurator(self, runid, policy, verbosity, logger):
        # TODO: get the real names specified in the policy, and store them in "type"
        configurator = None
        if type == "basic":
           configurator = BasicProductionRunConfigurator(runid, policy, verbosity, logger) 
        return configurator
