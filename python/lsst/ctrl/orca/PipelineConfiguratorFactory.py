class PipelineConfiguratorFactory:
    def createPipelineConfigurator(self, type):
        # TODO: get the real names specified in the policy
        configurator = None
        if type == "basic":
           configurator = BasicPipelineConfigurator() 
        return configurator
