import lsst.pex.config as pexConfig

class ExecuteConfig(pexConfig.Config):
    shutdownTopic = pexConfig.Field("shutdown topic",str)
    eventBrokerHost = pexConfig.Field("event broker host",str)

class FrameworkConfig(pexConfig.Config):
    script = pexConfig.Field("program to execute",str)
    type = pexConfig.Field("type",str)
    environment = pexConfig.Field("environment to set up",str)

class PipelineDefinitionConfig(pexConfig.Config):
    execute = pexConfig.ConfigField("execute",ExecuteConfig)
    framework = pexConfig.ConfigField("execute",FrameworkConfig)
