class ExecuteConfig(pexConfig.Config):
    shutdownTopic = pexConfig.Field("shutdown topic",str)
    eventBrokerHost = pexConfig.Field("event broker host",str)

class FrameworkConfig(pexConfig.Config):
    exec = pexConfig.Field("exec",str)
    type = pexConfig.Field("type",str)
    environment = pexConfig.Field("environment",str)

class JobOfficeConfig(pexConfig.Config):
    execute = pexConfig.ConfigField("execute",ExecuteConfig)
    framework = pexConfig.ConfigField("framework",FrameworkConfig)
