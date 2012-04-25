import sys
import lsst.pex.config as pexConfig
import PipelineDefinitionConfig as pipe
import FakeTypeMap as fake
from lsst.ctrl.orca.config.DirectoryConfig import DirectoryConfig

class AppStageConfig(pexConfig.Config):
    parallelClass = pexConfig.Field("class",str)
    eventTopic = pexConfig.Field("topic name",str)
    stageConfig = pexConfig.Field("config name",str)

class ExecuteConfig(pexConfig.Config):
    shutdownTopic = pexConfig.Field("shutdown topic",str)
    eventBrokerHost = pexConfig.Field("event broker host",str)
    dir = pexConfig.ConfigField("directories",DirectoryConfig)
    appStageOrder = pexConfig.ListField("stage order",str)
    appStage = pexConfig.ConfigChoiceField("appStage",fake.FakeTypeMap(AppStageConfig))
    failureStage = pexConfig.ConfigChoiceField("appStage",AppStageConfig)

class FrameworkConfig(pexConfig.Config):
    script = pexConfig.Field("program to execute",str)
    type = pexConfig.Field("type",str)
    environment = pexConfig.Field("environment to set up",str)

class DeployConfig(pexConfig.Config):
    processesOnNode = pexConfig.ListField("processes",str)

class PipelineDefinitionConfig(pexConfig.Config):
    execute = pexConfig.ConfigField("execute",ExecuteConfig)
    framework = pexConfig.ConfigField("execute",FrameworkConfig)
        

class PipelineConfig(pexConfig.Config):
    definition = pexConfig.ConfigField("definition",PipelineDefinitionConfig)
    deploy = pexConfig.ConfigField("deployment info",DeployConfig)
    runCount = pexConfig.Field("job definition",int)
    launch = pexConfig.Field("job definition",bool)

