import sys
import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import FakeTypeMap as fake

class DeployDataConfig(pexConfig.Config):
    dataRepository = pexConfig.Field("data repository",str)
    collection = pexConfig.Field("collection name",str)
    script = pexConfig.Field("script name",str)

class AnnounceDataConfig(pexConfig.Config):
    script = pexConfig.Field("announce script",str)
    topic = pexConfig.Field("event topic to broadcast on",str)
    inputdata = pexConfig.Field("input data file",str)

class ConfigurationConfig(pexConfig.Config):
    deployData = pexConfig.ConfigField("deploy data",DeployDataConfig)
    announceData = pexConfig.ConfigField("data announcement",AnnounceDataConfig)

class GenericWorkflowConfig(pexConfig.Config):
    shortName = pexConfig.Field("name of this workflow",str)
    platform = pexConfig.Field("platform configuration file",str)
    shutdownTopic = pexConfig.Field("topic used for shutdown events",str)
    configurationClass = pexConfig.Field("orca plugin class",str)
    configuration = pexConfig.ConfigField("configuration",ConfigurationConfig)
    pipelineNames = pexConfig.ListField("pipeline names",str)
    pipeline = pexConfig.ConfigChoiceField("pipeline",fake.FakeTypeMap(pipe.PipelineConfig))
