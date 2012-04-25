import sys
import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import FakeTypeMap as fake

class DeployDataConfig(pexConfig.Config):
    dataRepository = pexConfig.Field("data repository",str)
    collection = pexConfig.Field("collection name",str)
    script = pexConfig.Field("script name",str)

class DataCompletedConfig(pexConfig.Config):
    script = pexConfig.Field("completed script",str)
    topic = pexConfig.Field("event topic to broadcast on",str)
    status = pexConfig.Field("status message",str)

class AnnounceDataConfig(pexConfig.Config):
    script = pexConfig.Field("announce script",str)
    topic = pexConfig.Field("event topic to broadcast on",str)
    inputdata = pexConfig.Field("input data file",str)
    dataCompleted = pexConfig.ConfigField("data completion",DataCompletedConfig);

class GenericWorkflowConfig(pexConfig.Config):
    deployData = pexConfig.ConfigField("deploy data",DeployDataConfig)
    announceData = pexConfig.ConfigField("data announcement",AnnounceDataConfig)
