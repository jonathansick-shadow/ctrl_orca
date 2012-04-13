import sys
import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import FakeTypeMap as fake

class DeployDataConfig(pexConfig.Config):
    collection = pexConfig.Field("collection name",str)
    script = pexConfig.Field("script name",str)

class CondorDataConfig(pexConfig.Config):
    localScratch = pexConfig.Field("temp data area",str)
    loginNode = pexConfig.Field("node to use to perform SSH commands",str)
    ftpNode = pexConfig.Field("node to use to transfer files",str)
    transferProtocol = pexConfig.Field("method of file transfer",str)

class GlideinRequestConfig(pexConfig.Config):
    keyNames = pexConfig.ListField("keys",str)
    keyValuePairs = pexConfig.DictField("key value pairs",keytype=str, itemtype=str, default=dict())
    templateFileName = pexConfig.Field("name of template to fill in",str)
    outputFileName = pexConfig.Field("name of file to write",str)

class AnnounceDataConfig(pexConfig.Config):
    script = pexConfig.Field("announce script",str)
    topic = pexConfig.Field("event topic to broadcast on",str)
    inputdata = pexConfig.Field("input data file",str)

class VanillaCondorWorkflowConfig(pexConfig.Config):
    deployData = pexConfig.ConfigField("deploy data",DeployDataConfig)
    condorData = pexConfig.ConfigField("condor data",CondorDataConfig)
    glideinRequest = pexConfig.ConfigField("glidein info",GlideinRequestConfig)
    announceData = pexConfig.ConfigField("data announcement",AnnounceDataConfig)
