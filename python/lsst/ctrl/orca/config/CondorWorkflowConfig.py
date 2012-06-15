import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import FakeTypeMap as fake

class CondorDataConfig(pexConfig.Config):
    localScratch = pexConfig.Field("temp data area",str)

class TemplateConfig(pexConfig.Config):
    keywords = pexConfig.DictField("key value pairs",keytype=str, itemtype=str, default=dict())
    inputFile = pexConfig.Field("name of template to fill in",str, default=None)
    outputFile = pexConfig.Field("name of file to write",str, default=None)

class GlideinConfig(pexConfig.Config):
    template = pexConfig.ConfigField("condor template", TemplateConfig)

class CondorWorkflowConfig(pexConfig.Config):
    condorData = pexConfig.ConfigField("condor data",CondorDataConfig)
    glidein = pexConfig.ConfigField("glidein info",GlideinConfig)
