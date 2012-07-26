import sys
import lsst.pex.config as pexConfig
import PipelineDefinitionConfig as pipe
import FakeTypeMap as fake

class ScriptTemplateConfig(pexConfig.Config):
    inputFile = pexConfig.Field("input file", str)
    keywords = pexConfig.DictField("key value pairs",keytype=str, itemtype=str, default=dict())
    outputFile = pexConfig.Field("output file", str)

class JobTemplateConfig(pexConfig.Config):
    script = pexConfig.ConfigField("job script", ScriptTemplateConfig)
    condor = pexConfig.ConfigField("template", ScriptTemplateConfig)

class ScriptConfig(pexConfig.Config):
    script = pexConfig.ConfigField("job script", ScriptTemplateConfig)

class DagGeneratorConfig(pexConfig.Config):
    dagName = pexConfig.Field("dag name", str)
    script = pexConfig.Field("script", str)
    input = pexConfig.Field("input", str)
    idsPerJob = pexConfig.Field("the number of ids that will be handled per job", int)

class TaskConfig(pexConfig.Config):
    scriptDir = pexConfig.Field("script directory",str)
    preScript = pexConfig.ConfigField("pre script", ScriptConfig)
    preJob = pexConfig.ConfigField("pre job", JobTemplateConfig)
    postJob = pexConfig.ConfigField("post job", JobTemplateConfig)
    workerJob = pexConfig.ConfigField("worker job", JobTemplateConfig)
    dagGenerator = pexConfig.ConfigField("dagGenerator", DagGeneratorConfig)
