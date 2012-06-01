import sys
import lsst.pex.config as pexConfig
import PipelineDefinitionConfig as pipe
import FakeTypeMap as fake

#class PreScriptTemplateConfig(pexConfig.Config):
#    template = pexConfig.Field("template", str)
#    outputFile = pexConfig.Field("output file", str)

class ScriptTemplateConfig(pexConfig.Config):
    template = pexConfig.Field("template", str)
    outputFile = pexConfig.Field("output file", str)

class JobTemplateConfig(pexConfig.Config):
    script = pexConfig.ConfigField("job script", ScriptTemplateConfig)
    template = pexConfig.Field("template", str)
    outputFile = pexConfig.Field("output file", str)

class DagGeneratorConfig(pexConfig.Config):
    dagName = pexConfig.Field("dag name", str)
    script = pexConfig.Field("script", str)
    input = pexConfig.Field("input", str)

class TaskConfig(pexConfig.Config):
    scriptDir = pexConfig.Field("script directory",str)
    preScript = pexConfig.ConfigField("pre script", ScriptTemplateConfig)
    preJob = pexConfig.ConfigField("pre job", JobTemplateConfig)
    postJob = pexConfig.ConfigField("post job", JobTemplateConfig)
    workerJob = pexConfig.ConfigField("worker job", JobTemplateConfig)
    dagGenerator = pexConfig.ConfigField("dagGenerator", DagGeneratorConfig)
