import sys
import lsst.pex.config as pexConfig
import PipelineDefinitionConfig as pipe

class PipelineConfig(pexConfig.Config):
    shortName = pexConfig.Field("short name",str)
    definition = pexConfig.ConfigField("job definition", pipe.PipelineDefinitionConfig)
    runCount = pexConfig.Field("job definition",int)
    launch = pexConfig.Field("job definition",bool)
