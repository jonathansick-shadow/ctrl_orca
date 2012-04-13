import sys
import lsst.pex.config as pexConfig

class PipelineConfig(pexConfig.Config):
    shortName = pexConfig.Field("short name",str)
    definition = pexConfig.Field("job definition",str)
    runCount = pexConfig.Field("job definition",int)
    launch = pexConfig.Field("job definition",bool)
