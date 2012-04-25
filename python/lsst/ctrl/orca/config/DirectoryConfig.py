import sys
import lsst.pex.config as pexConfig

class DirectoryConfig(pexConfig.Config):
    defaultRoot = pexConfig.Field("default root",str)
    runDirPattern = pexConfig.Field("pattern",str)
    workDir = pexConfig.Field("work directory",str)
    inputDir = pexConfig.Field("input directory",str)
    outputDir = pexConfig.Field("output directory",str)
    updateDir = pexConfig.Field("update directory",str)
    scratchDir = pexConfig.Field("scratch directory",str)
