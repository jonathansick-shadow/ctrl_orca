import lsst.pex.config as pexConfig

class DirConfig(pexConfig.Config):
    defaultRoot = pexConfig.Field("root",str)
    runDirPattern = pexConfig.Field("substitution pattern",str)
    workDir = pexConfig.Field("work directory",str)
    inputDir = pexConfig.Field("input directory",str)
    outputDir = pexConfig.Field("output directory",str)
    updateDir = pexConfig.Field("update directory",str)
    scratchDir = pexConfig.Field("scratch directory",str)

class HwConfig(pexConfig.Config):
    nodeCount = pexConfig.Field("number of nodes",int)
    minCoresPerNode = pexConfig.Field("minimum cores per node",int)
    maxCoresPerNode = pexConfig.Field("maximum cores per node",int)
    minRamPerNode = pexConfig.Field("minimum RAM per node",float)
    maxRamPerNode = pexConfig.Field("maximum RAM per node",float)

class DeployConfig(pexConfig.Config):
    defaultDomain = pexConfig.Field("default internet domain",str)
    nodes = pexConfig.ListField("node names",str)

class PlatformConfig(pexConfig.Config):
    dir = pexConfig.ConfigField("directory info",DirConfig)
    hw = pexConfig.ConfigField("hardware info",HwConfig)
    deploy = pexConfig.ConfigField("deploy",DeployConfig)
