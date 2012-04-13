import lsst.pex.config as pexConfig

class DirConfig(pexConfig.Config):
    defaultRoot = pexConfig.Field("root",str)
    runDirPattern = pexConfig.Field("substitution pattern",str)
    work = pexConfig.Field("work directory",str)
    input = pexConfig.Field("input directory",str)
    output = pexConfig.Field("output directory",str)
    update = pexConfig.Field("update directory",str)
    scratch = pexConfig.Field("scratch directory",str)

class HwConfig(pexConfig.Config):
    nodeCount = pexConfig.nodeCount("number of nodes",int)
    minCoresPerNode = pexConfig.nodeCount("minimum cores per node",int)
    maxCoresPerNode = pexConfig.nodeCount("maximum cores per node",int)
    minRamPerNode = pexConfig.nodeCount("minimum RAM per node",float)
    maxRamPerNode = pexConfig.nodeCount("maximum RAM per node",float)

class DeployConfig(pexConfig.Config):
    managerClass = pexConfig.Field("class file",str)
    defaultDomain = pexConfig.Field("default internet domain",str)
    nodes = pexConfig.ListField("node names",str)

class PlatformConfig(pexConfig.Config):
    shortName = pexConfig.Field("name of platform",str)
    dir = pexConfig.ConfigField("directory info",DirConfig)
    hw = pexConfig.ConfigField("hardware info",HwConfig)
    deploy = pexConfig.ConfigField("deploy",DeployConfig)
