import lsst.pex.config as pexConfig
from lsst.ctrl.orca.config.DirectoryConfig import DirectoryConfig

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
    dir = pexConfig.ConfigField("directory info",DirectoryConfig)
    hw = pexConfig.ConfigField("hardware info",HwConfig)
    deploy = pexConfig.ConfigField("deploy",DeployConfig)
