import sys
import lsst.pex.config as pexConfig
import FakeTypeMap as fake
import WorkflowConfig as work

class ProductionLevelConfig(pexConfig.Config):
    configurationClass = pexConfig.Field("configuration class",str)

class Production(pexConfig.Config):
    shortName = pexConfig.Field("name of production", str)
    repositoryDirectory = pexConfig.Field("location of production repository", str)
    eventBrokerHost = pexConfig.Field("hostname of the event broker",str)
    productionShutdownTopic = pexConfig.Field("topic used to notify production shutdowns",str)
    configCheckCare = pexConfig.Field("config check",int)
    logThreshold = pexConfig.Field("logging threshold",int)
    configuration = pexConfig.ConfigField("production level confg",ProductionLevelConfig)


class AuthInfo(pexConfig.Config):
    host = pexConfig.Field("database host name",str)
    port = pexConfig.Field("database port number",int)


class RunCleanup(pexConfig.Config):
    daysFirstNotice = pexConfig.Field("when to first give notice",int)
    daysFinalNotice = pexConfig.Field("when to give final notice",int)

class DatabaseSystem(pexConfig.Config):
    authInfo = pexConfig.ConfigField("database authorization information",AuthInfo)
    runCleanup = pexConfig.ConfigField("runCleanup ",RunCleanup)

class DBLogger(pexConfig.Config):
    launch = pexConfig.Field("launch",bool)

class DatabaseConfiguration(pexConfig.Config):
    globalDbName = pexConfig.Field("global db name",str)
    dcVersion = pexConfig.Field("data challenge version",str)
    dcDbName = pexConfig.Field("data challenge database name",str)
    minPercDiskSpaceReq = pexConfig.Field("minimum percent disk space required",int)
    userRunLife = pexConfig.Field("user run life",int)

class Database(pexConfig.Config):
    name = pexConfig.Field("database name",str)
    system = pexConfig.ConfigField("database system info",DatabaseSystem)
    configurationClass = pexConfig.Field("configuration class",str)
    configuration = pexConfig.ConfigField("configuration dictionary",DatabaseConfiguration)
    logger = pexConfig.ConfigField("configuration dictionary",DBLogger)

class ProductionConfig(pexConfig.Config):
    production = pexConfig.ConfigField("production configuration",Production)
    databaseConfigNames = pexConfig.ListField("database configuration", str)
    database = pexConfig.ConfigChoiceField("database information", fake.FakeTypeMap(Database))
    workflowNames = pexConfig.ListField("workflow names",str)
    workflow = pexConfig.ConfigChoiceField("workflow",fake.FakeTypeMap(work.WorkflowConfig))
    configCheckCare = pexConfig.Field("config check care",int)
    configurationClass = pexConfig.Field("configuration class",str)

#config = ProductionConfig()
#config.load(sys.argv[1])
#
#print config
