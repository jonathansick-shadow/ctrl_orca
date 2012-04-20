import sys
import lsst.pex.config as pexConfig
import FakeTypeMap as fake
import WorkflowConfig as work

class AuthInfo(pexConfig.Config):
    host = pexConfig.Field("hostname",str)
    port = pexConfig.Field("database port",int)

class RunCleanup(pexConfig.Config):
    daysFirstNotice = pexConfig.Field("first notice",int)
    daysFinalNotice = pexConfig.Field("last notice",int)

class DatabaseSystem(pexConfig.Config):
    authInfo = pexConfig.ConfigField("database authorization information",AuthInfo)
    runCleanup = pexConfig.ConfigField("runCleanup ",RunCleanup)

class DBLogger(pexConfig.Config):
    launch = pexConfig.Field("launch",bool)

class ProductionDatabaseConfig(pexConfig.Config):
    globalDbName = pexConfig.Field("global db name",str)
    dcVersion = pexConfig.Field("data challenge version",str)
    dcDbName = pexConfig.Field("data challenge database name",str)
    minPercDiskSpaceReq = pexConfig.Field("minimum percent disk space required",int)
    userRunLife = pexConfig.Field("user run life",int)

class WorkflowDatabaseConfig(pexConfig.Config):
    dbName = pexConfig.Field("database name",str)

dbTypemap = {"production":ProductionDatabaseConfig,"workflow":WorkflowDatabaseConfig}

class DatabaseConfig(pexConfig.Config):
    name = pexConfig.Field("database name",str)
    system = pexConfig.ConfigField("database system info",DatabaseSystem)
    configurationClass = pexConfig.Field("database configuration class",str)
    configuration  = pexConfig.ConfigChoiceField("configuration",dbTypemap)
    logger = pexConfig.ConfigField("configuration dictionary",DBLogger)
