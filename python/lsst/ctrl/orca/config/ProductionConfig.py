import sys
import lsst.pex.config as pexConf
import FakeTypeMap as fake
import WorkflowConfig as work

class Production(pexConf.Config):
    shortName = pexConf.Field("name of production", str)
    repositoryDirectory = pexConf.Field("location of production repository", str)
    eventBrokerHost = pexConf.Field("hostname of the event broker",str)
    productionShutdownTopic = pexConf.Field("topic used to notify production shutdowns",str)


class AuthInfo(pexConf.Config):
    host = pexConf.Field("database host name",str)
    port = pexConf.Field("database port number",int)


class RunCleanup(pexConf.Config):
    daysFirstNotice = pexConf.Field("when to first give notice",int)
    daysFinalNotice = pexConf.Field("when to give final notice",int)

class DatabaseSystem(pexConf.Config):
    authInfo = pexConf.ConfigField("database authorization information",AuthInfo)
    runCleanup = pexConf.ConfigField("runCleanup ",RunCleanup)

class DBLogger(pexConf.Config):
    launch = pexConf.Field("launch",bool)

class Database(pexConf.Config):
    name = pexConf.Field("database name",str)
    system = pexConf.ConfigField("database system info",DatabaseSystem)
    configurationClass = pexConf.Field("configuration class",str)
    configurationDictionary = pexConf.Field("configuration dictionary",str)
    logger = pexConf.ConfigField("configuration dictionary",DBLogger)

class ProductionConfig(pexConf.Config):
    production = pexConf.ConfigField("production configuration",Production)
    database = pexConf.ConfigField("database information", Database)
    workflowNames = pexConf.ListField("workflow names",str)
    workflow = pexConf.ConfigChoiceField("workflow",fake.FakeTypeMap(work.WorkflowConfig))
    configCheckCare = pexConf.Field("config check care",int)
    configurationClass = pexConf.Field("configuration class",str)

config = ProductionConfig()
config.load(sys.argv[1])

print config
