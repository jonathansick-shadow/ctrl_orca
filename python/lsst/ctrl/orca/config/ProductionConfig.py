import sys
import lsst.pex.config as pexConf
import VanillaCondorWorkflowConfig as van
import GenericWorkflowConfig as gen

typemap = {"generic":gen.GenericWorkflowConfig,"vanilla":van.VanillaCondorWorkflowConfig}

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
    workflow = pexConf.ConfigChoiceField("workflow",typemap)
    configCheckCare = pexConf.Field("config check care",int)
    configurationClass = pexConf.Field("configuration class",str)

config = ProductionConfig()
config.load(sys.argv[1])

print config.production
print config.database.name
print config.database.system.authInfo
print config.database.system.runCleanup
print config.database.configurationClass
print config.database.configurationDictionary
print config.database.logger
print config.workflow.type
print config.workflow[config.workflow.type]
print config.workflow[config.workflow.type].configuration.deployData
print config.workflow[config.workflow.type].configuration.condorData
print config.workflow[config.workflow.type].configuration.glideinRequest
print config.workflow[config.workflow.type].configuration.announceData

print config.workflow[config.workflow.type].pipelineNames
for i in config.workflow[config.workflow.type].pipelineNames:
    obj = config.workflow[config.workflow.type].pipeline
    print obj[i]
