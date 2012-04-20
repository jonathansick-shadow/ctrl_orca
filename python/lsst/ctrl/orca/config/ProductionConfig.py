import sys
import lsst.pex.config as pexConfig
import FakeTypeMap as fake
import WorkflowConfig as work
import DatabaseConfig as db

class ProductionLevelConfig(pexConfig.Config):
    configurationClass = pexConfig.Field("configuration class",str)

class Production(pexConfig.Config):
    shortName = pexConfig.Field("name of production", str)
    repositoryDirectory = pexConfig.Field("location of production repository", str)
    eventBrokerHost = pexConfig.Field("hostname of the event broker",str)
    productionShutdownTopic = pexConfig.Field("topic used to notify production shutdowns",str)
    configCheckCare = pexConfig.Field("config check",int, default=-1)
    logThreshold = pexConfig.Field("logging threshold",int)
    configuration = pexConfig.ConfigField("production level confg",ProductionLevelConfig)

class ProductionConfig(pexConfig.Config):
    production = pexConfig.ConfigField("production configuration",Production)
    databaseConfigNames = pexConfig.ListField("database configuration", str)
    database = pexConfig.ConfigChoiceField("database information", fake.FakeTypeMap(db.DatabaseConfig))
    workflowNames = pexConfig.ListField("workflow names",str)
    workflow = pexConfig.ConfigChoiceField("workflow",fake.FakeTypeMap(work.WorkflowConfig))
    configCheckCare = pexConfig.Field("config check care",int, default=-1)
    configurationClass = pexConfig.Field("configuration class",str)
