import sys
import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import VanillaCondorWorkflowConfig as van
import GenericWorkflowConfig as gen
import FakeTypeMap as fake
import DatabaseConfig as data
import PlatformConfig as plat

typemap = {"generic":gen.GenericWorkflowConfig,"vanilla":van.VanillaCondorWorkflowConfig}

class WorkflowConfig(pexConfig.Config):
    shortName = pexConfig.Field("name of this workflow",str)
    platform = pexConfig.ConfigField("platform configuration file",plat.PlatformConfig)
    shutdownTopic = pexConfig.Field("topic used for shutdown events",str)

    configurationClass = pexConfig.Field("orca plugin class",str)
    configuration  = pexConfig.ConfigChoiceField("configuration",typemap)

    #
    # these usually aren't used
    databaseNames = pexConfig.ListField("database names",str)
    database = pexConfig.ConfigChoiceField("database",fake.FakeTypeMap(data.DatabaseConfig))

    pipelineNames = pexConfig.ListField("pipeline names",str)
    pipeline = pexConfig.ConfigChoiceField("pipeline",fake.FakeTypeMap(pipe.PipelineConfig))
