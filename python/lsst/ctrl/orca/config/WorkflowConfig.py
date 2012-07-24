import sys
import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import CondorWorkflowConfig as condor
import VanillaCondorWorkflowConfig as van
import GenericWorkflowConfig as gen
import FakeTypeMap as fake
import DatabaseConfig as data
import PlatformConfig as plat
import TaskConfig as task

typemap = {"generic":gen.GenericWorkflowConfig,"vanilla":van.VanillaCondorWorkflowConfig, "condor":condor.CondorWorkflowConfig}

class WorkflowConfig(pexConfig.Config):
    shortName = pexConfig.Field("name of this workflow",str)
    platform = pexConfig.ConfigField("platform configuration file",plat.PlatformConfig)
    shutdownTopic = pexConfig.Field("topic used for shutdown events",str)

    configurationType = pexConfig.Field("plugin type",str)
    configurationClass = pexConfig.Field("orca plugin class",str)
    configuration  = pexConfig.ConfigChoiceField("configuration",typemap)

    # this usually isn't used, but is here because the design calls for this
    # possibility.
    database = pexConfig.ConfigChoiceField("database",fake.FakeTypeMap(data.DatabaseConfig))

    #pipeline = pexConfig.ConfigChoiceField("pipeline",fake.FakeTypeMap(pipe.PipelineConfig))
    task = pexConfig.ConfigChoiceField("task",fake.FakeTypeMap(task.TaskConfig))
