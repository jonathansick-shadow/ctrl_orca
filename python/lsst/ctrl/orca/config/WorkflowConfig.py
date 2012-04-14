import sys
import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import VanillaCondorWorkflowConfig as van
import GenericWorkflowConfig as gen
import FakeTypeMap as fake

typemap = {"generic":gen.GenericWorkflowConfig,"vanilla":van.VanillaCondorWorkflowConfig}

class WorkflowConfig(pexConfig.Config):
    shortName = pexConfig.Field("name of this workflow",str)
    platform = pexConfig.Field("platform configuration file",str)
    shutdownTopic = pexConfig.Field("topic used for shutdown events",str)

    configurationClass = pexConfig.Field("orca plugin class",str)
    configuration  = pexConfig.ConfigChoiceField("configuration",typemap)

    pipelineNames = pexConfig.ListField("pipeline names",str)
    pipeline = pexConfig.ConfigChoiceField("pipeline",fake.FakeTypeMap(pipe.PipelineConfig))
