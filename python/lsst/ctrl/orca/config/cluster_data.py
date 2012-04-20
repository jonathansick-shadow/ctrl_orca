#
# Orchestration Layer Config for a DC3 production
#
root.production.shortName = "DataRelease"
root.production.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
root.production.repositoryDirectory = "/home/srp/working/pipeline_new"
root.production.productionShutdownTopic = "productionShutdown"

root.databaseConfigNames = ["db1"]
root.database["db1"].name = "dc3bGlobal"
root.database["db1"].system.authInfo.host = "fester.ncsa.uiuc.edu"
root.database["db1"].system.authInfo.port = 3306
root.database["db1"].system.runCleanup.daysFirstNotice = 7
root.database["db1"].system.runCleanup.daysFinalNotice = 1

root.database["db1"].configurationClass = "lsst.ctrl.orca.db.DC3Configurator"
root.database["db1"].configuration["production"].globalDbName = "GlobalDB"
root.database["db1"].configuration["production"].dcVersion = "PT1_2"
root.database["db1"].configuration["production"].dcDbName = "DC3b_DB"
root.database["db1"].configuration["production"].minPercDiskSpaceReq = 10
root.database["db1"].configuration["production"].userRunLife = 2
root.database["db1"].logger.launch = True

root.workflowNames = ["association"]

root.workflow["association"].platform.dir.defaultRoot = "/lsst/DC3root"
root.workflow["association"].platform.dir.runDirPattern = "%(runid)s"
root.workflow["association"].platform.dir.workDir = "work"
root.workflow["association"].platform.dir.inputDir = "input"
root.workflow["association"].platform.dir.outputDir = "output"
root.workflow["association"].platform.dir.updateDir = "update"
root.workflow["association"].platform.dir.scratchDir = "scratch"

root.workflow["association"].platform.hw.nodeCount = 1
root.workflow["association"].platform.hw.minCoresPerNode = 2
root.workflow["association"].platform.hw.maxCoresPerNode = 8
root.workflow["association"].platform.hw.minRamPerNode = 2.0
root.workflow["association"].platform.hw.maxRamPerNode = 16.0

root.workflow["association"].platform.deploy.defaultDomain = "ncsa.uiuc.edu"
root.workflow["association"].platform.deploy.nodes = "lsst10:2"

root.workflow["association"].shutdownTopic = "workflowShutdown"

root.workflow["association"].configurationClass = "lsst.ctrl.orca.GenericPipelineWorkflowConfigurator"
root.workflow["association"].configuration["generic"].deployData.dataRepository = "/lsst/DC3/data/obstest"
root.workflow["association"].configuration["generic"].deployData.collection = "CFHTLS/D2"
root.workflow["association"].configuration["generic"].deployData.script = "/home/srp/working/pipeline_new/deployData.sh"

root.workflow["association"].configuration["generic"].announceData.script = "$CTRL_SCHED_DIR/bin/announceDataset.py"
root.workflow["association"].configuration["generic"].announceData.topic = "RawCcdAvailable"
root.workflow["association"].configuration["generic"].announceData.inputdata = "/home/srp/working/pipeline_new/ISR/cfht-isr-inputdata-short.txt"
root.workflow["association"].configuration["generic"].announceData.dataCompleted.script = "$CTRL_ORCA_DIR/bin/sendStatus.py"
root.workflow["association"].configuration["generic"].announceData.dataCompleted.topic = "JobOfficeTopic"
root.workflow["association"].configuration["generic"].announceData.dataCompleted.status = "data:completed"

root.workflow["association"].pipelineNames = ["joboffices","isr","ccdAssembly"]

root.workflow["association"].pipeline["joboffices"].framework.script = "$DATAREL_DIR/etc/launchjoboffice.sh"
root.workflow["association"].pipeline["joboffices"].framework.type = "standard"
root.workflow["association"].pipeline["joboffices"].framework.environment = "$DATAREL_DIR/etc/setup.sh"
root.workflow["association"].pipeline["joboffices"].execute.shutdownTopic = "workflowShutdown"
root.workflow["association"].pipeline["joboffices"].execute.eventBrokerHost = "lsst8.ncsa.uiuc.edu"

root.workflow["association"].pipeline["joboffices"].deploy.processesOnNode = ["lsst9.ncsa.illinois.edu:1"]
root.workflow["association"].pipeline["joboffices"].runCount = 1
root.workflow["association"].pipeline["joboffices"].launch = True

root.workflow["association"].pipeline["isr"].framework.type = "standard"
root.workflow["association"].pipeline["isr"].framework.script = "$DATAREL_DIR/bin/launchPipeline.py"
root.workflow["association"].pipeline["isr"].framework.environment = "$DATAREL_DIR/etc/setup.sh"
root.workflow["association"].pipeline["isr"].execute.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
root.workflow["association"].pipeline["isr"].execute.shutdownTopic = "shutdownISR"

root.workflow["association"].pipeline["isr"].dir.shortName = "ISR"
root.workflow["association"].pipeline["isr"].dir.defaultRoot = "/lsst/DC3root"
root.workflow["association"].pipeline["isr"].dir.runDirPattern = "%(runid)s"
root.workflow["association"].pipeline["isr"].dir.workDir = "work"
root.workflow["association"].pipeline["isr"].dir.inputDir = "input"
root.workflow["association"].pipeline["isr"].dir.outputDir = "output"
root.workflow["association"].pipeline["isr"].dir.updateDir = "update"
root.workflow["association"].pipeline["isr"].dir.scratchDir = "scratch"

root.workflow["association"].pipeline["isr"].appStageNames = ["000-getajob", "010-cfht-input", "040-saturation", "050-overscan", "060-bias", "080-flat", "100-cfht-exposureOutput", "130-jobDone", "140-failure"]
root.workflow["association"].pipeline["isr"].appStage["000-getajob"].parallelClass = "lsst.ctrl.sched.pipeline.GetAJobParallelProcessing"
root.workflow["association"].pipeline["isr"].appStage["000-getajob"].eventTopic = "None"
root.workflow["association"].pipeline["isr"].appStage["000-getajob"].stageConfig = "GetAJob.py"

root.workflow["association"].pipeline["isr"].appStage["010-cfht-input"].parallelClass = "lsst.pex.harness.IOStage.InputStageParallel"
root.workflow["association"].pipeline["isr"].appStage["010-cfht-input"].eventTopic = "None"
root.workflow["association"].pipeline["isr"].appStage["010-cfht-input"].stageConfig = "CFHTInput.py"

root.workflow["association"].pipeline["isr"].appStage["040-saturation"].parallelClass = "lsst.ip.pipeline.IsrSaturationStageParllel"
root.workflow["association"].pipeline["isr"].appStage["040-saturation"].eventTopic = "None"
root.workflow["association"].pipeline["isr"].appStage["040-saturation"].stageConfig = "Saturation.py"

root.workflow["association"].pipeline["isr"].appStage["050-overscan"].parallelClass = "lsst.ip.pipeline.IsrOverscanStageParllel"
root.workflow["association"].pipeline["isr"].appStage["050-overscan"].eventTopic = "None"
root.workflow["association"].pipeline["isr"].appStage["050-overscan"].stageConfig = "Overscan.py"

root.workflow["association"].pipeline["isr"].appStage["060-bias"].parallelClass = "lsst.ip.pipeline.IsrBiasStageParllel"
root.workflow["association"].pipeline["isr"].appStage["060-bias"].eventTopic = "None"
root.workflow["association"].pipeline["isr"].appStage["060-bias"].stageConfig = "Bias.py"

root.workflow["association"].pipeline["isr"].appStage["080-flat"].parallelClass = "lsst.ip.pipeline.IsrFlatStageParllel"
root.workflow["association"].pipeline["isr"].appStage["080-flat"].eventTopic = "None"
root.workflow["association"].pipeline["isr"].appStage["080-flat"].stageConfig = "Flat.py"

root.workflow["association"].pipeline["isr"].appStage["100-cfht-exposureOutput"].parallelClass = "lsst.pex.harness.IOStage.OutputStageParallel"
root.workflow["association"].pipeline["isr"].appStage["100-cfht-exposureOutput"].eventTopic = "None"
root.workflow["association"].pipeline["isr"].appStage["100-cfht-exposureOutput"].stageConfig = "CFHTExposureOuput.py"

root.workflow["association"].pipeline["ca"].appStage["130-jobDone"].parallelClass = "lsst.ctrl.sched.pipeline.JobDoneParallelProcessing"
root.workflow["association"].pipeline["ca"].appStage["130-jobDone"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["130-jobDone"].stageConfig = "JobDone.py"

root.workflow["association"].pipeline["ca"].appStage["140-failure"].parallelClass = "lsst.ctrl.sched.pipeline.JobDoneParallelProcessing"
root.workflow["association"].pipeline["ca"].appStage["140-failure"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["140-failure"].stageConfig = "Failure.py"

########################
root.workflow["association"].pipeline["ca"].framework.type = "standard"
root.workflow["association"].pipeline["ca"].framework.script = "$DATAREL_DIR/bin/launchPipeline.py"
root.workflow["association"].pipeline["ca"].framework.environment = "$DATAREL_DIR/etc/setup.sh"
root.workflow["association"].pipeline["ca"].execute.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
root.workflow["association"].pipeline["ca"].execute.shutdownTopic = "shutdownCA"

root.workflow["association"].pipeline["ca"].deploy.processesOnNode = ["lsst14.ncsa.illinois.edu:1"]
root.workflow["association"].pipeline["ca"].runCount = 4
root.workflow["association"].pipeline["ca"].launch = True

root.workflow["association"].pipeline["ca"].dir.shortName = "CA"
root.workflow["association"].pipeline["ca"].dir.defaultRoot = "."
root.workflow["association"].pipeline["ca"].dir.runDirPattern = "../%(runid)s/$(shortname)s"
root.workflow["association"].pipeline["ca"].dir.workDir = "work"
root.workflow["association"].pipeline["ca"].dir.inputDir = "input"
root.workflow["association"].pipeline["ca"].dir.outputDir = "output"
root.workflow["association"].pipeline["ca"].dir.updateDir = "update"
root.workflow["association"].pipeline["ca"].dir.scratchDir = "scratch"

root.workflow["association"].pipeline["ca"].appStageNames = ["000-getajob", "010-cfht-input", "020-assembleCcd", "030-isrCcdDefect", "040-cfht-output", "050-jobDone", "060-failure"]

root.workflow["association"].pipeline["ca"].appStage["000-getajob"].parallelClass = "lsst.ctrl.sched.pipeline.GetAJobParallelProcessing"
root.workflow["association"].pipeline["ca"].appStage["000-getajob"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["000-getajob"].stageConfig = "GetAJob.py"

root.workflow["association"].pipeline["ca"].appStage["010-cfht-input"].parallelClass = "lsst.pex.harness.IOStage.InputStageParallel"
root.workflow["association"].pipeline["ca"].appStage["010-cfht-input"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["010-cfht-input"].stageConfig = "CFHTInput.py"

root.workflow["association"].pipeline["ca"].appStage["020-assembleCcd"].parallelClass = "lsst.ip.pipeline.IsrCcdAssemblyStageParllel"
root.workflow["association"].pipeline["ca"].appStage["020-assembleCcd"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["020-assembleCcd"].stageConfig = "AssembleCcd.py"

root.workflow["association"].pipeline["ca"].appStage["030-isrCcdDefect"].parallelClass = "lsst.ip.pipeline.IsrCcdDefectStageParallel"
root.workflow["association"].pipeline["ca"].appStage["030-isrCcdDefect"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["030-isrCcdDefect"].stageConfig = "IsrCcdDefect.py"

root.workflow["association"].pipeline["ca"].appStage["040-cfht-output"].parallelClass = "lsst.pex.harness.IOStage.OutputStageParallel"
root.workflow["association"].pipeline["ca"].appStage["040-cfht-output"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["040-cfht-output"].stageConfig = "CFHTOutput.py"

root.workflow["association"].pipeline["ca"].appStage["050-jobDone"].parallelClass = "lsst.ctrl.sched.pipeline.JobDoneParallelProcessing"
root.workflow["association"].pipeline["ca"].appStage["050-jobDone"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["050-jobDone"].stageConfig = "JobDone.py"

root.workflow["association"].pipeline["ca"].appStage["060-failure"].parallelClass = "lsst.ctrl.sched.pipeline.JobDoneParallelProcessing"
root.workflow["association"].pipeline["ca"].appStage["060-failure"].eventTopic = "None"
root.workflow["association"].pipeline["ca"].appStage["060-failure"].stageConfig = "Failure.py"





workflow: {

    # 
    # database: {
    # 
    # }

    pipeline: {
       shortName:     isr
       definition:    @cfht-isr-master.paf
       runCount: 4
       deploy: {
          # the schema for this node is determined by the above 
          # configuration class.  In this case, we can assign exactly the
          # number of processes to run on each node.  The 
          # 

          # an assignment of processes to nodes.  The first entry will 
          # be host the Pipeline process.  Note that while the name 
          # is the same as in the IPSD workflow, the value is interpreted
          # differently by virtue of the fact that different 
          # WorkflowConfigurator class is used.  
          # 
          processesOnNode: lsst14.ncsa.illinois.edu:1
          
       }
       launch: true
    }
    pipeline: {
       shortName:     ccdassembly
       definition:    @cfht-ca-master.paf
       runCount: 2
       deploy: {
          # the schema for this node is determined by the above 
          # configuration class.  In this case, we can assign exactly the
          # number of processes to run on each node.  The 
          # 

          # an assignment of processes to nodes.  The first entry will 
          # be host the Pipeline process.  Note that while the name 
          # is the same as in the IPSD workflow, the value is interpreted
          # differently by virtue of the fact that different 
          # WorkflowConfigurator class is used.  
          # 
          processesOnNode: lsst15.ncsa.illinois.edu:1
          
       }
       launch: true
    }
}
