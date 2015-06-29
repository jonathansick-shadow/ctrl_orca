root.production.shortName = "DataRelease"
root.production.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
root.production.repositoryDirectory = "./pipeline"
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

root.workflowNames = ["PT1Workflow"]

root.workflow["PT1Workflow"].platform.dir.defaultRoot = "/scratch/00482/srp/datarel-runs"
root.workflow["PT1Workflow"].platform.dir.runDirPattern = "%(runid)s"
root.workflow["PT1Workflow"].platform.dir.workDir = "work"
root.workflow["PT1Workflow"].platform.dir.inputDir = "input"
root.workflow["PT1Workflow"].platform.dir.outputDir = "output"
root.workflow["PT1Workflow"].platform.dir.updateDir = "update"
root.workflow["PT1Workflow"].platform.dir.scratchDir = "scratch"

root.workflow["PT1Workflow"].platform.hw.nodeCount = 1000
root.workflow["PT1Workflow"].platform.hw.minCoresPerNode = 12
root.workflow["PT1Workflow"].platform.hw.maxCoresPerNode = 12
root.workflow["PT1Workflow"].platform.hw.minRamPerNode = 32.0
root.workflow["PT1Workflow"].platform.hw.maxRamPerNode = 32.0

root.workflow["PT1Workflow"].platform.deploy.defaultDomain = "tacc.utexas.edu"
root.workflow["PT1Workflow"].platform.deploy.nodes = ["remoteabe1:8", "remoteabe2:8", "remoteabe3:8", "remoteabe4:8", "remoteabe5:8", "remoteabe6:8", "remoteabe7:8", "remoteabe8:8"];

root.workflow["PT1Workflow"].shutdownTopic = "workflowShutdown"

root.workflow["PT1Workflow"].configurationClass = "lsst.ctrl.orca.VanillaCondorWorkflowConfigurator"
root.workflow["PT1Workflow"].configuration["vanilla"].deployData.dataRepository = "/scratch/00342/ux453102/base/lsst/pt1_2-410/data/obs"
root.workflow["PT1Workflow"].configuration["vanilla"].deployData.collection = "ImSim"
root.workflow["PT1Workflow"].configuration["vanilla"].deployData.script = "$DATAREL_DIR/bin/runOrca/deployData-abe.sh"

root.workflow["PT1Workflow"].configuration["vanilla"].condorData.localScratch = "/home/daues/orca_scratch"
root.workflow["PT1Workflow"].configuration["vanilla"].condorData.loginNode = "login1.ls4.tacc.utexas.edu"
root.workflow["PT1Workflow"].configuration["vanilla"].condorData.ftpNode = "gridftp1.ls4.tacc.utexas.edu"
root.workflow["PT1Workflow"].configuration["vanilla"].condorData.transferProtocol = "gsiftp"
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyNames = ["CONDOR_SBIN", "CPU_COUNT", "MACHINE_COUNT", "QUEUE", "MAX_WALLTIME", "PROJECT"]
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["CONDOR_SBIN"] = "/share/apps/teragrid/condor-7.4.4.-r1/sbin"
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["CPU_COUNT"] = "6" 
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["MACHINE_COUNT"] = "64"
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["QUEUE"] = "normal"
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["MAX_WALLTIME"] = "180"
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["PROJECT"] = "TG-AST100018"
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.templateFileName = "$CTRL_ORCA_DIR/etc/lonestar/glidein_run.submit.dc3b"
root.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.outputFileName = "glidein_request.condor"
root.workflow["PT1Workflow"].configuration["vanilla"].announceData.script = "$CTRL_SCHED_DIR/bin/announceDataset.py"
root.workflow["PT1Workflow"].configuration["vanilla"].announceData.topic = "RawCcdAvailable"
root.workflow["PT1Workflow"].configuration["vanilla"].announceData.inputdata = "/nfs/lsst3/remote/Wint-2012/pipeline/miniprod.input"
root.workflow["PT1Workflow"].configuration["vanilla"].announceData.dataCompleted.script = "$CTRL_ORCA_DIR/bin/sendStatus.py"
root.workflow["PT1Workflow"].configuration["vanilla"].announceData.dataCompleted.topic = "JobOfficeTopic"
root.workflow["PT1Workflow"].configuration["vanilla"].announceData.dataCompleted.status = "data:completed"




root.workflow["PT1Workflow"].pipelineNames = ["joboffices","PT1Pipe"]

root.workflow["PT1Workflow"].pipeline["joboffices"].definition.execute.shutdownTopic = "workflowShutdown"
root.workflow["PT1Workflow"].pipeline["joboffices"].definition.execute.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
root.workflow["PT1Workflow"].pipeline["joboffices"].definition.framework.script = "$DATAREL_DIR/pipeline/PT1Pipe/joboffice-ImSim-abe.sh"
root.workflow["PT1Workflow"].pipeline["joboffices"].definition.framework.type = "standard"
root.workflow["PT1Workflow"].pipeline["joboffices"].definition.framework.environment = "$DATAREL_DIR/bin/runOrca/imsim-setupForOrcaUse-abe.sh"
root.workflow["PT1Workflow"].pipeline["joboffices"].runCount = 1
root.workflow["PT1Workflow"].pipeline["joboffices"].launch = True

#root.workflow["PT1Workflow"].pipeline["PT1Pipe"].definition = "@main-ImSim.paf"
#root.workflow["PT1Workflow"].pipeline["PT1Pipe"].runCount = 1
#root.workflow["PT1Workflow"].pipeline["PT1Pipe"].launch = True
