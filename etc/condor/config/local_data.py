#
# Example Orchestration Layer Config
#
root.production.shortName = "DataRelease"
root.production.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
root.production.repositoryDirectory = "/home/srp/working/pipeline_new"
root.production.productionShutdownTopic = "productionShutdown"

root.database["db1"].name = "dc3bGlobal"
root.database["db1"].system.authInfo.host = "fester.ncsa.uiuc.edu"
root.database["db1"].system.authInfo.port = 3306
root.database["db1"].system.runCleanup.daysFirstNotice = 7
root.database["db1"].system.runCleanup.daysFinalNotice = 1

root.database["db1"].configurationClass = "lsst.ctrl.orca.db.DC3Configurator"
root.database["db1"].configuration["production"].globalDbName = "GlobalDB"
root.database["db1"].configuration["production"].dcVersion = "S12_lsstsim"
root.database["db1"].configuration["production"].dcDbName = "DC3b_DB"
root.database["db1"].configuration["production"].minPercDiskSpaceReq = 10
root.database["db1"].configuration["production"].userRunLife = 2
root.database["db1"].logger.launch = True

root.workflow["association"].platform.dir.defaultRoot = "/lsst/DC3root"
root.workflow["association"].platform.dir.workDir = "work"
root.workflow["association"].platform.dir.inputDir = "input"
root.workflow["association"].platform.dir.outputDir = "output"
root.workflow["association"].platform.dir.updateDir = "update"
root.workflow["association"].platform.dir.scratchDir = "scratch"

root.workflow["association"].platform.deploy.defaultDomain = "ncsa.illinois.edu"

root.workflow["association"].shutdownTopic = "workflowShutdown"

root.workflow["association"].configurationType = "condor"
root.workflow["association"].configurationClass = "lsst.ctrl.orca.CondorWorkflowConfigurator"
root.workflow["association"].configuration["condor"].condorData.localScratch = "/home/srp/orca_scratch"


# where the script will be deposited in the localScratch

root.workflow["association"].task["isr"].scriptDir = "workers"
# this is used if we're running a script that runs LOCALLY before the
# preJob condor job is submitted.
# commented out for now
#root.workflow["association"].task["isr"].preScript.template = "$CTRL_ORCA_DIR/etc/condor/templates/preScript.template"
#root.workflow["association"].task["isr"].preScript.outputFile = "pre.sh"

#
# There are two stages of templating.  
# [preJob|postJob|workerJob].script.template is used to create
# [preJob|postJob|workerJob].script.output
# This "output" file is the executable script condor will run. 
#
# That file is substituted in the [preJob|postJob|workerJob].template 
# to create the [preJob|postJob|workerJob].outputFile, which is the
# actual Condor file used in the Condor DAG.


# 
# preJob
#
root.workflow["association"].task["isr"].preJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/preJob.sh.template"
root.workflow["association"].task["isr"].preJob.script.outputFile = "preJob.sh"
root.workflow["association"].task["isr"].preJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/preJob.condor.template"
root.workflow["association"].task["isr"].preJob.outputFile = "W2012Pipe.pre"

# 
# postJob
#
root.workflow["association"].task["isr"].postJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/postJob.sh.template"
root.workflow["association"].task["isr"].postJob.script.outputFile = "postJob.sh"
root.workflow["association"].task["isr"].postJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/postJob.condor.template"
root.workflow["association"].task["isr"].postJob.outputFile = "W2012Pipe.post"

# 
# workerJob
#
root.workflow["association"].task["isr"].workerJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/helloworld.sh.template"
root.workflow["association"].task["isr"].workerJob.script.outputFile = "helloworld.sh"
root.workflow["association"].task["isr"].workerJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/workerJob.condor.template"
root.workflow["association"].task["isr"].workerJob.outputFile = "W2012Pipeline-template.condor"

#
# This configures a diamond condor DAG.
# The "preJob" is the head of the diamond
# The contents dagGenerator.input are used in conjunction with the workerJob
# to form the middle of the diamond and the "postJob" is the tail of the 
# diamond
#
root.workflow["association"].task["isr"].dagGenerator.dagName = "W2012Pipe"
root.workflow["association"].task["isr"].dagGenerator.script = "$CTRL_ORCA_DIR/etc/condor/scripts/generateDag.py"
root.workflow["association"].task["isr"].dagGenerator.input = "$CTRL_ORCA_DIR/etc/condor/input/9429-CCDs.input"

