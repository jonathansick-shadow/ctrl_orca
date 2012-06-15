#
# Example Orchestration Layer Config
#
root.production.shortName = "DataRelease"
root.production.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
root.production.repositoryDirectory = "/home/srp/working/pipeline_new"
root.production.productionShutdownTopic = "productionShutdown"

root.database["db1"].name = "dc3bGlobal"
root.database["db1"].system.authInfo.host = "lsst10.ncsa.uiuc.edu"
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

root.workflow["workflow1"].platform.dir.defaultRoot = "/lsst/DC3root"

root.workflow["workflow1"].platform.deploy.defaultDomain = "ncsa.illinois.edu"

root.workflow["workflow1"].configurationType = "condor"
root.workflow["workflow1"].configurationClass = "lsst.ctrl.orca.CondorWorkflowConfigurator"
root.workflow["workflow1"].configuration["condor"].condorData.localScratch = "/home/srp/orca_scratch"
root.workflow["workflow1"].configuration["condor"].glidein.template.inputFile = "$CTRL_ORCA_DIR/etc/condor/templates/lonestar_glidein.template"
root.workflow["workflow1"].configuration["condor"].glidein.template.keywords["CONDOR_SBIN"] = "/share1/apps/teragrid/condor-7.4.4-r1/sbin"
root.workflow["workflow1"].configuration["condor"].glidein.template.keywords["CPU_COUNT"] = "6"
root.workflow["workflow1"].configuration["condor"].glidein.template.keywords["MACHINE_COUNT"] = "2"
root.workflow["workflow1"].configuration["condor"].glidein.template.keywords["MAX_WALLTIME"] = "30"
root.workflow["workflow1"].configuration["condor"].glidein.template.keywords["QUEUE"] = "normal"
root.workflow["workflow1"].configuration["condor"].glidein.template.keywords["PROJECT"] = "TG-AST100018"
root.workflow["workflow1"].configuration["condor"].glidein.template.outputFile = "lonestar_glidein.condor"


# where the script will be deposited in the localScratch

root.workflow["workflow1"].task["task1"].scriptDir = "workers"
# this is used if we're running a script that runs LOCALLY before the
# preJob condor job is submitted.
#root.workflow["workflow1"].task["task1"].preScript.template = "$CTRL_ORCA_DIR/etc/condor/templates/preScript.template"
#root.workflow["workflow1"].task["task1"].preScript.outputFile = "pre.sh"

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
root.workflow["workflow1"].task["task1"].preJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/preJob.sh.template"
root.workflow["workflow1"].task["task1"].preJob.script.outputFile = "preJob.sh"
root.workflow["workflow1"].task["task1"].preJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/preJob.condor.template"
root.workflow["workflow1"].task["task1"].preJob.outputFile = "W2012Pipe.pre"

# 
# postJob
#
root.workflow["workflow1"].task["task1"].postJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/postJob.sh.template"
root.workflow["workflow1"].task["task1"].postJob.script.outputFile = "postJob.sh"
root.workflow["workflow1"].task["task1"].postJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/postJob.condor.template"
root.workflow["workflow1"].task["task1"].postJob.outputFile = "W2012Pipe.post"

# 
# workerJob
#
root.workflow["workflow1"].task["task1"].workerJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/helloworld.sh.template"
root.workflow["workflow1"].task["task1"].workerJob.script.outputFile = "helloworld.sh"
root.workflow["workflow1"].task["task1"].workerJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/workerJob.condor.template"
root.workflow["workflow1"].task["task1"].workerJob.outputFile = "W2012Pipeline-template.condor"

#
# This configures a diamond condor DAG.
# The "preJob" is the head of the diamond
# The contents dagGenerator.input are used in conjunction with the workerJob
# to form the middle of the diamond and the "postJob" is the tail of the 
# diamond
#
root.workflow["workflow1"].task["task1"].dagGenerator.dagName = "W2012Pipe"
root.workflow["workflow1"].task["task1"].dagGenerator.script = "$CTRL_ORCA_DIR/etc/condor/scripts/generateDag.py"
root.workflow["workflow1"].task["task1"].dagGenerator.input = "$CTRL_ORCA_DIR/etc/condor/input/short.input"

