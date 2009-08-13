from lsst.pex.logging import Log
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.PipelineManager import PipelineManager

class ProductionRunConfigurator:
    def __init__(self, runid, policy, logger, verbosity):
        self.logger = logger
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:__init__")
        self.runid = runid
        self.policy = policy
        self.verbosity = verbosity


    def createPipelineManager(self, shortName, pipelinePolicy, policyOverrides, pipelineVerbosity):
        # shortName - the short name for the pipeline to be configured
        # prodPolicy - the policy that describes this production run
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:createPipelineManager")

        #
        # we're given a pipelinePolicy, and things that need to be overridden
        #
        print "shortName = ",shortName

        #  read in default policy        
        #  read in given policy
        #  in given policy:
        #     set: execute.eventBrokerHost
        #     set: execute.dir
        #     set: execute.database.url
        #  write new policy file with overridden values        
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()        # 
        # copy the policies to the working directory
        polfile = os.path.join(self.repository, shortName+".paf")
            
        newPolicy = pol.Policy.createPolicy(polfile, False)
                
        if policyOverrides is not None:
            for name in policyOverrides.paramNames():
                newPolicy.set(name, policyOverrides.get(name))
                    
        executeDir = self.pipelinePolicy.get("platform.dir")
        newPolicy.set("execute.dir", executeDir)
                    
        newPolicy.set("execute.database.url",self.dbRunURL)
    

        polbasefile = os.path.basename(polfile)
        newPolicyFile = os.path.join(self.dirs.get("work"), shortName+".paf")       
        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN,
                       "Working directory already contains %s; won't overwrite" % \                                polbasefile)        else:
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(newPolicy)
            pw.close()
        
        # provenance really should be recorded here, since orca is supposed
        # to be in control of everything.
        #self.provenance.recordPolicy(newPolicyFile)
        
        pipelineManager = PipelineManager(newPolicyFile, self.logger, self.verbosity)
        return pipelineManager

    def configure(self):
        self.logger.log(Log.DEBUG, "ProductionRunConfigurator:configure")
        return []

