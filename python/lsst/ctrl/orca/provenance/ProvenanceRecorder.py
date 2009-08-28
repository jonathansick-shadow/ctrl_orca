import os
import sets
import lsst.pex.policy as pol
class ProvenanceRecorder:
    def __init__(self, repository):
        self.policySet = sets.Set()
        self.repository = repository
        return

    def configure(self, configurationDict):
        return

    def record(self, filename):
        # prov object init-ed here
        # prov.recordPolicy(filename)
        # pr = ProvenanceRecorder("/lsst/home/srp/temp_merge/ctrl_dc3pipe/branches/dc3b_proto/pipeline")
        # policySet = pr.extractPolicies("/lsst/home/srp/temp_merge/ctrl_dc3pipe/branches/dc3b_proto/pipeline/ap-cfht-nfs.paf")
        #for p in policySet:
        #    prov.recordPolicy(p)
        
        
        
    def extractPolicies(self, filename):
        policyObj = pol.Policy.createPolicy(filename, False)
        pipelinePolicySet = sets.Set()
        self.extractChildPolicies(self.repository, policyObj, pipelinePolicySet)
        return pipelinePolicySet

    def extractChildPolicies(self, repos, policy, pipelinePolicySet):
        names = policy.fileNames()
        for name in names:
            if name.rfind('.') > 0:
                desc = name[0:name.rfind('.')]
                field = name[name.rfind('.')+1:]
                policyObjs = policy.getPolicyArray(desc)
                print "policyObjs = ",policyObjs
                for policyObj in policyObjs:
                    self._extract(field, repos, policyObj, pipelinePolicySet)
            else:
                field = name
                self._extract(field, repos, policy, pipelinePolicySet)

        return

    def _extract(self, field, repos, policy, pipelinePolicySet):
        if policy.getValueType(field) == pol.Policy.FILE:
            filename = policy.getFile(field).getPath()
            filename = os.path.join(repos, filename)
            if (filename in self.policySet) == False:
                self.policySet.add(filename)
            if (filename in pipelinePolicySet) == False:
                pipelinePolicySet.add(filename)
            newPolicy = pol.Policy.createPolicy(filename, False)
            self.extractChildPolicies(repos, newPolicy, pipelinePolicySet)

pr = ProvenanceRecorder("/lsst/home/srp/temp_merge/ctrl_dc3pipe/branches/dc3b_proto/pipeline")
policySet = pr.extractPolicies("/lsst/home/srp/temp_merge/ctrl_dc3pipe/branches/dc3b_proto/pipeline/ap-cfht-nfs.paf")

for p in policySet:
    print p
