#!/usr/bin/env python
import os, os.path, getopt, sets, sys
import lsst.pex.policy as pol
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory

class Recorder:
    def __init__(self, provenance, repository):
        self.policySet = sets.Set()
        self.provenance = provenance
        self.repository = repository
        return

    def record(self, name):
        # prov object init-ed here
        filename = os.path.join(self.repository,name) 
        provenance.recordPolicy(filename)
        policyNamesSet = self._extractPolicies(filename)
        for p in policyNamesSet:
            provenance.recordPolicy(p)

    def _extractPolicies(self, filename):
        policyObj = pol.Policy.createPolicy(filename, False)
        pipelinePolicySet = sets.Set()
        self._extractChildPolicies(self.repository, policyObj, pipelinePolicySet)
        return pipelinePolicySet

    def _extractChildPolicies(self, repos, policy, pipelinePolicySet):
        names = policy.fileNames()
        for name in names:
            if name.rfind('.') > 0:
                desc = name[0:name.rfind('.')]
                field = name[name.rfind('.')+1:]
                policyObjs = policy.getPolicyArray(desc)
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
            self._extractChildPolicies(repos, newPolicy, pipelinePolicySet)

if __name__ == "__main__":
    arguments = "--type=<type> --runid=<runid> --user=<user>, --dbrun=<dbrun> --dbglobal=<dbglobal> --filename=<file> --repos=<repos>"
    options, xarguments = getopt.getopt(sys.argv[1:], "h", ["type=", "runid=", "user=", "dbrun=", "dbglobal=", "filename=", "repos="])


    dict = {}
    for a,o in options:
        print a,o
        dict[a.lstrip('-')] = o

    print "finally:"
    print dict
    
    classFactory = NamedClassFactory()
    provClass = classFactory.createClass(dict["type"])
    provenance = provClass(dict)


    recorder = Recorder(provenance,dict["repos"])
    recorder.record(dict["filename"])
