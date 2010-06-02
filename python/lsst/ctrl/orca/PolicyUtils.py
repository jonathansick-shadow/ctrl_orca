import os.path
import lsst.pex.policy as pol

class PolicyUtils(object):

    ##
    # @brief given a policy, recursively add all child policies to a policy set
    # 
    def getAllFilenames(repos, policy, policySet):
        names = policy.fileNames()
        for name in names:
            if name.rfind('.') > 0:
                desc = name[0:name.rfind('.')]
                field = name[name.rfind('.')+1:]
                policyObjs = policy.getPolicyArray(desc)
                for policyObj in policyObjs:
                    if policyObj.getValueType(field) == pol.Policy.FILE:
                        filename = policyObj.getFile(field).getPath()
                        filename = os.path.join(repos, filename)
                        if (filename in policySet) == False:
                            policySet.add(filename)
                        newPolicy = pol.Policy.createPolicy(filename, False)
                        PolicyUtils.getAllFilenames(repos, newPolicy, policySet)
            else:
                field = name
                if policy.getValueType(field) == pol.Policy.FILE:
                    filename = policy.getFile(field).getPath()
                    filename = os.path.join(repos, filename)
                    if (filename in policySet) == False:
                        policySet.add(filename)
                    newPolicy = pol.Policy.createPolicy(filename, False)
                    PolicyUtils.getAllFilenames(repos, newPolicy, policySet)
    getAllFilenames = staticmethod(getAllFilenames)
