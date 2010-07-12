# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import os.path
import lsst.pex.policy as pol

class PolicyUtils:

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
