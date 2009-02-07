#!/usr/bin/env python

import eups
import hashlib
import os
import re
from lsst.pex.policy import Policy
from lsst.daf.persistence import DbStorage, LogicalLocation
from lsst.daf.base import DateTime

class Provenance:
    """Class to maintain provenance for LSST pipelines."""

    def __init__(self, username, runId, dbHost):
        """Initialize the Provenance controller."""
        self.username = username
        self.runId = runId
        self.policyFileId = 1
        self.policyKeyId = 1
        loc = LogicalLocation("mysql://%s:3306/provenance" % (dbHost))
        self.db = DbStorage()
        self.db.setPersistLocation(loc)

    def recordEnvironment(self):
        """Record the software environment of the pipeline."""

        self.db.startTransaction()
        
        id = 1
        setupList = eups.Eups().listProducts(setup=True)
        for pkg, ver, db, productDir, isCurrent, isSetup in setupList:
            self.db.setTableForInsert("prv_SoftwarePackage")
            self.db.setColumnString("runId", self.runId)
            self.db.setColumnInt("packageId", id)
            self.db.setColumnString("packageName", pkg)
            self.db.insertRow()

            self.db.setTableForInsert("prv_cnf_SoftwarePackage")
            self.db.setColumnString("runId", self.runId)
            self.db.setColumnInt("packageId", id)
            self.db.setColumnString("version", ver)
            self.db.setColumnString("directory", productDir)
            self.db.insertRow() 

            id += 1

        self.db.endTransaction()

    def recordPolicy(self, policyFile):
        """Record the contents of the given Policy as part of provenance."""
        md5 = hashlib.md5()
        f = open(policyFile, 'r')
        for line in f:
            md5.update(line)
        f.close()

        self.db.startTransaction()

        self.db.setTableForInsert("prv_PolicyFile")
        self.db.setColumnString("runId", self.runId)
        self.db.setColumnInt("policyFileId", self.policyFileId)
        self.db.setColumnString("pathname", policyFile)
        self.db.setColumnString("hashValue", md5.hexdigest())
        self.db.setColumnInt64("modifiedDate",
                DateTime(os.stat(policyFile)[8] * 1000000000L).nsecs())
        self.db.insertRow()

        p = Policy.createPolicy(policyFile)
        for key in p.paramNames():
            val = p.str(key) # works for arrays, too
            val = re.sub(r'\0', r'', val) # extra nulls get included

            self.db.setTableForInsert("prv_PolicyKey")
            self.db.setColumnString("runId", self.runId)
            self.db.setColumnInt("policyKeyId", self.policyKeyId)
            self.db.setColumnInt("policyFileId", self.policyFileId)
            self.db.setColumnString("keyName", key)
            self.db.setColumnString("keyType", p.getTypeName(key))
            self.db.insertRow()

            self.db.setTableForInsert("prv_cnf_PolicyKey")
            self.db.setColumnString("runId", self.runId)
            self.db.setColumnInt("policyKeyId", self.policyKeyId)
            self.db.setColumnString("value", val)
            self.db.insertRow()

            self.policyKeyId += 1

        self.policyFileId += 1

        self.db.endTransaction()
