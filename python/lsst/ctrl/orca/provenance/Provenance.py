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

    def __init__(self, username, runId, dbLoc, globalDbLoc):
        """Initialize the Provenance controller."""
        self.username = username
        self.runId = runId

        loc = LogicalLocation(dbLoc)
        self.db = DbStorage()
        self.db.setPersistLocation(loc)

        globalLoc = LogicalLocation(globalDbLoc)
        self.globalDb = DbStorage()

        self.globalDb.setPersistLocation(globalLoc)

        self.globalDb.startTransaction()
        self.globalDb.setTableForInsert("prv_Run")
        self.globalDb.setColumnString("runId", runId)
        self.globalDb.insertRow()
        self.globalDb.endTransaction()

        self.globalDb.setRetrieveLocation(globalLoc)

        self.globalDb.startTransaction()
        self.globalDb.setTableForQuery("prv_Run")
        self.globalDb.outColumn("offset")
        self.globalDb.condParamString("runId", runId)
        self.globalDb.setQueryWhere("runId = :runId")
        self.globalDb.query()
        if not self.globalDb.next() or self.globalDb.columnIsNull(0):
            raise pex.exceptions.LsstException("runId not found")
        self.offset = self.globalDb.getColumnByPosInt(0) * 65536
        self.globalDb.finishQuery()
        self.globalDb.endTransaction()

        self.globalDb.setPersistLocation(globalLoc)

        self.policyFileId = self.offset + 1
        self.policyKeyId = self.offset + 1

    def recordEnvironment(self):
        """Record the software environment of the pipeline."""

        setupList = eups.Eups().listProducts(setup=True)
        self._realRecordEnvironment(self.db, setupList)
        self._realRecordEnvironment(self.globalDb, setupList)

    def _realRecordEnvironment(self, db, setupList):
        db.startTransaction()
        
        id = self.offset + 1
        for pkg, ver, eupsDb, productDir, isCurrent, isSetup in setupList:
            db.setTableForInsert("prv_SoftwarePackage")
            db.setColumnInt("packageId", id)
            db.setColumnString("packageName", pkg)
            db.insertRow()

            db.setTableForInsert("prv_cnf_SoftwarePackage")
            db.setColumnInt("packageId", id)
            db.setColumnString("version", ver)
            db.setColumnString("directory", productDir)
            db.insertRow() 

            id += 1

        db.endTransaction()

    def recordPolicy(self, policyFile):
        """Record the contents of the given Policy as part of provenance."""

        md5 = hashlib.md5()
        f = open(policyFile, 'r')
        for line in f:
            md5.update(line)
        f.close()

        self._realRecordPolicyFile(self.db, policyFile, md5)
        self._realRecordPolicyFile(self.globalDb, policyFile, md5)

        p = Policy.createPolicy(policyFile)
        for key in p.paramNames():
            type = p.getTypeName(key)
            val = p.str(key) # works for arrays, too
            val = re.sub(r'\0', r'', val) # extra nulls get included
            self._realRecordPolicyContents(self.db, key, type, val)
            self._realRecordPolicyContents(self.globalDb, key, type, val)

            self.policyKeyId += 1

        self.policyFileId += 1

    def _realRecordPolicyFile(self, db, file, md5):
        db.startTransaction()

        db.setTableForInsert("prv_PolicyFile")
        db.setColumnInt("policyFileId", self.policyFileId)
        db.setColumnString("pathname", file)
        db.setColumnString("hashValue", md5.hexdigest())
        db.setColumnInt64("modifiedDate",
                DateTime(os.stat(file)[8] * 1000000000L, DateTime.UTC).nsecs())
        db.insertRow()

        db.endTransaction()

    def _realRecordPolicyContents(self, db, key, type, val):
        db.startTransaction()
        db.setTableForInsert("prv_PolicyKey")
        db.setColumnInt("policyKeyId", self.policyKeyId)
        db.setColumnInt("policyFileId", self.policyFileId)
        db.setColumnString("keyName", key)
        db.setColumnString("keyType", type)
        db.insertRow()

        db.setTableForInsert("prv_cnf_PolicyKey")
        db.setColumnInt("policyKeyId", self.policyKeyId)
        db.setColumnString("value", val)
        db.insertRow()

        db.endTransaction()
