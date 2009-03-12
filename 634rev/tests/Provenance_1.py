#!/usr/bin/env python

import eups
import hashlib
import os
import re
import time
import unittest

import lsst.ctrl.orca.provenance as orcaProv
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import DbStorage, LogicalLocation

runId = "test_" + repr(time.time())

class ProvenanceTestCase(unittest.TestCase):
    """A test case for Provenance."""

    def setUp(self):
        global runId
        self.user = "test"
        self.runId = runId
        self.dbLoc = "mysql://lsst10.ncsa.uiuc.edu:3306/provenance"
        self.globalDbLoc = "mysql://lsst10.ncsa.uiuc.edu:3306/provglobal"
        db = DbStorage()
        db.setPersistLocation(LogicalLocation(self.dbLoc))
        for table in ("prv_SoftwarePackage", "prv_cnf_SoftwarePackage",
                "prv_PolicyFile", "prv_PolicyKey", "prv_cnf_PolicyKey"):
            db.truncateTable(table)

    def testConstruct(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.dbLoc,
                self.globalDbLoc)
        self.assert_(ps is not None)
        self.assert_(ps.db is not None)

    def testEnvironment(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.dbLoc,
                self.globalDbLoc)
        ps.recordEnvironment()

        db = DbStorage()
        db.setRetrieveLocation(LogicalLocation(self.dbLoc))

        db.startTransaction()

        db.setTableListForQuery(
                ("prv_SoftwarePackage", "prv_cnf_SoftwarePackage"))
        db.outColumn("prv_SoftwarePackage.packageId")
        db.outColumn("packageName")
        db.outColumn("version")
        db.outColumn("directory")
        db.orderBy("packageName")
        db.setQueryWhere("prv_SoftwarePackage.packageId = prv_cnf_SoftwarePackage.packageId")
        db.query()

        i = 1
        pkgs = eups.Eups().listProducts(setup=True)
        while db.next():
            self.assert_(not db.columnIsNull(0))
            self.assert_(not db.columnIsNull(1))
            self.assert_(not db.columnIsNull(2))
            self.assert_(not db.columnIsNull(3))
            self.assertEqual(db.getColumnByPosInt(0), i)
            self.assertEqual(db.getColumnByPosString(1), pkgs[i - 1][0])
            self.assertEqual(db.getColumnByPosString(2), pkgs[i - 1][1])
            self.assertEqual(db.getColumnByPosString(3), pkgs[i - 1][3])
            i += 1
        db.finishQuery()

        db.endTransaction()

    def testPolicies(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.dbLoc,
                self.globalDbLoc)
        paths = ("tests/policy/dc2pipe.paf",
                "tests/policy/imageSubtractionDetection.paf")
        for p in paths:
            ps.recordPolicy(p)

        db = DbStorage()
        db.setRetrieveLocation(LogicalLocation(self.dbLoc))

        for p in paths:
            md5 = hashlib.md5()
            f = open(p, 'r')
            for line in f:
                md5.update(line)
            f.close()
            hash = md5.hexdigest()
            mod = os.stat(p)[8] * 1000000000L

            pol = pexPolicy.Policy(p)
            names = pol.paramNames()

            db.startTransaction()

            db.setTableListForQuery(
                    ("prv_PolicyFile", "prv_PolicyKey", "prv_cnf_PolicyKey"))
            db.outColumn("hashValue")
            db.outColumn("modifiedDate")
            db.outColumn("keyName")
            db.outColumn("keyType")
            db.outColumn("value")
            db.setQueryWhere(
                """pathname = '%s'
                AND prv_PolicyFile.policyFileId = prv_PolicyKey.policyFileId
                AND prv_PolicyKey.policyKeyId = prv_cnf_PolicyKey.policyKeyId
                """ % (p))
            db.query()

            while db.next():
                self.assert_(not db.columnIsNull(0))
                self.assert_(not db.columnIsNull(1))
                self.assert_(not db.columnIsNull(2))
                self.assert_(not db.columnIsNull(3))
                self.assert_(not db.columnIsNull(4))
                self.assertEqual(db.getColumnByPosString(0), hash)
                self.assertEqual(db.getColumnByPosInt64(1), mod)
                key = db.getColumnByPosString(2)
                self.assert_(pol.exists(key))
                self.assert_(key in names)
                names.remove(key)
                self.assertEqual(db.getColumnByPosString(3),
                        pol.getTypeName(key))
                correct = pol.str(key)
                correct = re.sub(r'\0', r'', correct)
                self.assertEqual(db.getColumnByPosString(4), correct)

            db.finishQuery()

            db.endTransaction()

            self.assertEqual(len(names), 0)

if __name__ == '__main__':
    unittest.main()
