#!/usr/bin/env python

import eups
import time
import unittest

import lsst.ctrl.orca.provenance as orcaProv
from lsst.daf.persistence import DbStorage, LogicalLocation

runId = "test_" + repr(time.time())

class ProvenanceTestCase(unittest.TestCase):
    """A test case for Provenance."""

    def setUp(self):
        global runId
        self.user = "test"
        self.runId = runId
        self.host = "lsst10.ncsa.uiuc.edu"

    def testConstruct(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.host)
        self.assert_(ps is not None)
        self.assert_(ps.db is not None)

    def testEnvironment(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.host)
        ps.recordEnvironment()

        db = DbStorage()
        db.setRetrieveLocation(
                LogicalLocation("mysql://%s:3306/provenance" % (self.host)))

        db.startTransaction()

        db.setTableListForQuery(
                ("prv_SoftwarePackage", "prv_cnf_SoftwarePackage"))
        db.outColumn("prv_SoftwarePackage.runId")
        db.outColumn("prv_SoftwarePackage.packageId")
        db.outColumn("packageName")
        db.outColumn("version")
        db.outColumn("directory")
        db.orderBy("packageName")
        db.setQueryWhere(
                """prv_SoftwarePackage.runId = '%s'
                AND prv_SoftwarePackage.runId = prv_cnf_SoftwarePackage.runId
                AND prv_SoftwarePackage.packageId =
                prv_cnf_SoftwarePackage.packageId""" % (self.runId))
        db.query()

        i = 1
        pkgs = eups.Eups().listProducts(setup=True)
        while db.next():
            self.assert_(not db.columnIsNull(0))
            self.assert_(not db.columnIsNull(1))
            self.assert_(not db.columnIsNull(2))
            self.assert_(not db.columnIsNull(3))
            self.assert_(not db.columnIsNull(4))
            self.assertEqual(db.getColumnByPosString(0), self.runId)
            self.assertEqual(db.getColumnByPosInt(1), i)
            self.assertEqual(db.getColumnByPosString(2), pkgs[i - 1][0])
            self.assertEqual(db.getColumnByPosString(3), pkgs[i - 1][1])
            self.assertEqual(db.getColumnByPosString(4), pkgs[i - 1][3])
            i += 1
        db.finishQuery()

        db.endTransaction()

    def testPolicies(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.host)
        ps.recordPolicy("tests/policy/dc2pipe.paf")
        ps.recordPolicy("tests/policy/imageSubtractionDetection.paf")

if __name__ == '__main__':
    unittest.main()
