#!/usr/bin/env python

import time
import unittest

import lsst.ctrl.orca.provenance as orcaProv

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

if __name__ == '__main__':
    unittest.main()
