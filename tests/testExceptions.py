#!/usr/bin/env python
"""
Tests of orca exceptions
"""
import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

from lsst.ctrl.orca.exceptions import *

class MultiIssueTestCase(unittest.TestCase):
    unspecified = "Unspecified configuration problems encountered"
    generic = "Multiple configuration problems encountered"
    
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testNoProb(self):
        err = MultiIssueConfigurationError()
        self.assert_(not err.hasProblems(), "no problems added yet")
        self.assertEquals(str(err), self.unspecified)
        
        probs = err.getProblems()
        self.assertEquals(len(probs), 0)
        
    def testOneProb(self):
        msg = "first problem"
        err = MultiIssueConfigurationError(problem=msg)
        self.assertEquals(str(err), msg)

        probs = err.getProblems()
        self.assertEquals(len(probs), 1)
        self.assertEquals(probs[0], msg)
        
    def testTwoProbs(self):
        msg = "first problem"
        err = MultiIssueConfigurationError(problem=msg)
        self.assertEquals(str(err), msg)
        msg2 = "second problem"
        err.addProblem(msg2)
        self.assertEquals(str(err), self.generic)

        probs = err.getProblems()
        self.assertEquals(len(probs), 2)
        self.assertEquals(probs[0], msg)
        self.assertEquals(probs[1], msg2)

    def testGenericMsg(self):
        msg = "problems encountered while checking configuration"
        err = MultiIssueConfigurationError(msg)
        self.assertEquals(str(err), self.unspecified)

        msg1 = "first problem"
        err.addProblem(msg1)
        self.assertEquals(str(err), msg1)

        err.addProblem("2nd problem")
        self.assertEquals(str(err), msg)
        

    
__all__ = "MultiIssueTestCase".split()        

if __name__ == "__main__":
    unittest.main()

