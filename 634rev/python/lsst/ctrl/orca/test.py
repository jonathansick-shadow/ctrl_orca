#!/usr/bin/env python

import optparse
from lsst.pex.policy import Policy

opts = {}
args = []

parser = optparse.OptionParser()

# parse and check command line arguments
(opts, args) = parser.parse_args()

pipelinePolicyFile = args[0]

print "pipelinePolicyFile = "+pipelinePolicyFile

policy = Policy.createPolicy(pipelinePolicyFile)
print policy.toString()
