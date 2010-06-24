import os
import sys
import re
import socket
import lsst.pex.policy as pol

#
# This class takes template files and substitutes the values for the given
# keys, writing a new file generated from the template.
#
class TemplateWriter:
    def __init__(self):
        self.orcaValues = pol.Policy()
        self.orcaValues.set("ORCA_LOCAL_HOSTNAME", socket.gethostname())
        return

    #
    # given a input template, take the keys from the key/values in the policy
    # object and substitute the values, and write those to the output file.
    #
    def rewrite(self, input, output, pairs):
        fpInput = open(input, 'r')
        fpOutput = open(output, 'w')

        while True:
            line = fpInput.readline()
            if len(line) == 0:
                break

            # replace the "standard" orca names first
            for name in self.orcaValues.names():
                key = "$"+name
                val = str(self.orcaValues.get(name))
                line = line.replace(key,val)

            # replace the user defined names
            for name in pairs.names():
                key = "$"+name
                val = str(pairs.get(name))
                line = line.replace(key, val)
            fpOutput.write(line)
        fpInput.close()
        fpOutput.close()
