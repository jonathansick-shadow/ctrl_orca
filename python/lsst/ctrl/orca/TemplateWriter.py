import os
import sys
import re

#
# This class takes template files and substitutes the values for the given
# keys, writing a new file generated from the template.
#
class TemplateWriter:
    def __init__(self):
        return

    def rewrite(self, input, output, pairs):
        #pairs = [("$START_OWNER", "srp"), ("$ADMIN_EMAIL", "srp@ncsa.uiuc.edu"), ("$MACHINE_COUNT", "1"), ("$TIME_REQUESTED","15")]

        print input, "-", output

        fpInput = open(input, 'r')
        fpOutput = open(output, 'w')

        while True:
            line = fpInput.readline()
            if len(line) == 0:
                break

            for pair in pairs:
                line = line.replace(pair[0],pair[1])
            print line,
        fpInput.close()
        fpOutput.close()

