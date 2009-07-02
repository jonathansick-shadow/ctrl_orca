#!/usr/bin/env python

from lsst.ctrl.orca.dbservers.MySQLConfigurator import MySQLConfigurator
from lsst.cat.MySQLBase import MySQLBase
from lsst.cat.policyReader import PolicyReader
from lsst.daf.persistence import DbAuth

import getpass
import os
import subprocess

catDir = os.environ["CAT_DIR"]
policyF = os.path.join(catDir, 'policy/defaultTestCatPolicy.paf')

r = PolicyReader(policyF)
(host, port) = r.readAuthInfo()
(globalDbName, dcVersion, dcDb, \
 minPercDiskSpaceReq, userRunLife) = r.readGlobalSetup()


def resetGlobalDb():
    x = os.path.join(catDir, 'bin/destroyGlobal.py')
    cmd = '%s -f %s' % (x, policyF)
    subprocess.call(cmd.split())

    x = os.path.join(catDir, 'bin/setupGlobal.py')
    cmd = '%s -f %s' % (x, policyF)
    subprocess.call(cmd.split())

                            
def dropDB(usr, pwd):
    admin = MySQLBase(host, port)
    admin.connect(usr, pwd)
    admin.dropDb('%s_%s_u_myFirstRun' % (usr, dcVersion))
    admin.dropDb('%s_%s_u_mySecondRun' % (usr, dcVersion))
    admin.dropDb('%s_%s_p_prodRunA' % (usr, dcVersion))


def authUser(usr, pwd):
    print 'authorizing user...'
    x = os.path.join(catDir, 'bin/addMySqlUser.py')
    cmd = '%s -f %s -u %s -p %s' % (x, policyF, usr, pwd)
    subprocess.call(cmd.split())


def markRunFinished(dbName):
    admin = MySQLBase(host, port)
    admin.connect(usr, pwd, globalDbName)
    r = admin.execCommand1("SELECT setRunFinished('%s')" % dbName)
    print r

def startSomeRuns():
    x = MySQLConfigurator(host, port, globalDbName, dcVersion,
                          dcDb, minPercDiskSpaceReq, userRunLife)

    x.checkStatus(usr, pwd, host)

    r = x.prepareForNewRun('myFirstRun',  usr, pwd);
    print r
    markRunFinished('%s_%s_u_myFirstRun' %(usr, dcVersion))

    r = x.prepareForNewRun('mySecondRun', usr, pwd);
    print r
    #markRunFinished('%s_%s_u_mySecondRun' %(usr, dcVersion))


####################################################

if DbAuth.available(host, str(port)):
    usr = DbAuth.username(host, str(port))
    pwd = DbAuth.password(host, str(port))
else:
    print "Authorization unavailable for %s:%s" % (host, port)
    usr = raw_input("Enter mysql account name: ")
    pwd = getpass.getpass()

#authUser(usr, pwd)

resetGlobalDb()
dropDB(usr, pwd)

startSomeRuns()
