# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import os, stat
import lsst.ctrl.orca as orca
import lsst.ctrl.provenance.dc3 as dc3
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.db.MySQLConfigurator import MySQLConfigurator

class SdssConfigurator:
    def __init__(self, runid, dbPolicy, prodPolicy=None, wfPolicy=None, logger=None):
        """
        create a generic 
        @param type      the category of configurator
        @param dbPolicy  the database policy
        @param logger    the caller's Log instance from which this manager can.
                            create a child Log
        """
        if logger is None:  logger = orca.logger
        self.logger = Log(logger, "dbconfig")

        self.logger.log(Log.DEBUG, "SdssConfigurator:__init__")
        self.type = "mysql"
        self.runid = runid
        self.dbPolicy = dbPolicy
        self.perProductionRunDatabase = None

        self.platformName = ""
        self.prodPolicy = prodPolicy
        if self.prodPolicy is not None:
            self.platformName = "production"
        self.wfPolicy = wfPolicy

        #
        # extract the databaseConfig.database policy to get required
        # parameters from it.

        self.dbHostName = dbPolicy.get("system.authInfo.host")
        self.dbPort = dbPolicy.get("system.authInfo.port")
        self.globalDbName = dbPolicy.get("configuration.globalDbName")
        self.dcVersion = dbPolicy.get("configuration.dcVersion")
        self.dcDbName = dbPolicy.get("configuration.dcDbName")
        self.minPercDiskSpaceReq = dbPolicy.get("configuration.minPercDiskSpaceReq")
        self.userRunLife = dbPolicy.get("configuration.userRunLife")
        self.sqlDir = os.path.join(os.environ["CAT_DIR"], "sql")
        self.delegate = MySQLConfigurator(self.dbHostName, self.dbPort,
                self.globalDbName, self.dcVersion, self.dcDbName,
                self.minPercDiskSpaceReq, self.userRunLife)

    def setup(self, provSetup):
        self.logger.log(Log.DEBUG, "SdssConfigurator:setup")

        # TODO: use provSetup when it's implemented

        dbNames = self.setupInternal()

        # construct dbRun and dbGlobal URLs here

        dbBaseURL = self.getHostURL()
        self.perProductionRunDatabase = dbNames[0]
        dbRun = dbBaseURL+"/"+dbNames[0]
        dbGlobal = dbBaseURL+"/"+dbNames[1]

        recorder = dc3.Recorder(self.runid, self.prodPolicy.get("shortName"), self.platformName, dbRun, dbGlobal, 0, None, self.logger)
        provSetup.addProductionRecorder(recorder)

        arglist = []
        arglist.append("--runid=%s" % self.runid)
        arglist.append("--dbrun=%s" % dbRun)
        arglist.append("--dbglobal=%s" % dbGlobal)
        arglist.append("--runoffset=%s" % recorder.getRunOffset())
        provSetup.addWorkflowRecordCmd("PipelineProvenanceRecorder.py", arglist)

    def getDBInfo(self):
        dbInfo = {} 
        dbInfo["host"] = self.dbHostName
        dbInfo["port"] = self.dbPort
        dbInfo["runid"] = self.runid
        dbInfo["dbrun"] = self.perProductionRunDatabase
        return dbInfo

    def setupInternal(self):
        self.logger.log(Log.DEBUG, "SdssConfigurator:setupInternal")

        self.checkConfiguration(self.dbPolicy)
        dbNames = self.prepareForNewRun(self.runid)
        return dbNames

    def checkConfiguration(self, val):
        self.logger.log(Log.DEBUG, "SdssConfigurator:checkConfiguration")
        # TODO: use val when implemented
        self.checkConfigurationInternal()

    def checkConfigurationInternal(self):
        self.logger.log(Log.DEBUG, "SdssConfigurator:checkConfigurationInternal")
        #
        # first, check that the $HOME/.lsst directory is protected
        #
        dbPolicyDir = os.path.join(os.environ["HOME"], ".lsst")
        self.checkUserOnlyPermissions(dbPolicyDir)

        #
        # next, check that the $HOME/.lsst/db-auth.paf file is protected
        #
        dbPolicyCredentialsFile = os.path.join(os.environ["HOME"], ".lsst/db-auth.paf")
        self.checkUserOnlyPermissions(dbPolicyCredentialsFile)

        #
        # now, look up and initialize the authorization information for host and port
        #
        self.initAuthInfo(self.dbPolicy)

    def getHostURL(self):
        schema = self.type.lower()
        retVal = schema+"://"+self.dbHost+":"+str(self.dbPort)
        return retVal

    def getUser(self):
        return self.dbUser;

    def checkUserOnlyPermissions(self, checkFile):
        mode = os.stat(checkFile)[stat.ST_MODE]

        permissions = stat.S_IMODE(mode)

        errorText = "File permissions on "+checkFile+" should not be readable, writable, or executable  by 'group' or 'other'.  Use chmod to fix this. (chmod 700 ~/.lsst)"

        if (mode & getattr(stat, "S_IRWXG")) != 0:
            raise RuntimeError(errorText)
        if (mode & getattr(stat, "S_IRWXO")) != 0:
            raise RuntimeError(errorText)

    def prepareForNewRun(self, runName, runType='u'):
        userName = self.dbUser
        userPassword = self.dbPassword

        if (runType != 'p' and runType != 'u'):
            raise RuntimeError("Invalid runType '%c', expected 'u' or 'p'" % \
                               runType)
        if runName == "":
            raise RuntimeError("Invalid (empty) runName")

        # prepare list of sql scripts to load
        fN = "lsstSchema4mysql%s.sql" % self.dcVersion
        dbScripts = [os.path.join(self.sqlDir, fN),
                     os.path.join(self.sqlDir, "setup_storedFunctions.sql"),
                     os.path.join(self.sqlDir, "setup_perRunTables_sdss.sql")]

        # Verify these scripts exist
        for f in dbScripts:
            if not os.path.exists(f):
                raise RuntimeError("Can't find file '%s'" % f)

        # connect to the global database
        self.delegate.connect(userName, userPassword, self.globalDbName)

        # check if available disk space is not below required limit
        # for that directory
        # does not work if db server is accessed remotely...
        # percDiskSpaceAvail = self.getDataDirSpaceAvailPerc()
        # if percDiskSpaceAvail < self.minPercDiskSpaceReq:
        #    self.disconnect()
        #    raise RuntimeError(
        #        "Not enough disk space available in mysql " +
        #        "datadir, required %i%%, available %i%%" % 
        #        (self.minPercDiskSpaceReq, percDiskSpaceAvail))

        if runType == 'p':
            runLife = 1000 # ensure this run "never expire"
            # TODO: check if userName is authorized to start production run
        else:
            runLife = self.userRunLife

        # create database for this new run
        # format: <userName>_<DC version>_<u|p>_<run number or name>
        runDbName = "%s_%s_%c_%s" % (userName, self.dcVersion, runType, runName)
        self.delegate.createDb(runDbName)

        # load all scripts
        for fP in dbScripts:
            self.delegate.loadSqlScript(fP, userName, userPassword, runDbName)

        # Register this run in the global database
        cmd = """INSERT INTO RunInfo 
                 (runName, dcVersion, dbName, startDate, expDate, initiator)
                 VALUES ('%s', '%s', '%s', NOW(), 
                     DATE_ADD(NOW(), INTERVAL %i WEEK), '%s')""" % \
            (runName, self.dcVersion, runDbName, runLife, userName)
        self.delegate.execCommand0(cmd)

        self.delegate.disconnect()

        return (runDbName, self.dcDbName)

    def runFinished(self, dbName):
        pass

    ##
    # initAuthInfo - given a policy object with specifies "database.host" and
    # optionally, "database.port", match it against the credential policy
    # file $HOME/.lsst/db-auth.paf
    #
    # The credential policy has the following format:
    #
    # database: {
    #     authInfo: {
    #         host:  lsst10.ncsa.uiuc.edu
    #         user:  moose
    #         password:  squirrel
    #         port: 3306
    #     }
    # 
    #     authInfo: {
    #         host:  lsst10.ncsa.uiuc.edu
    #         user:  boris
    #         password:  natasha
    #         port: 3307
    #     }
    # }
    #
    #
    #
    # Terms "database.host" and "database.port" must be specified, 
    # and will match against the first "database.authInfo.host" and 
    # "database.authInfo.port"  in the credentials policy.
    #
    # If there is no match, an exception is thrown.
    # 
    def initAuthInfo(self, policy):
        host = policy.get("system.authInfo.host")
        if host == None:
            raise RuntimeError("database host must be specified in policy")
        port = policy.get("system.authInfo.port")
        if port == None:
            raise RuntimeError("database port must be specified in policy")
        dbPolicyCredentialsFile = os.path.join(os.environ["HOME"], ".lsst/db-auth.paf")
        
        dbPolicyCredentials = Policy.createPolicy(dbPolicyCredentialsFile)

        authArray = dbPolicyCredentials.getPolicyArray("database.authInfo")

        for auth in authArray:
            self.dbHost = auth.get("host")
            self.dbPort = auth.get("port")
            self.dbUser = auth.get("user")
            self.dbPassword = auth.get("password")
            if (self.dbHost == host) and (self.dbPort == port):
                    self.logger.log(Log.DEBUG, "using host %s at port %d" % (host, port))
                    return
        raise RuntimeError("couldn't find any matching authorization for host %s and port %d " % (host, port))
