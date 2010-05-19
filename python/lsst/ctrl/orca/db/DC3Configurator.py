import os, stat
import lsst.ctrl.orca as orca
import lsst.ctrl.provenance.dc3 as dc3
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.db.MySQLConfigurator import MySQLConfigurator

class DC3Configurator:
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

        self.logger.log(Log.DEBUG, "DC3Configurator:__init__")
        self.type = "mysql"
        self.runid = runid
        self.dbPolicy = dbPolicy
        self.delegate = None

        self.platformName = ""
        self.prodPolicy = prodPolicy
        if self.prodPolicy is not None:
            self.platformName = "production"
        self.wfPolicy = wfPolicy

        #
        # extract the databaseConfig.database policy to get required
        # parameters from it.

        dbHostName = dbPolicy.get("system.authInfo.host");
        portNo = dbPolicy.get("system.authInfo.port");
        globalDbName = dbPolicy.get("configuration.globalDbName")
        dcVersion = dbPolicy.get("configuration.dcVersion")
        dcDbName = dbPolicy.get("configuration.dcDbName")
        minPercDiskSpaceReq = dbPolicy.get("configuration.minPercDiskSpaceReq")
        userRunLife = dbPolicy.get("configuration.userRunLife")

        self.delegate = MySQLConfigurator(dbHostName, portNo, globalDbName, dcVersion, dcDbName, minPercDiskSpaceReq, userRunLife)

    def setup(self, provSetup):
        self.logger.log(Log.DEBUG, "DC3Configurator:setup")

        # TODO: use provSetup when it's implemented

        dbNames = self.setupInternal()

        # construct dbRun and dbGlobal URLs here

        dbBaseURL = self.getHostURL()
        dbRun = dbBaseURL+"/"+dbNames[0];
        dbGlobal = dbBaseURL+"/"+dbNames[1];

        print "dbRun =",dbRun
        print "dbGlobal =",dbGlobal
        recorder = dc3.Recorder(self.runid, self.prodPolicy.get("shortName"), self.platformName, dbRun, dbGlobal, 0, None, self.logger)
        provSetup.addProductionRecorder(recorder)

        arglist = []
        arglist.append("--runid=%s" % self.runid)
        arglist.append("--dbrun=%s" % dbRun)
        arglist.append("--dbglobal=%s" % dbGlobal)
        arglist.append("--runoffset=%s" % recorder.getRunOffset())
        provSetup.addWorkflowRecordCmd("PipelineProvenanceRecorder.py", arglist)

    def setupInternal(self):
        self.logger.log(Log.DEBUG, "DC3Configurator:setupInternal")

        self.checkConfiguration(self.dbPolicy)
        dbNames = self.prepareForNewRun(self.runid)
        return dbNames

    def checkConfiguration(self, val):
        self.logger.log(Log.DEBUG, "DC3Configurator:checkConfiguration")
        # TODO: use val when implemented
        self.checkConfigurationInternal()

    def checkConfigurationInternal(self):
        self.logger.log(Log.DEBUG, "DC3Configurator:checkConfigurationInternal")
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
        # now, look up, and initialize the authorization information for host and port
        #
        self.initAuthInfo(self.dbPolicy)

        # 
        # Now that everything looks sane, execute the delegate's checkStatus method 
        #
        self.delegate.checkStatus(self.dbUser, self.dbPassword, os.uname()[1])

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
        return self.delegate.prepareForNewRun(runName, self.dbUser, self.dbPassword, runType)

    def runFinished(self, dbName):
        self.delegate(dbName)

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
