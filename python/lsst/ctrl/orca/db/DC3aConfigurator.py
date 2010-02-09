import os, stat
import lsst.ctrl.orca as orca
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.db.MySQLConfigurator import MySQLConfigurator

class DC3aConfigurator:
    def __init__(self, runid, policy, logger=None):
        """
        create a generic 
        @param type      the category of configurator
        @param policy    the policy to use in the configuration
        @param logger    the caller's Log instance from which this manager can.
                            create a child Log
        """
        if logger is None:  logger = orca.logger
        self.logger = Log(logger, "dbconfig")

        self.logger.log(Log.DEBUG, "Dc3aConfigurator:__init__")
        self.type = "mysql"
        self.runid = runid
        self.policy = policy
        self.delegate = None

        #
        # extract the databaseConfig.database policy to get required
        # parameters from it.

        dbHostName = policy.get("system.authInfo.host");
        portNo = policy.get("system.authInfo.port");
        globalDbName = policy.get("configuration.globalDbName")
        dcVersion = policy.get("configuration.dcVersion")
        dcDbName = policy.get("configuration.dcDbName")
        minPercDiskSpaceReq = policy.get("configuration.minPercDiskSpaceReq")
        userRunLife = policy.get("configuration.userRunLife")

        self.dbPolicy = policy

        self.delegate = MySQLConfigurator(dbHostName, portNo, globalDbName, dcVersion, dcDbName, minPercDiskSpaceReq, userRunLife)

    def setup(self):
        self.logger.log(Log.DEBUG, "Dc3aConfigurator:setup")

        self.checkConfiguration(self.dbPolicy)
        dbNames = self.prepareForNewRun(self.runid)
        return dbNames

    def checkConfiguration(self, policy):
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
        self.initAuthInfo(policy)

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

        errorText = "File permissions on "+checkFile+" should not be readable by 'group' or 'other'.  Use chmod to fix this."

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
