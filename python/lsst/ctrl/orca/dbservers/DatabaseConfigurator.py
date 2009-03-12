import os
import stat
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.ctrl.orca.dbservers import MySQLConfigurator

class DatabaseConfigurator:
    def __init__(self):
        self.logger = Log(Log.getDefaultLog(), "dc3pipe")
        

    def checkConfiguration(self):
        dbPolicyDir = os.path.join(os.environ["HOME"], ".lsst")
        self.checkUserOnlyPermissions(dbPolicyDir)

        dbPolicyFile = os.path.join(os.environ["HOME"], ".lsst/lsst-db-auth.paf")
        self.checkUserOnlyPermissions(dbPolicyFile)
        print "Was OK"

    def checkUserOnlyPermissions(self, checkFile):
        mode = os.stat(checkFile)[stat.ST_MODE]

        permissions = stat.S_IMODE(mode)

        errorText = "File permissions on "+checkFile+" should not be readable by 'group' or 'other'.  Use chmod to fix this."

        if (mode & getattr(stat, "S_IRWXG")) != 0:
            raise RuntimeError(errorText)
        if (mode & getattr(stat, "S_IRWXO")) != 0:
            raise RuntimeError(errorText)

    def configureDatabase(self, policy, runId):
        self.logger.log(Log.DEBUG, "DatabaseConfigurator:configure called")


    ##
    # initAuthInfo - given a policy object with specifies "database.host" and
    # optionally, "database.port", match it against the credential policy
    # file $HOME/.lsst/lsst-db-auth.paf
    #
    # The credential policy has the following format:
    #
    # database: {
    #     authinfo: {
    #         host:  lsst10.ncsa.uiuc.edu
    #         user:  moose
    #         password:  squirrel
    #         port: 3306
    #     }
    # 
    #     authinfo: {
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
    # and will match against the first "database.authinfo.host" and 
    # "database.authinfo.port"  in the credentials policy.
    #
    # If there is no match, an exception is thrown.
    # 
    def initAuthInfo(self, policy):
        host = policy.get("database.host")
        if host == None:
            raise RuntimeError("database host must be specified in policy")
        port = policy.get("database.port")
        if port == None:
            raise RuntimeError("database port must be specified in policy")
        dbPolicyFile = os.path.join(os.environ["HOME"], ".lsst/lsst-db-auth.paf")
        
        dbPolicy = Policy.createPolicy(dbPolicyFile)

        authArray = dbPolicy.getArray("database.authinfo")

        for auth in authArray:
            self.dbHost = auth.get("host")
            self.dbPort = auth.get("port")
            self.dbUser = auth.get("user")
            self.dbPassword = auth.get("password")
            if (self.dbHost == host) and (self.dbPort == port):
                    self.logger.log(Log.DEBUG, "using host %s at port %d" % (host, port))
                    return
            raise RuntimeError("couldn't find any matching authorization for host %s and port %d " % (host, port))
