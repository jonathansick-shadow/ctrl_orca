import os
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

class DatabaseConfigurator:
    def __init__(self):
        self.logger = Log(Log.getDefaultLog(), "dc3pipe")

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
    # This routine has the following behavior:
    #
    # 1) If only "database.host" is specified, it will match against the first
    # "database.authinfo.host" that matches it in the credentials policy, regardless
    # of the "database.authinfo.port".
    #
    # 2) If "database.host" and "database.port" are specified, it will match
    # against the first "database.authinfo.host" and "database.authinfo.port"
    # in the credentials policy.
    #
    # 3) If there is no match, an exception is thrown.
    # 
    def initAuthInfo(self, policy):
        host = policy.get("database.host")
        if host == None:
            raise RuntimeError("database host not specified in policy")
        port = policy.get("database.port")
        dbPolicyFile = os.path.join(os.environ["HOME"], ".lsst/lsst-db-auth.paf")
        dbPolicy = Policy.createPolicy(dbPolicyFile)

        authArray = dbPolicy.getArray("database.authinfo")

        for auth in authArray:
            self.dbHost = auth.get("host")
            self.dbPort = auth.get("port")
            self.dbUser = auth.get("user")
            self.dbPassword = auth.get("password")
            if self.dbHost == host:
                if port == None:
                    self.logger.log(Log.DEBUG, "using host %s at default port" % host)
                    return
                elif self.dbPort == port:
                    self.logger.log(Log.DEBUG, "using host %s at port %d" % (host, port))
                    return
        if port == None:
            raise RuntimeError("couldn't find any matching authorization for "+host)
        else:
            raise RuntimeError("couldn't find any matching authorization for host %s and port %d " % (host, port))
