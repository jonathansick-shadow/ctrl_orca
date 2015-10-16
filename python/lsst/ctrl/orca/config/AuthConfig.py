import lsst.pex.config as pexConfig
import FakeTypeMap as fake

class AuthInfoConfig(pexConfig.Config):
    ## host name 
    host = pexConfig.Field("host",str)
    ## user name 
    user = pexConfig.Field("user",str)
    ## password
    password = pexConfig.Field("password",str)
    ## port number
    port = pexConfig.Field("port",int)

class AuthDatabaseConfig(pexConfig.Config):
    ## authorization configuration
    authInfo = pexConfig.ConfigChoiceField("auth info",fake.FakeTypeMap(AuthInfoConfig))

class AuthConfig(pexConfig.Config):
    ## authorization database configuration
    database = pexConfig.ConfigField("root", AuthDatabaseConfig)
