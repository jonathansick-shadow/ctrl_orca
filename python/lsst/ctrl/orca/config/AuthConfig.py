import lsst.pex.config as pexConfig
import FakeTypeMap as fake

class AuthInfoConfig(pexConfig.Config):
    host = pexConfig.Field("host",str)
    user = pexConfig.Field("user",str)
    password = pexConfig.Field("password",str)
    port = pexConfig.Field("port",int)

class AuthDatabaseConfig(pexConfig.Config):
    #authNames = pexConfig.ListField("names",str)
    authInfo = pexConfig.ConfigChoiceField("auth info",fake.FakeTypeMap(AuthInfoConfig))

class AuthConfig(pexConfig.Config):
    database = pexConfig.ConfigField("root", AuthDatabaseConfig)
