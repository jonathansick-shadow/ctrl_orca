import sys
import lsst.pex.config as pexConfig
import FakeTypeMap as fake

# workflow monitor configuration


class MonitorConfig(pexConfig.Config):
    # number of seconds to wait between status checks
    statusCheckInterval = pexConfig.Field("interval to wait for condor_q status checks", int, default=5)
