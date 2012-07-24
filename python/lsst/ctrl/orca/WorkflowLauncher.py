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

import subprocess
from lsst.pex.logging import Log
from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor

##
# @brief an abstract class for configuring a workflow
#
# This class should not be used directly but rather must be subclassed,
# providing an implementation for _configureSpecialized.
# 
class WorkflowLauncher:
    ##
    # @brief
    #
    # This constructor should only be called from a subclass's
    # constructor, in which case the fromSub parameter must be
    # set to True.
    # 
    # @param wfConfig    workflow config
    # @param logger      the logger used by the caller.  This class
    #                       will set this create a child log with the
    #                       subname "config".  A sub class may wish to
    #                       reset the child logger for a different subname.
    # 
    def __init__(self, wfConfig, logger = None):
        if not logger:
            logger = Log.getDefaultLog()
        self.parentLogger = logger
        self.logger = Log(logger, "launch")
        self.logger.log(Log.DEBUG, "WorkflowLauncher:__init__")

        self.wfConfig = wfConfig

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        self.logger.log(Log.DEBUG, "WorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener):
        self.logger.log(Log.DEBUG, "WorkflowLauncher:launch")

        self.workflowMonitor = WorkflowMonitor(self.logger, self.wfConfig)
        if statusListener != None:
            self.workflowMonitor.addStatusListener(statusListener)
        return self.workflowMonitor # returns WorkflowMonitor
