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
import lsst.log as log

##
# @brief an interface for getting notified about changes in the status
# of a workflow: when it has started and when it has finished.
#


class StatusListener:
    # initializer

    def __init__(self):
        log.debug("StatusListener:__init__")

    ##
    #
    # @brief indicate taht a workflow has experience an as-yet unhandled
    # failure that prevents further processing
    #
    def workflowFailed(self, name, errorName, errmsg, event, pipelineName):
        return

    ##
    #
    # @brief the workflow has successfully shutdown and ready to be
    # cleaned up
    #
    def workflowShutdown(self, name):
        return

    ##
    #
    # @brief Called when a workflow has started up correctly and is
    # ready to process data.  Note that if a pipeline is waiting for
    # an event, the listener should be notified via a workflowWaiting()
    # message.
    def workflowStarted(self, name):
        return

    ##
    #
    # @brief Indicate that a workflow is waiting for an event to proceed.
    # This should only be called only for events that are expected from
    # outside the workflow.  Events that are meant to travel between Pipelines
    # within a workflow should not trigger this notification.
    #
    def workflowWaiting(self, name):
        return
