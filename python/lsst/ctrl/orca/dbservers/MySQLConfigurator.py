from __future__ import with_statement
import os, subprocess
from lsst.ctrl.orca.dbservers.DatabaseConfigurator import DatabaseConfigurator
from lsst.pex.logging import Log
from lsst.ctrl.orca.DryRun import DryRun


class MySQLConfigurator(DatabaseConfigurator):

    def configureDatabase(self, policy, runId):
        self.logger.log(Log.DEBUG, "StdDatabaseConfigurator:configure called")

        command = "mysql -h %s -u%s -p%s "
        
        dbHost = self.dbPolicy.get("dbHost")

        # TODO: These next two line lines are placeholders, to
        # be replaced with reading from a .mysql/creds file
        dbHost = self.dbPolicy.get("database.host")
        dbUser = self.dbPolicy.get("database.user")
        dbPassword = self.dbPolicy.get("database.password")
        dbCommandFiles = policy.getArray("configuration.setup.database.script")

       
        dbCommand = command % (dbHost, dbUser, dbPassword)

        packageDirEnv = policy.get("packageDirectoryEnv")
        sqldir = os.path.join(os.environ["CAT_DIR"], "sql")

        cmd = '%s-e' % dbCommand
        createcmd = 'create database %s' % runId

        cmd = cmd.split()
        cmd.append(createcmd)

        print "would execute: ",cmd
        if self.dryrun.value == True:
            print "would execute: ",cmd
        else :
            if (subprocess.call(cmd) != 0):
                raise RuntimeError("Failed to create database for run " + runId)


        print "dbCommandFiles :",dbCommandFiles
        for sqlCmdFile in dbCommandFiles:
            cmd = dbCommand + runId

            self.logger.log(Log.DEBUG, "cmd = " + cmd)
            self.logger.log(Log.DEBUG, "sqldir = " + sqldir)
            self.logger.log(Log.DEBUG, "sqlCmdFile = " + sqlCmdFile)
            with file(os.path.join(sqldir, sqlCmdFile)) as sqlFile:
                if self.dryrun.value == True:
                    print "will execute ",cmd.split()
                    print "using ", sqlFile
                else:
                    if subprocess.call(cmd.split(), stdin=sqlFile) != 0:
                        raise RuntimeError("Failed to create execute " + sqlCmdFile)
