from __future__ import with_statement
import os, subprocess
import lsst.ctrl.orca as orca
from lsst.ctrl.orca.dbservers.DatabaseConfigurator import DatabaseConfigurator
from lsst.pex.logging import Log


class MySQLConfigurator(DatabaseConfigurator):

    def configureDatabase(self, policy, runId):
        self.logger.log(Log.DEBUG, "StdDatabaseConfigurator:configure called")

        self.initAuthInfo(policy)

        command = "mysql -h %s -u%s -p%s "
        
        # TODO: These next two line lines are placeholders, to
        # be replaced with reading from a .mysql/creds file
        dbCommandFiles = policy.getArray("configuration.setup.database.script")

       
        dbCommand = command % (self.dbHost, self.dbUser, self.dbPassword)

        packageDirEnv = policy.get("packageDirectoryEnv")
        sqldir = os.path.join(os.environ["CAT_DIR"], "sql")

        cmd = '%s-e' % dbCommand
        createcmd = 'create database %s' % runId

        cmd = cmd.split()
        cmd.append(createcmd)

        print "would execute: ",cmd
        if orca.dryrun == True:
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
                if orca.dryrun == True:
                    print "will execute ",cmd.split()
                    print "using ", sqlFile
                else:
                    if subprocess.call(cmd.split(), stdin=sqlFile) != 0:
                        raise RuntimeError("Failed to create execute " + sqlCmdFile)
