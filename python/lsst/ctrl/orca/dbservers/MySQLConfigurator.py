from __future__ import with_statement
from lsst.ctrl.orca.dbservers.DatabaseConfigurator import DatabaseConfigurator

import os

class MySQLConfigurator(DatabaseConfigurator):


    def configureDatabase(self, policy, runId):
        print "StdDatabaseConfigurator:configure called"

        command = "mysql -h %s -u%s -p%s "
        
        dbHost = policy.get("dbHost")

        # TODO: These next two line lines are placeholders, to
        # be replaced with reading from a .mysql/creds file
        dbUser = policy.get("dbUser")
        dbPassword = policy.get("dbPassword")
        dbCommandFilePolicy = policy.get("dbCommandFiles")
        dbCommandFiles = dbCommandFilePolicy.getArray("file")

        dbCommand = command % (dbHost, dbUser, dbPassword)

        packageDirEnv = policy.get("packageDirectoryEnv")
        sqldir = os.path.join(os.environ[packageDirEnv], "etc")

        cmd = '%s-e' % dbCommand
        createcmd = 'create database "%s"' % runId

        cmd = cmd.split()
        cmd.append(createcmd)

        print "would execute: ",cmd
        # TODO: execute this properly when "dryrun" in implemented
        # if (subprocess.call(cmd) != 0):
        #     raise RuntimeError("Failed to create database for run " + runId)


        for sqlCmdFile in dbCommandFiles:
            cmd = dbCommand + runId

            print "cmd = ",cmd
            print "sqldir = ",sqldir
            print "sqlCmdFile = ",sqlCmdFile
            with file(os.path.join(sqldir, sqlCmdFile)) as sqlFile:
                 print "would execute -> ",cmd.split
                 print "using stdin of = ", sqlFile
# TODO: execute this properly when "dryrun" in implemented
#                if subprocess.call(cmd.split(), stdin=sqlFile) != 0:
#                    raise RuntimeError("Failed to create execute " + sqlCmdFile)
