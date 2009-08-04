class BasicPipelineConfigurator:
    def __init__(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:__init__")
        self.nodes = None

    def configure(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:configure")
        self.nodes = createNodeList()
        prepPlatform()
        createLaunchScript()
        deploySetup()
        setupDatabase()
        return 0 # return PipelineLauncher

    def createNodeList(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:createNodeList")
        node = self.policy.getArray("platform.deploy.nodes")

        nodes = map(self.expandNodeHost, node)
        # by convention, the master node is the first node in the list
        # we use this later to launch things, so strip out the info past ":", if it's there.
        self.masterNode = nodes[0]
        colon = self.masterNode.find(':')
        if colon > 1:
            self.masterNode = self.masterNode[0:colon]
        if orca.dryrun == False:
            nodelist = open(os.path.join(self.dirs.get("work"), "nodelist.scr"), 'w')
            for node in nodes:
                print >> nodelist, node
            nodelist.close()

        return nodes


    def prepPlatform(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:prepPlatform")
        createDirs()

    def deploySetup(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:deploySetup")

    def createDirs(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:createDirs")

        dirPolicy = self.policy.getPolicy("platform.dir")
        directories = Directories(dirPolicy, self.pipeline, self.runId)
        self.dirs = directories.getDirs()

        for name in self.dirs.names():
            if orca.dryrun == True:
                print "would create ",self.dirs.get(name)
            else:
                if not os.path.exists(self.dirs.get(name)): os.makedirs(self.dirs.get(name))


    def setupDatabase(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:setupDatabase")

        #classFactory = NamedClassFactory()
        #databaseConfigName = self.policy.get("databaseConfig.configuratorClass")

        dbConfigPolicy = self.policy.getPolicy("databaseConfig")
        dbPolicy = dbConfigPolicy.loadPolicyFiles(self.repository)
        dbPolicy = dbConfigPolicy.getPolicy("database")
        dbPolicy.loadPolicyFiles(self.repository)
        dbType = self.policy.get("databaseConfig.type")

        #self.logger.log(Log.DEBUG, "databaseConfigName = " + databaseConfigName)
        #databaseConfiguratorClass = classFactory.createClass(databaseConfigName)
        #self.databaseConfigurator = databaseConfiguratorClass(dbType, dbPolicy)        
        self.dbConfigurator = DatabaseConfigurator(dbType, dbPolicy)
        self.dbConfigurator.setup()
        # these next two lines have to be done within setup in dbConfigurator
        #self.dbConfigurator.checkConfiguration(dbPolicy)
        #dbNames = self.dbConfigurator.prepareForNewRun(self.runid)

    def expandNodeHost(self, nodeentry):
        """Add a default network domain to a node list entry if necessary """

        if nodeentry.find(".") < 0:
            node = nodeentry
            colon = nodeentry.find(":")
            if colon == 0:
                raise RuntimeError("bad nodelist format: " + nodeentry)
            elif colon > 0:
                node = nodeentry[0:colon]
                if len(node) < 3:
                    #logger.log(Log.WARN, "Suspiciously short node name: " + nod
e)
                    self.logger.log(Log.DEBUG, "Suspiciously short node name: " 
+ node)
                self.logger.log(Log.DEBUG, "-> nodeentry  =" + nodeentry)
                self.logger.log(Log.DEBUG, "-> node  =" + node)
                node += "."+self.defaultDomain
                nodeentry = "%s:%s" % (node, nodeentry[colon+1:])
            else:
                nodeentry = "%s%s:1" % (node, self.defaultDomain)
        
