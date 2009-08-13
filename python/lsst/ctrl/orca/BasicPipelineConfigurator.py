class BasicPipelineConfigurator(PipelineConfigurator):
    def __init__(self):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:__init__")
        self.nodes = None

    def configure(self, policy):
        self.logger.log(Log.DEBUG, "BasicPipelineConfigurator:configure")
        self.nodes = createNodeList()
        self.policy = policy
        prepPlatform()
        createLaunchScript()
        deploySetup()
        setupDatabase()
        pipelineLauncher = PipelineLauncher()
        return pipelineLauncher

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

       # copy /bin/sh script responsible for environment setting

        setupPath = self.policy.get("configuration.framework.environment")
        if setupPath == None:
             raise RuntimeError("couldn't find configuration.framework.environment")
        self.script = EnvString.resolve(setupPath)

        ## TODO: We did this same thing in DC2. We shouldn't be
        ## depending the system we launch on to determine which version
        ## of the setup.*sh script to run.   The remote systems aren't
        ## guaranteed to be running the same shell as the interactive
        ## shell from which orca was launched.

        if orca.envscript == None:
            print "using default setup.sh"
        else:
            self.script = orca.envscript

        shutil.copy(self.script, self.dirs.get("work"))

        # now point at the new location for the setup script
        self.script = os.path.join(self.dirs.get("work"),os.path.basename(self.script))

        # 
        #  read in default policy
        #  read in given policy
        #  in given policy:
        #     set: execute.eventBrokerHost
        #     set: execute.dir
        #     set: execute.database.url
        #  write new policy file with overridden values
        #  write file to self.dirs["work"]
        #  call provenance.recordPolicy()
        # 
        # copy the policies to the working directory
        polfile = os.path.join(self.repository, self.pipeline+".paf")

        newPolicy = pol.Policy.createPolicy(polfile, False)

        if self.prodPolicyOverrides is not None:
            for name in self.prodPolicyOverrides.paramNames():
                newPolicy.set(name, self.prodPolicyOverrides.get(name))

#        eventBrokerHost = self.policy.get("configuration.execute.eventBrokerHost")
#        newPolicy.set("execute.eventBrokerHost", eventBrokerHost)

        executeDir = self.policy.get("platform.dir")
        newPolicy.set("execute.dir", executeDir)

        #baseURL = self.dbConfigurator.getHostURL()

        #fullURL = baseURL+"/"+ dbNames[0]
        newPolicy.set("execute.database.url",self.dbRunURL)


        polbasefile = os.path.basename(polfile)
        newPolicyFile = os.path.join(self.dirs.get("work"), self.pipeline+".paf")
        if os.path.exists(newPolicyFile):
            self.logger.log(Log.WARN,
                       "Working directory already contains %s; won't overwrite" % \
                           polbasefile)
        else:
            pw = pol.PAFWriter(newPolicyFile)
            pw.write(newPolicy)
            pw.close()

        self.provenance.recordPolicy(newPolicyFile)
        self.policySet.add(newPolicyFile)

        # XXX - reuse "newPolicy"?
        newPolicyObj = pol.Policy.createPolicy(newPolicyFile, False)
        pipelinePolicySet = sets.Set()
        self.recordChildPolicies(self.repository, newPolicyObj, pipelinePolicySet)

        if os.path.exists(os.path.join(self.dirs.get("work"), self.pipeline)):
            self.logger.log(Log.WARN,
              "Working directory already contains %s directory; won't overwrite" % \
                           self.pipeline)
        else:
            #shutil.copytree(os.path.join(self.repository, self.pipeline), os.path.join(self.dirs.get("work"),self.pipeline))
            #
            # instead of blindly copying the whole directory, take the set
            # if files from policySet and copy those.
            #
            # This is slightly tricky, because we want to copy from the policy file     
            # repository directory to the "work" directory, but we also want to keep    
            # that partial directory hierarchy we're copying to as well.
            #
            for filename  in pipelinePolicySet:
                destinationDir = self.dirs.get("work")
                destName = filename.replace(self.repository+"/","")
                tokens = destName.split('/')
                tokensLength = len(tokens)
                # if the destination directory heirarchy doesn't exist, create all          
                # the missing directories
                destinationFile = tokens[len(tokens)-1]
                for newDestinationDir in tokens[:len(tokens)-1]:                    newDir = os.path.join(destinationDir, newDestinationDir)
                    if os.path.exists(newDir) == False:
                        os.mkdir(newDir)
                    destinationDir = newDir
                shutil.copyfile(filename, os.path.join(destinationDir, destinationFile))


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
        
