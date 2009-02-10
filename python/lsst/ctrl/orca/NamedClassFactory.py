class NamedClassFactory:
    def createClass(self, name):
        #print "NamedClassFactory: called"
        dot = name.rindex('.')
        pack = name[0:dot]
        modname = name[dot+1:]
        #print "pack = "+pack
        #print "mod = "+modname
        module = __import__(name, globals(), locals(), [modname], -1)
        #print dir(module)
        classobj = getattr(module,modname)
        #print "NamedClassFactory: dir classobj is", dir(classobj)
        return classobj
