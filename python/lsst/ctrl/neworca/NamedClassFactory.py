class NamedClassFactory:
    def createClass(self, name):
        dot = name.rindex('.')
        pack = name[0:dot]
        modname = name[dot+1:]
        module = __import__(name, globals(), locals(), [modname], -1)
        classobj = getattr(module,modname)
        return classobj
