class Verbosity( object ):
    _obj = None
    
    class Singleton:
        def __init__(self):
            self.value = 0
 
    def __init__( self ):
        # Check whether we already have an instance
        if Verbosity._obj is None:
            # Create and remember instanc
            Verbosity._obj = Verbosity.Singleton()
 
        # Store instance reference as the only member in the handle
        self.__dict__['_value_'] = Verbosity._obj
    
    
    def __getattr__(self, aAttr):
        return getattr(self._obj, aAttr)
 
 
    def __setattr__(self, aAttr, aValue):
        return setattr(self._obj, aAttr, aValue)
 
if __name__ == '__main__':
     s1 = Verbosity()
     print s1.value
     s1.value = 1
     s2 = Verbosity()
     print s2.value

