class DryRun( object ):
    _obj = None
    
    class Singleton:
        def __init__(self):
            self.value = False
 
    def __init__( self ):
        # Check whether we already have an instance
        if DryRun._obj is None:
            # Create and remember instanc
            DryRun._obj = DryRun.Singleton()
 
        # Store instance reference as the only member in the handle
        self.__dict__['_value_'] = DryRun._obj
    
    
    def __getattr__(self, aAttr):
        return getattr(self._obj, aAttr)
 
 
    def __setattr__(self, aAttr, aValue):
        return setattr(self._obj, aAttr, aValue)
 
if __name__ == '__main__':
     s1 = DryRun()
     print s1.value
     s1.value = True
     s2 = DryRun()
     print s2.value

