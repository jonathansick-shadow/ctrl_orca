import re,os
class EnvString:

    ##
    # given a string, look for any $ prefixed word, attempt to subsitute
    # an environment variable with that name.  
    #
    # @throw exception if the environment variable doesn't exist
    #
    # Return the resulting string
    def resolve(strVal):
        p = re.compile('\$[a-zA-Z0-9_]+')
        retVal = strVal
        exprs = p.findall(retVal)
        for i in exprs:
            var = i[1:]
            val = os.getenv(var, None)
            if val == None:
                raise RuntimeError("couldn't find "+i+" environment variable")
            retVal = p.sub(val,retVal,1)
        return retVal
    resolve = staticmethod(resolve)
