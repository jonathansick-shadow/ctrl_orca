class ConfigurationError(RuntimeError):
    """
    an exception that indicates that an error occurred in the configuration
    of a production run or one of its components.
    """
    pass

class MultiIssueConfigurationError(ConfigurationError):
    """
    a configuration error that can report on multiple problems.
    
    The intented pattern of use for this class is that it is
    created before any problems are found.  As they are found,
    calls are made to addProblem().  Finally, after all possible
    problems are found, one can call hasProblems().  If it is
    true, then this exception instance should be raised.
    """

    def __init__(self, msg=None, problem=None):
        """
        create the exception.
        @param msg      the general message to report when more than problem has
                          been encountered.  If only one problem is added to this
                          exception, that problem message will be displayed.
                          If None, a generic message is set.
        @param problem  the first problem to add to this exception.  
        """
        if msg is None:
            msg = "Multiple configuration problems encountered"
        ConfigurationError.__init__(self, msg)
        self._probs = []
        if problem is not None:
            self.addProblem(problem)

    def addProblem(self, msg):
        """
        add a message indicating one of the problems encountered
        """
        self._probs.append(msg)

    def hasProblems(self):
        """
        return True if this exception as at least one problem added to it.
        """
        return len(self._probs) > 0

    def getProblems(self):
        """
        return a copy of the list of problems
        """
        return list(self._probs)

    def __str__(self):
        if len(self._probs) < 1:
            return "Unspecified configuration problems encountered"
        elif len(self._probs) == 1:
            return self._probs[0]
        else:
            return ConfigurationError.__str__(self)

    def __repr__(self):
        return "MultiIssueConfigurationError: " + str(self)



