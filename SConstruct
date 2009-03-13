# -*- python -*-
#
# Setup our environment
#
import glob, os.path, re, sys
import lsst.SConsUtils as scons

dependencies = ["boost", "pex_exceptions", "utils", "daf_base", "pex_policy", "daf_persistence"]

env = scons.makeEnv("ctrl_orca",
                    r"$HeadURL$",
                    [["boost", "boost/regex.hpp", "boost_regex:C++"],
		     ["pex_exceptions", "lsst/pex/exceptions.h", "pex_exceptions:C++"],
                     ["utils", "lsst/utils/Utils.h", "utils:C++"],
                     ["daf_base", "lsst/daf/base.h", "daf_base:C++"],
                     ["pex_policy", "lsst/pex/policy/Policy.h", "pex_policy:C++"],
                     ["daf_persistence", "lsst/daf/persistence.h", "daf_persistence:C++"]
                    ])
env.Help("""
LSST Pipeline Orchestration package
""")

###############################################################################
# Boilerplate below here

pkg = env["eups_product"]
env.libs[pkg] += env.getlibs(" ".join(dependencies))

#
# Build/install things
#
for d in Split("lib python/lsst/" + re.sub(r'_', "/", pkg) + " examples tests doc"):
    if os.path.isdir(d):
        try:
            SConscript(os.path.join(d, "SConscript"))
        except Exception, e:
            print >> sys.stderr, "%s: %s" % (os.path.join(d, "SConscript"), e)

env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

Alias("install", [env.Install(env['prefix'], "python"),
                  env.Install(env['prefix'], "bin"),
                  env.Install(env['prefix'], "policies"),
                  env.Install(env['prefix'], "setups"),
                  env.InstallEups(env['prefix'] + "/ups")])

scons.CleanTree(r"*~ core *.so *.os *.o")

#
# Build TAGS files
#
files = scons.filesToTag()
if files:
    env.Command("TAGS", files, "etags -o $TARGET $SOURCES")

env.Declare()
