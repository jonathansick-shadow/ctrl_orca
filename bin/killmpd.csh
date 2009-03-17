#!/bin/sh
ssh lsst5 "source /lsst/DC3/stacks/default/loadLSST.csh; setup mpich2; mpdallexit;"
ssh lsst6 "source /lsst/DC3/stacks/default/loadLSST.csh; setup mpich2; mpdallexit;"
ssh lsst7 "source /lsst/DC3/stacks/default/loadLSST.csh; setup mpich2; mpdallexit;"
ssh lsst8 "source /lsst/DC3/stacks/default/loadLSST.csh; setup mpich2; mpdallexit;"
ssh lsst9 "source /lsst/DC3/stacks/default/loadLSST.csh; setup mpich2; mpdallexit;"
ssh lsst10 "source /lsst/DC3/stacks/default/loadLSST.csh; setup mpich2; mpdallexit;"
