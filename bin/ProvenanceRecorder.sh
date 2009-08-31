#!/bin/sh
echo $PYTHONPATH
$CTRL_ORCA_DIR/python/lsst/ctrl/orca/provenance/Recorder.py --type=lsst.ctrl.orca.provenance.BasicProvenanceRecorder --runid=runid --user=user --dbrun=dbrun --dbglobal=dbglobal --repos=$CTRL_DC3PIPE_DIR/pipeline  --filename=ap-cfht-nfs.paf
