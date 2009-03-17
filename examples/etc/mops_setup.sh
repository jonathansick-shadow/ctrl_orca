source /lsst/DC3/stacks/default/loadLSST.sh
# hack to get things running until dc3pipe is released to main stack
HERE=$PWD
cd /home/srp/temp_merge/dc3pipe/trunk
setup -r .
cd $HERE
# eoh (end of hack)
setup mops
