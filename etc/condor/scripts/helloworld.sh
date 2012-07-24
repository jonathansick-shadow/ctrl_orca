#!/bin/sh
date
echo "Hello from"
/bin/hostname -f
/bin/hostname -f >hostname.$$.out
sleep 60
date

