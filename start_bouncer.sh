#!/bin/bash
cd /app
nohup python3 -u bouncer/main.py > /tmp/bouncer_log.txt 2>&1 &
sleep 2
cat /tmp/bouncer_log.txt
