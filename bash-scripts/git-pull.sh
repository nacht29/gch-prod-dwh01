#!/bin/bash

# Set PATH fro commands
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# git pull in gwh-prod-dwh01
cd /home/yanzhe/gch-prod-dwh01/ || exit 1
/usr/bin/git pull