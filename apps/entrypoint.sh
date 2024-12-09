#!/usr/bin/bash

#  Run supervisord
source ./bin/activate
supervisord --user="$(whoami)" --configuration="$(pwd)/supervisord.conf" --pidfile="/var/run/supervisord.pid"
#  supervisord --user="$(whoami)" --directory="${TARGET_PATH}" --logfile="/var/log/supervisord_activity.log" --pidfile="/var/run/supervisord.pid" --configuration="${TARGET_PATH}/supervisord.conf"
