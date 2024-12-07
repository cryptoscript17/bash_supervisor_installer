#!/usr/bin/sudo /usr/bin/bash

# Check if supervisord is running
if pgrep -x "supervisord" > /dev/null
then
  echo "supervisord is running, stopping it..."
  
  # Shutdown supervisord using supervisorctl
  sudo supervisorctl shutdown
  
  # Optionally, use systemctl if supervisord is managed by systemd
  # sudo systemctl stop supervisord
  
  echo "supervisord has been stopped."
else
  echo "supervisord is not running."
fi

