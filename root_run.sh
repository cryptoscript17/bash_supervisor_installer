#!/usr/bin/sudo /usr/bin/bash

# # # # #
#  |  To execute a shell script with sudo privileges directly from the script 
#  |  (without manually typing sudo ./script.sh),
#  |  you can specify sudo as part of the shebang line.

echo "You are '$(whoami)' user.."
#  echo $(gosu docker whoami)

apt update -y

apt install aiogram -y

export VENV_NAME="py_apps"
export PY_APPS_PATH="$(pwd)/${VENV_NAME}"

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root or with sudo"
  exit 1
else
  echo "Please welcome, ${EUID}, $(whoami) !"
fi

runuser -u docker -- cd "${PY_APPS_PATH}"
runuser -u docker -- mkdir -p "${PY_APPS_PATH}/temp"
echo "Python virtual environment «${VENV_NAME}» creating in «${PY_APPS_PATH}».."
runuser -u docker -- python3 -m venv $PY_APPS_PATH --system-site-packages --symlinks --prompt $VENV_NAME

chown -R docker "${PY_APPS_PATH}"

cd "${PY_APPS_PATH}"

echo "Activating VENV «${VENV_NAME}».."
#  chmod +x "${PY_APPS_PATH}/bin/activate"
source "${PY_APPS_PATH}/bin/activate"


for i in {1..3}; do
  runuser -u docker -- mkdir -p "${PY_APPS_PATH}/project_00${i}"
  cat << 'EOF' > "${PY_APPS_PATH}/project_00${i}/main.py"
  #!/usr/bin/env python3 
  # -*- coding: utf-8 -*-
  
  import os
  import json
  import datetime
  import logging
  
  logging.basicConfig(level=logging.INFO)
  
  def write_to_file(fpath, sample_json):
    with open(fpath, 'w') as fp:
      json.dump(sample_json, fp, indent=2)
    logging.info(f'Collected data saved to {filename}')
    return

  if __name__ == '__main__':
    print(f"Started _main_ execution..)
    # asyncio.run(main())
EOF
chown docker "${PY_APPS_PATH}/project_00${i}/main.py"
done
