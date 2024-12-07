#!/usr/bin/sudo /usr/bin/bash
set -e
set -x

# # # # #
#  |  To execute a shell script with sudo privileges directly from the script 
#  |  (without manually typing sudo ./script.sh),
#  |  you can specify sudo as part of the shebang line.

export USER_EXECUTOR="docker"
export EXECUTOR_GROUP=$USER_EXECUTOR
export NEW_VENV_NAME="docker_supervisord"
export NEW_VENV_PATH="/home/docker/docker_app/docker_supervisord"
export FROM_PATH=$(pwd) && echo $FROM_PATH

# apt update -y
# apt install aiogram -y

export VENV_NAME="py_apps"
export PY_APPS_PATH="$(pwd)/${VENV_NAME}"

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root or with sudo"
  exit 1
else
  echo "Please welcome, ${EUID}, $(whoami) !"
fi

echo "You are '$(whoami)' user.."
#  echo $(gosu docker whoami)

rm -rf $PY_APPS_PATH
runuser -u $USER_EXECUTOR -- mkdir -p "${PY_APPS_PATH}/temp"
runuser -u $USER_EXECUTOR -- cp -v -R "${FROM_PATH}/apps/" "${PY_APPS_PATH}/"
runuser -u $USER_EXECUTOR -- cp -v "${FROM_PATH}/configs/supervisord.conf" "${PY_APPS_PATH}"
runuser -u $USER_EXECUTOR -- cp -v "${FROM_PATH}/configs/vendors_json.txt" "${PY_APPS_PATH}"
runuser -u $USER_EXECUTOR -- cp -v "${FROM_PATH}/scripts/create_requirements.sh" "${PY_APPS_PATH}"
runuser -u $USER_EXECUTOR -- cp -v "${FROM_PATH}/scripts/shutdown_supervisord.sh" "${PY_APPS_PATH}"


echo "Python virtual environment «${VENV_NAME}» creating in «${PY_APPS_PATH}».."
runuser -u $USER_EXECUTOR -- python3 -m venv $PY_APPS_PATH --prompt "."

chown -c -v -h -R docker $PY_APPS_PATH
cd $PY_APPS_PATH

echo "User $(whoami), python: $(which python), bin: $(ls ./bin/)"

echo [ "$(python3 -V)" ] | [ "$(pip -V)" ]

#  Создаём «requirements.txt»
chmod +x ./create_requirements.sh
runuser -u docker -- sh -c ./create_requirements.sh

#  Устанавливаем в  «pip» пакеты из «requirements.txt»
#  --dry-run \	#	ONLY CHECK
chown -v -h -R $USER_EXECUTOR $PY_APPS_PATH
chmod +x "${PY_APPS_PATH}/bin/activate"
cd $PY_APPS_PATH

source "${PY_APPS_PATH}/bin/activate" && \
runuser -u docker -- pip install --requirement ./requirements.txt

#vPY_APPS_PATH="/home/docker/docker_app/docker_supervisord/py_apps/" && source "${PY_APPS_PATH}/bin/activate"

#  Создаём «requirements.txt»
chmod +x ./shutdown_supervisord.sh
./shutdown_supervisord.sh

#  Run supervisord
supervisord --user="${USER_EXECUTOR}" --directory="${PY_APPS_PATH}" --configuration="${PY_APPS_PATH}/supervisord.conf"
