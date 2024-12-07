#!/usr/bin/bash

USER_EXECUTOR="$(whoami)"
TARGET_PATH="/usr/local"
VENV_NAME="supervisord"

echo "User $(whoami), python: $(python -V)"

cd $TARGET_PATH

#  Creating «requirements.txt»
cat > ./requirements.txt <<EOF
PyYAML==6.0.2
supervisor==4.2.5
asyncio==3.4.3
aiohttp==3.8.6
aiogram==2.25.1
psycopg2-binary==2.9.10
asyncpg==0.30.0
pandas==1.5.0
requests==2.27.1
timeout-decorator==0.5.0
pathlib2==2.3.7.post1
EOF

#  Устанавливаем в  «pip» пакеты из «requirements.txt»
echo "Python virtual environment «${VENV_NAME}» creating in «${TARGET_PATH}».."
python -m venv $TARGET_PATH --prompt "."
cd "${TARGET_PATH}" && \
chmod +x ./bin/activate && \
source ./bin/activate
pip install -r requirements.txt

#  Run supervisord
#  supervisord --user="$(whoami)" --directory="${TARGET_PATH}" --configuration="${TARGET_PATH}/supervisord.conf"
