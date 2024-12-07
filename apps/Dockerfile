FROM python:3.10-slim

ENV TARGET_PATH="/usr/local"

WORKDIR $TARGET_PATH

# ...put your own build instructions here...
COPY ../apps $TARGET_PATH
RUN apt update && apt-get install -y systemctl supervisor python3-venv
#	apt-get install -y systemctl supervisor python3-wheel python3-venv python3-openssl python3-cryptography python3-utf8-locale python3-fastapi python3-flask python3-uvicorn python3-pandas python3-openpyxl

RUN chmod +x ./setup_main.sh && ./setup_main.sh

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# EXPOSE 9002/tcp
# EXPOSE 9003/tcp

ENTRYPOINT ["sh", "-c"]
