FROM python:3.10-slim

ENV TARGET_PATH="/usr/local"

WORKDIR $TARGET_PATH

# ...put your own build instructions here...
COPY ../apps $TARGET_PATH

RUN <<EOF
apt update
apt-get install -y supervisor python3-venv
chmod +x ./setup_main.sh
chmod +x ./entrypoint.sh
ln -s ${TARGET_PATH}/entrypoint.sh /usr/bin/entrypoint.sh
./setup_main.sh
cp "${TARGET_PATH}/supervisord.conf" "/etc/supervisor/conf.d/supervisord.conf"
cp /etc/ssl/private/
apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*	# Clean up APT when done.
EOF


#  RUN apt update && apt-get install -y supervisor python3-venv
#  apt-get install -y systemctl supervisor python3-wheel python3-venv python3-openssl python3-cryptography python3-utf8-locale python3-fastapi python3-flask python3-uvicorn python3-pandas python3-openpyxl
#  RUN chmod +x ./setup_main.sh && chmod +x ./entrypoint.sh && ./setup_main.sh

EXPOSE 9002/tcp

#  ENTRYPOINT ["start", "supervisord", "-c", "supervisord.conf"]
#  ENTRYPOINT ["entrypoint.sh"]
CMD ["/usr/bin/supervisord", "--nodaemon","--configuration", "supervisord.conf"]
