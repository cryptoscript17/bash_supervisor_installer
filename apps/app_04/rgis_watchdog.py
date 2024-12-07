#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import time
from datetime import datetime
import math

from rgis_requests import (return_datetime_string,
													select_device_ids,
													insert_device_sensors_data,
													over_pdk_data_json_to_telegram_text)

chat_ids = [606301502, 873769825]

while True:
	begin = time.time()
	msg = ''
	device_ids = select_device_ids()
	msg += f"⌛ Сканируем [{len(device_ids)}] постов..\n"
	msg += f"💥 Данные о превышениях ПДКсс из публички РГИС:\n"
	msg += f"{return_datetime_string()}\n"

	fpath = insert_device_sensors_data(chat_ids, do_send=False)

	if os.path.exists(fpath):
			msg += over_pdk_data_json_to_telegram_text(fpath)
	else:
			msg = 'Not found. Wait 5 minutes and try again...'

	runtime = time.time() - begin
	msg += f"\n---- Выполнено за {runtime} сек. ---\n"

	for chat_id in chat_ids:
		telegram_send_message = f"https://api.telegram.org/bot5958541467:AAEZbniR2C5hvf8T_iCleZzJEdVaK8r904g/sendMessage?chat_id={chat_id}&text={msg}"
		response = requests.get(telegram_send_message)

	if runtime <= 0:
			runtime = 3600
	if runtime > 3600:
			print(f"{datetime.fromisoformat(datetime.fromtimestamp(time.time()).isoformat()).strftime('%Y-%m-%d %H:%M:%S')}\tRUNTIME [{runtime}]!!!\tSkipping because runtime > 1200 seconds...!!!!!\n")
			continue
	time.sleep(math.floor(3600 - runtime))