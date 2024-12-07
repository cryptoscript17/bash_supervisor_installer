#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import os
import requests
import json
import logging

from datetime import datetime
from datetime import timedelta

import psycopg2
from psycopg2 import Error

logging.basicConfig(level=logging.INFO)

workdir = os.path.dirname(os.path.realpath(__file__))

target_conn_params = {
    'user': 'postgres',
    'password': '',
    'host': '127.0.0.1',
    'port': '65432',
    'database': 'iot'
}


def get_rgis_devices() -> dict:
    url = 'https://rgis.mosreg.ru/v3/ecomonitoring/getPostName'
    headers = {'Accept': 'application/json, text/plain, */*'}
    try:
        response = requests.get(url, headers=headers)
        if response.ok:
            return response.json()
    except Exception as e:
        logging.error(f"get_rgis_devices function error: {e}")
    return None


def get_rgis_device_data(device_id: int) -> dict:
    url = f'https://rgis.mosreg.ru/v3/ecomonitoring/getMainInfo?post_id={device_id}'
    headers = {'Accept': 'application/json, text/plain, */*'}
    try:
        response = requests.get(url, headers=headers)
        if response.ok:
            return response.json()
    except Exception as e:
        logging.error(f"get_rgis_device_data function error: {e}")
    return None


IS_WINDOWS = True
IS_DEV = True


def return_datetime_string(delim=None) -> str:
    current_date_time = datetime.now()
    short_date_time = current_date_time.strftime(f"%Y-%m-%d %H:%M:%S")
    return short_date_time


def execute_query(query, timeout_duration=None):
    """
    Выполняет SQL-запрос к базе данных PostgreSQL с тайм-аутом.

    :param query: SQL-запрос
    :param user: Имя пользователя
    :param password: Пароль
    :param host: Хост базы данных
    :param port: Порт базы данных
    :param database: Имя базы данных
    :param timeout_duration: Максимальное время выполнения запроса в секундах (по умолчанию 900 секунд, т.е., 15 минут)
    :return: Результат выполнения запроса
    """
    if timeout_duration is None:
        timeout_duration = 60

    if IS_DEV:
        user = "postgres"
        password = ""
        host = "127.0.0.1"
        port = "65432"
        database = "iot"
    else:
        user = "postgres"
        password = "qAyexo4MEI"
        host = "10.14.126.166"
        port = "5432"
        database = "iot"

    connection_params = {
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'database': database
    }

    try:
        if IS_WINDOWS:
            def run_insert_query():
                with psycopg2.connect(**connection_params) as conn:
                    # print(f"{return_datetime_string()}\tINFO:\t\tPG_Connection created successfully!")
                    if query.startswith('SELECT'):
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            rows = cursor.fetchall()
                            return rows
                    elif query.startswith('INSERT') or query.startswith('UPDATE'):
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            conn.commit()
                            return True
            return run_insert_query()
        else:
            # Установка тайм-аута на выполнение запроса
            @timeout(timeout_duration)
            def run_insert_query():
                with psycopg2.connect(**connection_params) as conn:
                    # print(f"{return_datetime_string()}\tINFO:\t\tPG_Connection created successfully!")
                    if query.startswith('SELECT'):
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            rows = cursor.fetchall()
                            return rows
                    elif query.startswith('INSERT') or query.startswith('UPDATE'):
                        with conn.cursor() as cursor:
                            query = f"{query[0:len(query) - 1]} ON CONFLICT DO NOTHING;" if query.endswith(
                                ';') else f"{query} ON CONFLICT DO NOTHING;"
                            cursor.execute(query)
                            conn.commit()
                            return True
            return run_insert_query()

    except TimeoutError:
        print(f"{return_datetime_string()}\tERROR:\t\tВремя выполнения запроса превысило {timeout_duration} секунд. Запрос отменен.")
        return None
    except psycopg2.Error as e:
        print(
            f"{return_datetime_string()}\tERROR:\t\tОшибка выполнения запроса: {e}, QUERY:\n{query}")
        return None


def rgis_devices_insert() -> None:
    start_time = time.time()
    devices_tuples_list = []
    devices_json = get_rgis_devices()
    if devices_json is not None:
        for device_json in devices_json:
            devices_tuples_list.append(tuple(device_json.values()))
        try:
            devices_insert_query = "INSERT INTO iot.rgis_devices(name, address, is_published, device_id, lat, lon, card_id) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (device_id) DO NOTHING;"
            # devices_tuples_list = [('Ступино-5', 'Московская область, городской округ Ступино, село Ивановское', False, '1214', 55.073582, 37.843137, 12377670371), ('Воскресенск-4', 'Московская область, городской округ Воскресенск, рабочий посёлок имени Цюрупы', False, '1215', 55.491302, 38.649487, 12656543541)]
            target_conn = psycopg2.connect(**target_conn_params)
            target_cursor = target_conn.cursor()
            target_cursor.executemany(
                devices_insert_query, devices_tuples_list)
            # psycopg2.extras.execute_values(cur, sql, data)
            target_conn.commit()
            print(f"!\t!\t{len(devices_tuples_list)} | S U C C E S S ! ! !")
        except Error as err:
            print(f"Error executing query: {err}")
        finally:
            if target_cursor:
                target_cursor.close()
            if target_conn:
                target_conn.close()
            runtime = round(time.time() - start_time, 2)
        print(f"RUNTIME:\t [{runtime} sec.]")


IS_NEW_DEVICE_MODE = True


def insert_device_sensors():
    device_ids = []

    device_sensors_data = []
    select_device_ids_query = 'SELECT device_id FROM iot.rgis_devices ORDER BY device_id ASC;'
    select_device_ids_result = execute_query(select_device_ids_query, 60)
    for device_id in select_device_ids_result:
        if isinstance(device_id[0], int):
            device_id = device_id[0]
            if device_id not in device_ids:
                device_ids.append(device_id)
    for index, device_id in enumerate(device_ids):
        if index >= 0:
            device_data = get_rgis_device_data(device_id)
            if 'indicators' in device_data:
                device_sensors = []
                for sensor_json in device_data['indicators']:
                    device_sensors.append(
                        (device_id, sensor_json['sensor_id']))
                    # device_sensors_data.append(())
                if IS_NEW_DEVICE_MODE:
                    try:
                        device_sensors_insert_query = 'INSERT INTO iot.rgis_sensors(device_id, paramtype_id) VALUES (%s, %s);'
                        target_conn = psycopg2.connect(**target_conn_params)
                        target_cursor = target_conn.cursor()
                        target_cursor.executemany(
                            device_sensors_insert_query, device_sensors)
                        # psycopg2.extras.execute_values(cur, sql, data)
                        target_conn.commit()
                        print(
                            f"!\t!\t{len(device_sensors)} | INSERT device sensors id. S U C C E S S ! ! !")
                    except Error as err:
                        print(f"Error executing query: {err}")
                    finally:
                        if target_cursor:
                            target_cursor.close()
                        if target_conn:
                            target_conn.close()

                device_data_insert_query = 'INSERT INTO iot.rgis_values(sensor_id, value) VALUES (%s, %s);'
    print(device_data)


def insert_pdk_daily():
    pdk_daily_data = [
        (4, 'PM2.5', 0.035),
        (5, 'PM10', 0.06),
        (6, 'CO', 3),
        (8, 'NO', 0.06),
        (9, 'NO2', 0.1),
        (10, 'SO2', 0.05),
        (12, 'H2S', 0.15),
        (13, 'CH2O', 0.01),
        (17, 'NH3', 0.1)]
    try:
        pdk_daily_insert_query = 'INSERT INTO iot.rgis_pdk(paramtype_id, title, pdk) VALUES (%s, %s, %s);'
        target_conn = psycopg2.connect(**target_conn_params)
        target_cursor = target_conn.cursor()
        target_cursor.executemany(pdk_daily_insert_query, pdk_daily_data)
        # psycopg2.extras.execute_values(cur, sql, data)
        target_conn.commit()
        print(
            f"!\t!\t{len(pdk_daily_data)} | INSERT pdk dictionary. S U C C E S S ! ! !")
    except Error as err:
        print(f"Error executing query: {err}")
    finally:
        if target_cursor:
            target_cursor.close()
        if target_conn:
            target_conn.close()


def get_rgis_date():
    now = datetime.now()
    if now.hour < 16:
        return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        return datetime.today().strftime("%Y-%m-%d")


def select_pdk_by_paramtype_id(paramtype_id) -> tuple:
    pdk_select_query = f"SELECT title, pdk FROM iot.rgis_pdk WHERE paramtype_id = {paramtype_id};"
    pdk_select_result = execute_query(pdk_select_query)[0]
    return pdk_select_result


def analyze_rgis_device_response_json(device_id, device_data) -> list:
    device_sensors_over_pdk_json = []
    device_name = device_data['title']
    device_address = device_data['adress']
    for sensor_json in device_data['indicators']:
        paramtype_id = sensor_json['sensor_id']
        sensor_value = float(sensor_json['current'])
        sensor_pdk = float(sensor_json['standard'])
        sensor_name = sensor_json['title']
        if sensor_value > sensor_pdk:
            pdk_relation = round(sensor_value / sensor_pdk, 4)
            device_sensors_over_pdk_json.append({
                'device_id': device_id,
                'device_name': device_name,
                'paramtype_id': paramtype_id,
                'sensor_name': sensor_name,
                'sensor_value': sensor_value,
                'sensor_pdk': sensor_pdk,
                'pdk_relation': pdk_relation,
                'device_address': device_address,
            })
        else:
            pdk_relation = -1
    if len(device_sensors_over_pdk_json) > 0:
        return device_sensors_over_pdk_json
    else:
        return None


def return_datetime_string(delim=None) -> str:
    current_date_time = datetime.now()
    short_date_time = current_date_time.strftime(f"%Y-%m-%d %H:%M:%S")
    return short_date_time


def get_file_minutes_age(fpath):
    file_modified = os.stat(fpath).st_mtime
    current_time = time.time()
    elapsed_seconds = current_time - file_modified
    minutes = elapsed_seconds / 60
    return minutes


def check_file_age(fpath):
    file_modified = os.stat(fpath).st_mtime
    current_time = time.time()
    elapsed_seconds = current_time - file_modified
    minutes = elapsed_seconds / 60
    if minutes > 10:
        return True
    else:
        return False


def check_operation_in_progress():
    status_file_path = os.path.join(workdir, 'operation_status.txt')
    if os.path.exists(status_file_path):
        status_file_age = check_file_age(status_file_path)
        if status_file_age:
            return False
        else:
            os.remove(status_file_path)
            return True
    else:
        with open(status_file_path, "w") as outfile:
            outfile.write('+')
        return False


def select_device_ids() -> list:
    device_ids = []
    select_device_ids_query = 'SELECT device_id FROM iot.rgis_devices ORDER BY device_id ASC;'
    try:
        select_device_ids_result = execute_query(select_device_ids_query, 60)
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    for device_id in select_device_ids_result:
        if isinstance(device_id[0], int):
            device_id = device_id[0]
            if device_id not in device_ids:
                device_ids.append(device_id)
    if len(device_ids) > 0:
        return device_ids
    else:
        return None


def insert_device_sensors_data(chat_ids, do_send=False):
    '''
        Основная функция сбора данных
        с публички РГИС и отправки в Телеграм
    '''
    start_time = time.time()
    devices_response_ok, devices_response_bad = [], []
    devices_with_over_pdk_list, devices_under_pdk_list, over_pdk_data_json = [], [], []
    device_data_analyze_result = []

    device_ids = select_device_ids()
    if device_ids is not None:
        for index, device_id in enumerate(device_ids):
            if index > 0:
                device_sensors_duplicates = 0
                device_data = get_rgis_device_data(device_id)
                if device_data is not None:
                    if 'indicators' in device_data:
                        if len(device_data['indicators']) > 0:
                            analyze_result = analyze_rgis_device_response_json(
                                device_id, device_data)
                            if analyze_result is not None:
                                for item in analyze_result:
                                    if item not in device_data_analyze_result:
                                        # device_data_analyze_result.append(item)
                                        over_pdk_data_json.append(item)

                                devices_with_over_pdk_list.append(device_id)

                                #   INSERT TO DB
                                device_data_insert_query = "INSERT INTO iot.rgis_daily(response_json) VALUES (%s);" % f"'{json.dumps(device_data)}'"
                                device_data_insert_result = execute_query(
                                    device_data_insert_query, 60)
                                # print(device_data_insert_result)
                                if device_data_insert_result:
                                    logging.info(
                                        f"[{index} from {len(device_ids)}]\t\n!!\t\t | RECEIVED device [{device_id}] sensors count [{len(device_data['indicators'])}] values. S U C C E S S ! ! !")
                                else:
                                    logging.error(
                                        f"[{index} from {len(device_ids)}]\t\n!!\t\t | ERROR RECEIVING device [{device_id}] sensors count [{len(device_data['indicators'])}] values. E R R O R ! ! !")
                            else:
                                devices_under_pdk_list.append(device_id)
                        else:
                            logging.error(
                                f"[{index} from {len(device_ids)}]\tdevice_id [{device_id}] Sensors data empty, skip... ")
                    devices_response_ok.append(device_id)
                    if 'indicators' in device_data:
                        logging.info(
                            f"[{index} from {len(device_ids)}]\t\n!!\t\t | CHECK device [{device_id}] sensors count [{len(device_data['indicators'])}] values. N O R M A L ! ! !")
                    else:
                        logging.info(
                            f"[{index} from {len(device_ids)}]\t\n!!\t\t | CHECK device [{device_id}] sensors count [None] values. N O R M A L ! ! !")
                else:
                    devices_response_bad.append(device_id)
        logging.info(
            f"\nBad device responses [{len(devices_response_bad)}]: {devices_response_bad}\nGood device responses [{len(devices_response_ok)}]: {devices_response_ok}\n")
        logging.info(
            f"\nDevices with over PDK values count: [{len(devices_with_over_pdk_list)}]: \nNormal sensor values devices count [{len(devices_under_pdk_list)}]\n")

        msg = f"\nBad device responses [{len(devices_response_bad)}]: {devices_response_bad}\nGood device responses [{len(devices_response_ok)}]\n"
        msg += f"\nDevices with over PDK values count: [{len(devices_with_over_pdk_list)}]: \nNormal sensor values devices count [{len(devices_under_pdk_list)}]\n"

        if do_send:
            for chat_id in chat_ids:
                telegram_send_message = f"https://api.telegram.org/bot5958541467:AAEZbniR2C5hvf8T_iCleZzJEdVaK8r904g/sendMessage?chat_id={chat_id}&text={msg}"
                response = requests.get(telegram_send_message)

        over_pdk_data_json_path = os.path.join(
            workdir, '_over_pdk_data_json.txt')
        if len(over_pdk_data_json) > 0:
            with open(over_pdk_data_json_path, "w") as outfile:
                outfile.write(json.dumps(over_pdk_data_json, indent=2))
    else:
        logging.error("DB ERROR get device_ids from table error!")
    runtime = int(round(time.time() - start_time, 0))
    logging.info(f"RUNTIME: [{runtime}] sec.")
    return over_pdk_data_json_path


def over_pdk_data_json_to_telegram_text(over_pdk_data_json_path):
    msg = ''
    with open(over_pdk_data_json_path, newline='', encoding='utf-8') as json_file:
        json_text = ''.join(x for x in json_file.readlines())
        json_data = json.loads(json_text)
    for sensor_json in json_data:
        msg += f"{sensor_json['device_id']} ⋮ {sensor_json['device_name']} ⋮ {sensor_json['sensor_name']} ⋮ {round(sensor_json['pdk_relation'], 2)}\n"
    return msg


if __name__ == '__main__':
    over_pdk_data_json_path = os.path.join(workdir, '_over_pdk_data_json.txt')
    print(over_pdk_data_json_to_telegram_text(over_pdk_data_json_path))
    quit()
    # rgis_devices_insert()
    if not check_operation_in_progress():
        insert_device_sensors_data()
    else:
        print('Wait 5 minutes and try again...')
    # print(get_rgis_date())
