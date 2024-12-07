# -*- coding: utf-8 -*-
import os
import json
import time
import requests
import math
import glob
import psycopg2
from timeout_decorator import timeout, TimeoutError
from datetime import datetime
from pathlib import Path
import pandas as pd

IS_DEV = False
IS_WINDOWS = True
workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))

def return_datetime_string(delim = None) -> str:
    current_date_time = datetime.now()
    short_date_time = current_date_time.strftime(f"%Y-%m-%d %H:%M:%S")
    return short_date_time


def convert_date_to_unix(df, col_name):
    df[col_name] = pd.to_datetime(df[col_name])
    df[col_name] = df[col_name].apply(lambda x: int(x.timestamp()))
    return df

def date_to_unix(date):
    dt = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    return int((dt - datetime(1970, 1, 1)).total_seconds())

def timestamp_to_rnox_datetime(ts):
    ts = int(ts / 1200) * 1200
    return str(datetime.fromisoformat(datetime.fromtimestamp(ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S')) + '+03:00'

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def get_bdate_edate():
    start = time.time()
    bdate_ts = int((math.floor(start / 1200) - 1) * 1200)
    edate_ts = int((math.ceil(start / 1200) - 1) * 1200)
    bdate = str(datetime.fromisoformat(datetime.fromtimestamp(bdate_ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S'))
    edate = str(datetime.fromisoformat(datetime.fromtimestamp(edate_ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S'))
    return (bdate, edate)

def save_string_to_file(filename, text):
    try:
        file = open(filename, "w", encoding='utf-8')
        file.write(text)
        file.close
    finally:
        try:
            my_file = Path(filename)
            my_abs_path = Path(filename).resolve(strict=True)
        except FileNotFoundError:
            return False
        else:
            return True


def request_device_samples():
    stime = time.time()
    timeout = 300
    (bdate, edate) = get_bdate_edate()

    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    dataset_fname = str(bdate).replace('-', '_').replace(':', '_') + '_odintsovo_sample_raw.txt'
    dataset_path = os.path.join(workdir, dataset_fname)

    url = 'https://moecom.poligon-odintsovo.ru'
    header = {'Content-Type':'application/json'}
    body = {"request":"avg20m_ecolab_posts_data"}

    is_response = False
    print('Requesting period is [' + bdate + ' - ' + edate + ']')
    while not is_response:
        response = requests.post(url, headers=header, json=body, timeout=(5,60))
        if response.status_code == 200:
            json_data = response.json()
            if len(json_data) > 0:
                if save_string_to_file(dataset_path, json.dumps(json_data, indent=2)):
                    is_response = True
                    print(f'{return_datetime_string()}\tRequested All and Recieved ' + str(len(json_data)) + ' devices.\n')
                else:
                    print(f'{return_datetime_string()}\tResponse from server and save error!\n')
                    break
            else:
                is_response = False
                local_datasets_csv = glob.glob(os.path.join(workdir, dataset_fname))
                print(f'{return_datetime_string()}\tResponse from is empty!\n')
                print('Sleeeping.....' + str(timeout) + 'sec.\n')
                time.sleep(timeout)
        else:
            is_response = False
            print(f'{return_datetime_string()}\tRequest to server error! Status_code is ' + str(response.status_code) + '\n')
            print('Sleeeping.....' + str(timeout) + 'sec.\n')
            time.sleep(timeout)
    if is_response:
        return json_data
    else:
        print('Request/Response error!\n')

def convert_request_samples(json_data):

    #with open(os.path.join(workdir, 'moecom.polygon-odintsovo.ru'), newline='', encoding='utf-8') as json_file:
    #    json_text = ''.join(x for x in json_file.readlines())
    #    json_data = json.loads(json_text)

    devices_json_new = []
    for i, device_json in enumerate(json_data):
        if i < 4:
            device_json_new = {}
            device_id = device_json['DeviceId']
            if 'first' in device_id:
                device_id = 'AN99186'
            elif 'second' in device_id:
                device_id = 'AN99185'
            elif 'third' in device_id:
                device_id = 'AN99184'
            elif 'fourth' in device_id:
                device_id = 'AN99187'
            else:
                continue
            device_json_new.update({'DeviceId': device_id})

            meteo_json = device_json['Devices']
            meteo_json_new = []
            for meteo_item in meteo_json:
                if meteo_item['Data'] is not None and isfloat(meteo_item['Data']):
                    if meteo_item["Field"] == 'P_AMBIANT':
                        #print(meteo_item["Field"], meteo_item['Data'])
                        meteo_item['Data'] = round(meteo_item['Data'] / 7.5006156, 1)
                    #else:
                        #print(meteo_item["Field"], meteo_item['Data'])
                meteo_item['SampleStartTime'] = timestamp_to_rnox_datetime(date_to_unix(meteo_item['SampleStartTime']))
                meteo_item['SampleEndTime'] = timestamp_to_rnox_datetime(date_to_unix(meteo_item['SampleEndTime']))
                if meteo_item['Data'] is not None and isfloat(meteo_item['Data']):
                    meteo_json_new.append(meteo_item)
                #else:
                #    print(meteo_item['Data'])
            device_json_new.update({'Devices': meteo_json_new})

            sensors_json = device_json['Sensors']
            sensors_json_new = {}
            for sensor_item in sensors_json:
                if sensor_item == 'NO2':
                    k = 1.9085 * 1000
                elif sensor_item == 'NO':
                    k = 1.24478 * 1000
                elif sensor_item == 'NH3':
                    k = 0.62226 * 1000
                elif sensor_item == 'SO2':
                    k = 2.65722 * 1000
                elif sensor_item == 'H2S':
                    k = 1.41386 * 1000
                elif sensor_item == 'CH4':
                    k = 0.66541
                elif sensor_item == 'HCl':
                    k = 1.51254 * 1000
                elif sensor_item == 'O3':
                    k = 1.99116 * 1000
                elif sensor_item == 'CO':
                    k = 1.16197 * 1000
                elif sensor_item == 'CO2':
                    k = 1.82572 * 1000
                elif sensor_item == 'CH2O':
                    k = 1.24577 * 1000
                else:
                    k = 0

                if sensors_json[sensor_item][0]['Data'] is not None and isfloat(sensors_json[sensor_item][0]['Data']):
                    #sensors_json_new.update({sensor_item: [{'SampleStartTime': timestamp_to_rnox_datetime(date_to_unix(sensors_json[sensor_item][0]['SampleStartTime'])), 'SampleEndTime': timestamp_to_rnox_datetime(date_to_unix(sensors_json[sensor_item][0]['SampleEndTime'])), 'Field': 'C1_PPB', 'Data': sensors_json[sensor_item][0]['Data']*k, 'k': k}]})
                    sensors_json_new.update({sensor_item: [{'SampleStartTime': timestamp_to_rnox_datetime(date_to_unix(sensors_json[sensor_item][0]['SampleStartTime'])), 'SampleEndTime': timestamp_to_rnox_datetime(date_to_unix(sensors_json[sensor_item][0]['SampleEndTime'])), 'Field': 'C1_PPB', 'Data': sensors_json[sensor_item][0]['Data']*k}]})

            device_json_new.update({'Sensors': sensors_json_new})
            if len(device_json_new['Devices']) > 0 and len(device_json_new['Sensors']) > 0:
                devices_json_new.append(device_json_new)
    #print(devices_json_new)
    return devices_json_new

def construct_query(responses_total, sensor_types):
    table = 'iot.iot'
    url = "'https://moecom.polygon-odintsovo.ru'"
    (bdate, edate) = get_bdate_edate()
    url = url.replace('=bdate','='+str(bdate)).replace('=edate', '='+str(edate))
    #sensors_string = "'{CO,NO2,SO2,O3,OPC,NH3,H2S,NO,NOx}'"
    sensors_string = "'{" + ','.join(x for x in sensor_types) + "}'"
    fields_string = "'{c_ppb,cmg,PM25,PM10,windSpeed,windVane,c1_ppb,t_ambiant,p_ambiant,rh_ambiant}'"
    query = f"""INSERT INTO {table} (url, device_id, sensors, fields, response_data) VALUES """
    for i, task_response in enumerate(responses_total):
        device_id = chr(39)+task_response['DeviceId']+chr(39)
        data = (url, device_id, sensors_string, fields_string, chr(39)+json.dumps(task_response)+chr(39))
        if i < len(responses_total) - 1:
            query += f"""(%s, %s, %s, %s, %s),""" % data
        else:
            query += f"""(%s, %s, %s, %s, %s);""" % data
    return query


def execute_query(query, timeout_duration):
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
    timeout_duration=900
    if IS_DEV:
        user="postgres"
        password=""
        host="127.0.0.1"
        port="65432"
        database="iot"
    else:
        user="postgres"
        password="qAyexo4MEI"
        host="10.14.126.166"
        port="5432"
        database="iot"

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
                    print(f"{return_datetime_string()}\tINFO:\t\tPG_Connection created successfully!")
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        conn.commit()
                        print(f"{return_datetime_string()}\tINFO:\t\tPostgreSQL query executed and committed. Closing connection.")
                        return True
                    
            return run_insert_query()
        else:
            # Установка тайм-аута на выполнение запроса
            @timeout(timeout_duration)
            def run_insert_query():
                with psycopg2.connect(**connection_params) as conn:
                    print(f"{return_datetime_string()}\tINFO:\t\tPG_Connection created successfully!")
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        conn.commit()
                        print(f"{return_datetime_string()}\tINFO:\t\tPostgreSQL query executed and committed. Closing connection.")
                        return True

            return run_insert_query()

    except TimeoutError:
        print(f"{return_datetime_string()}\tERROR:\t\tВремя выполнения запроса превысило {timeout_duration} секунд. Запрос отменен.")
        return None

    except psycopg2.Error as e:
        print(f"{return_datetime_string()}\tERROR:\t\tОшибка выполнения запроса: {e}, QUERY:\n{query}")
        return None


while True:
    start_time = int(time.time())
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))

    (bdate, edate) = get_bdate_edate()
    dataset_fname = str(bdate).replace('-', '_').replace(':', '_') + '_odintsovo_sample_converted.txt'

    json_data = request_device_samples()
    sensor_types = []
    for i, device_json in enumerate(json_data):
        for sensor_type in device_json['Sensors']:
            if sensor_type not in sensor_types:
                sensor_types.append(sensor_type)

    if len(json_data) > 0:
        json_result = convert_request_samples(json_data)
        save_string_to_file(os.path.join(workdir, dataset_fname), json.dumps(json_result, indent=2))
        query = construct_query(json_result, sensor_types)
        #run_parallel_insert(query)  #   LEGACY
        # insert_result = 'TEST'
        print(f"QUERY:\n{query}")
        insert_result = execute_query(query, timeout_duration = 900)
    
    runtime = int(time.time() - start_time)
    print(f"{return_datetime_string()}\tINSERT_result: {insert_result}, Runtime: {runtime}")

    if runtime >= 1200:
        print(f"Skiping loop iteration, runtime = [{runtime}]")
        continue
    else:
        time.sleep(math.floor(1200 - runtime))