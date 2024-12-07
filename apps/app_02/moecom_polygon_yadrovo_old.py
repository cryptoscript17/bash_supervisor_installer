import os, json, time, requests, math
import glob
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2 import pool

import pandas as pd

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
    dataset_fname = str(bdate).replace('-', '_').replace(':', '_') + '_yadrovo_sample_raw.txt'
    dataset_path = os.path.join(workdir, dataset_fname)

    url = 'https://moecom.polygon-yadrovo.ru'
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
                    print('Requested All and Recieved ' + str(len(json_data)) + ' devices.\n')
                else:
                    print('Response from server and save error!\n')
                    break
            else:
                is_response = False
                local_datasets_csv = glob.glob(os.path.join(workdir, dataset_fname))
                print('Response from is empty!\n')
                print('Sleeeping.....' + str(timeout) + 'sec.\n')
                time.sleep(timeout)
        else:
            is_response = False
            print('Request to server error! Status_code is ' + str(response.status_code) + '\n')
            print('Sleeeping.....' + str(timeout) + 'sec.\n')
            time.sleep(timeout)
    if is_response:
        return json_data
    else:
        print('Request/Response error!\n')

def convert_request_samples(json_data):
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))

    #with open(os.path.join(workdir, 'moecom.polygon-yadrovo.ru'), newline='', encoding='utf-8') as json_file:
    #    json_text = ''.join(x for x in json_file.readlines())
    #    json_data = json.loads(json_text)

    devices_json_new = []
    for i, device_json in enumerate(json_data):
        if i < 4:
            device_json_new = {}
            device_id = device_json['DeviceId']
            if 'first' in device_id:
                device_id = 'AN6901'
            elif 'second' in device_id:
                device_id = 'AN6902'
            elif 'third' in device_id:
                device_id = 'AN6903'
            elif 'fourth' in device_id:
                device_id = 'AN6904'
            else:
                break
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
    url = "'https://moecom.polygon-yadrovo.ru'"
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

def run_parallel_insert(query):
    try:
        #postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user="postgres", password="", host="127.0.0.1", port="65432", database="iot")
        postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user="postgres", password="qAyexo4MEI", host="10.14.126.145", port="65432", database="iot")
        if (postgreSQL_pool):
            print('INFO:   ', 'Connection pool created successfully!')
            ps_connection = postgreSQL_pool.getconn()
            if (ps_connection):
                print('INFO:   ', 'Successfully recived connection from connection pool!')
                ps_cursor = ps_connection.cursor()
                ps_cursor.execute(query)
                ps_connection.commit()
                ps_cursor.close()
                postgreSQL_pool.putconn(ps_connection)
                print('INFO:   ', 'PostgreSQL query executed and committed. Put away connection.')
    except (Exception, psycopg2.DatabaseError) as error:
        print('ERROR:   ', 'Error while connecting to PostgreSQL', error)
    finally:
        # closing database connection.
        if 'postgreSQL_pool' in locals():
            postgreSQL_pool.closeall
        print('INFO:   ', 'PostgreSQL connection pool is closed.')


while True:
    start_time = int(time.time())
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    #json_fname = '10.14.16.41.json'
    #json_path = os.path.join(workdir, json_fname)
    (bdate, edate) = get_bdate_edate()
    dataset_fname = str(bdate).replace('-', '_').replace(':', '_') + '_yadrovo_sample_converted.txt'
    #print('bdate = ', bdate, 'edate = ', edate, '\r\n')
    #sample_json = get_semos_samples()
    #filename_path = os.path.join(workdir, str(datetime.fromisoformat(datetime.fromtimestamp(bdate).isoformat()).strftime('%Y_%m_%d_%H_%M_')) + '_semos_sample_raw.txt')
    #if save_string_to_file(filename_path, sample_json):
    #filename_path = os.path.join(workdir, '2023_03_16_11_20_yadrovo_sample_raw.txt')

    #semos_samples_json = get_json_samples_from_file(filename_path)
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
        #print(query, '\n')
        #quit()
        run_parallel_insert(query)
        #print(rnox_samples_json)
    runtime = int(time.time() - start_time)
    print('runtime = ', runtime, '\r\n')
    time.sleep(math.floor(1200 - runtime))
    if runtime >= 1200: 
        continue