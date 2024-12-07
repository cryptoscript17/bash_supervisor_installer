import os, json, time, requests, math
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2 import pool

def get_timestamps():
    bdate = (math.floor(time.time() / 1200) - 1) * 1200
    edate = (math.ceil(time.time() / 1200) - 1) * 1200
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
    if filename.exists():
        return True
    return False

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

def construct_query(responses_total):
    table = 'iot.iot'
    url = "'http://10.14.16.41:8011?device_id=all&bdate=bdate&edate=edate'"
    (bdate, edate) = get_timestamps()
    url = url.replace('=bdate','='+str(bdate)).replace('=edate', '='+str(edate))
    sensors_string = "'{CO,NO2,SO2,O3,OPC,NH3,H2S,NO,NOx}'"
    fields_string = "'{c_ppb,cmg,PM25,PM10,windSpeed,windVane,c1_ppb,t_ambiant,p_ambiant,rh_ambiant}'"
    query = f"""INSERT INTO {table} (url, device_id, sensors, fields, response_data) VALUES """
    for i, task_response in enumerate(responses_total):
        device_id = chr(39)+task_response['DeviceId']+chr(39)
        #url = url.replace('=vendor_id', '='+task_response['DeviceId']).replace('AN79', '')
        data = (url, device_id, sensors_string, fields_string, chr(39)+json.dumps(task_response)+chr(39))
        #query = f"""INSERT INTO {table} (url, device_id, sensors, fields, response_data) VALUES (%s, %s, %s, %s, %s);""" % data
        if i < len(responses_total) - 1:
            query += f"""(%s, %s, %s, %s, %s),""" % data
        else:
            query += f"""(%s, %s, %s, %s, %s);""" % data
    return query



def get_semos_samples(headers=None):
    url = 'http://10.14.16.41:8011?device_id=all&bdate=bdate&edate=edate'
    (bdate, edate) = get_timestamps()
    #print('bdate = ', bdate, 'edate = ', edate)
    url = url.replace('=bdate','='+str(bdate)).replace('=edate', '='+str(edate))
    print(url, '\n')
    response = requests.get(url, headers=headers, timeout=(60, 3600))
    if response.status_code == 200:
        result = response.text
    else:
        result = {}
    return result

def get_json_samples_from_file(json_path):
    with open(json_path, newline='', encoding='utf-8') as json_file:
        #print(json_file.readlines())
        samples_json = []
        json_text = ''.join(x for x in json_file.readlines())
        json_partial = json_text.split('}\r\r\n{')
        for i, json_part in enumerate(json_partial):
            #json_part = json.loads(json_part)
            if i < 1:
                json_part += '}'
            elif i == len(json_partial) - 1:
                json_part = '{' + json_part
            else:
                json_part = '{' + json_part + '}'
                #print(json_part)
            json_part = json.loads(json_part)
            if json_part['deviceId'] in [317, 318, 319, 320]:
                #print(json_part['deviceId'])
                samples_json.append(json_part)
        return samples_json

def timestamp_to_rnox_time(ts):
    return str(datetime.fromisoformat(datetime.fromtimestamp(ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S') + '+03:00')

def convert_samles_to_rnox_json(samples_json):
    results = []
    for j, sample_json in enumerate(samples_json):
        if j < 5:
            device_id = 'AN79'+str(sample_json['deviceId'])
            sample_arr = []
            gas_sensors = {}
            meteo_sensors = []
            OPC = []
            for sensor_json in sample_json['Data']:
                if len(sensor_json['Results']) > 0:
                    sensor_name = sensor_json['sensor']
                    sensor_bdate = timestamp_to_rnox_time(sensor_json['Results'][0]['bdate'])
                    sensor_edate = timestamp_to_rnox_time(sensor_json['Results'][0]['edate'])
                    sensor_value = sensor_json['Results'][0]['value']
                    H2S = []
                    SO2 = []
                    CO = []
                    NH3 = []
                    NO2 = []
                    NO = []
                    NOx = []

                    if [device_id, sensor_name, sensor_bdate, sensor_edate, sensor_value] not in sample_arr:
                        sample_arr.append([device_id, sensor_name, sensor_bdate, sensor_edate, sensor_value])
                        if sensor_name in ['Temp_Out', 'P_Atm', 'RH_AMBIANT', 'WINDSPEED', 'WINDVANE']:
                            if sensor_name == 'Temp_Out':
                                sensor_name = 'T_AMBIANT'
                                meteo_sensors.append({'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': sensor_name,'Data': str(sensor_value)})
                            elif sensor_name == 'P_Atm':
                                sensor_name = 'P_AMBIANT'
                                meteo_sensors.append({'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': sensor_name,'Data': str(round(sensor_value / 7.5006156, 1))})
                            elif sensor_name == 'RH_AMBIANT':
                                sensor_name = 'RH_AMBIANT'
                                meteo_sensors.append({'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': sensor_name,'Data': str(sensor_value)})
                            elif sensor_name == 'WINDSPEED':
                                sensor_name = 'WINDSPEED'
                                meteo_sensors.append({'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': sensor_name,'Data': str(sensor_value)})
                            elif sensor_name == 'WINDVANE':
                                sensor_name = 'WINDVANE'
                                meteo_sensors.append({'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': sensor_name,'Data': str(sensor_value)})
                            else:
                                pass
                        elif sensor_name in ['H2S', 'SO2', 'CO', 'NH3', 'NO2', 'NO', 'NOx', 'PM25', 'PM10']:
                            if sensor_name == 'H2S':
                                H2S = {'H2S': [{'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': 'C1_PPB','Data': str(sensor_value*1000)}]}
                                gas_sensors.update(H2S)
                            elif sensor_name == 'SO2':
                                SO2 = {'SO2': [{'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': 'C1_PPB','Data': str(sensor_value*1000)}]}
                                gas_sensors.update(SO2)
                            elif sensor_name == 'CO':
                                CO = {'CO': [{'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': 'C1_PPB','Data': str(sensor_value*1000)}]}
                                gas_sensors.update(CO)
                            elif sensor_name == 'NH3':
                                NH3 = {'NH3': [{'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': 'C1_PPB','Data': str(sensor_value*1000)}]}
                                gas_sensors.update(NH3)
                            elif sensor_name == 'NO2':
                                NO2 = {'NO2': [{'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': 'C1_PPB','Data': str(sensor_value*1000)}]}
                                gas_sensors.update(NO2)
                            elif sensor_name == 'NO':
                                NO = {'NO': [{'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': 'C1_PPB','Data': str(sensor_value*1000)}]}
                                gas_sensors.update(NO)
                            elif sensor_name == 'NOx':
                                NOx = {'NOx': [{'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': 'C1_PPB','Data': str(sensor_value*1000)}]}
                                gas_sensors.update(NOx)
                            elif sensor_name in ['PM25', 'PM10']:
                                OPC.append({'SampleStartTime': sensor_bdate, 'SampleEndTime': sensor_edate, 'Field': sensor_name,'Data': str(sensor_value*1000)})
                            else:
                                pass
                        else:
                            pass
                    if len(OPC) > 0:
                        gas_sensors.update({'OPC': OPC})
            if len(meteo_sensors) > 0 and len(gas_sensors) > 0:
                results.append({'DeviceId': device_id, 'Devices': meteo_sensors, 'Sensors': gas_sensors})
            #print({'DeviceId': device_id, 'Devices': meteo_sensors, 'Sensors': gas_sensors})
    return results

while True:
    start_time = int(time.time())
    this_script_path = os.path.abspath(os.path.realpath(__file__))
    workdir = os.path.dirname(this_script_path)
    #json_fname = '10.14.16.41.json'
    #json_path = os.path.join(workdir, json_fname)
    (bdate, edate) = get_timestamps()
    print('bdate = ', bdate, 'edate = ', edate, '\r\n')
    sample_json = get_semos_samples()
    filename_path = os.path.join(workdir, str(datetime.fromisoformat(datetime.fromtimestamp(bdate).isoformat()).strftime('%Y_%m_%d_%H_%M_')) + '_semos_sample_raw.txt')
    if save_string_to_file(filename_path, sample_json):
        semos_samples_json = get_json_samples_from_file(filename_path)
        if len(semos_samples_json) > 0:
            rnox_samples_json = convert_samles_to_rnox_json(semos_samples_json)
            filename_path = filename_path.replace('_sample_raw', '_sample_rnox')
            save_string_to_file(filename_path, '\n'.join(json.dumps(x) for x in rnox_samples_json))
            query = construct_query(rnox_samples_json)
            print(query, '\n')
            run_parallel_insert(query)
            #print(rnox_samples_json)
    runtime = int(time.time() - start_time)
    print('runtime = ', runtime, '\r\n')
    time.sleep(math.floor(1200 - runtime))
    if runtime >= 1200: 
        continue