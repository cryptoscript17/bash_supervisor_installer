import os
import json
import time
import requests
import math
from datetime import datetime, timedelta
import psycopg2
# from psycopg2 import pool
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
samples_path = os.path.join(workdir, 'samples')

def debug_func():
  fpath = 'C:\\_python_services\\semos_parser\\2024_10_17_16_14_34_semos_sample.txt'
  with open(fpath, 'r') as fp:
    samples_json = json.load(fp)
    rnox_samples_json = convert_samles_to_rnox_json(samples_json)
    for sample_json in rnox_samples_json:
      print(sample_json['DeviceId'])
  quit()


def timestamp_to_rnox_time(ts):
  return str(datetime.fromisoformat(datetime.fromtimestamp(ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S') + '+03:00')


def get_timestamps():
  bdate = (math.floor(time.time() / 1200) - 1) * 1200
  edate = (math.ceil(time.time() / 1200) - 1) * 1200
  return (bdate, edate)


def get_url():
  bdate, edate = get_timestamps()
  url = f"http://10.14.16.41:8011?device_id=all&bdate={bdate}&edate={edate}"
  # url = url.replace('=bdate','='+str(bdate)).replace('=edate', '='+str(edate))
  return url


def parse_semos_raw_response(text):
  try:
    text = text.replace('}\r\n{','},\n{')
    samples = []
    samples_json = json.loads(f'[{text}]')
    for sample in samples_json:
      if sample['deviceId'] not in [290, 291, 391, 392]:    #   Mob.lab.ids
        samples.append(sample)
    return samples
  except Exception as error:
    logging.error(f'Sample parse and save error: {error}')
    return []


def get_semos_samples(headers=None):
  '''
    Функция запроса API Сэмос для получения
    данных по всем постам "device_id=all"
    за интервалы дат, ближайшие к текущему
    времени, округлённому до безостаточного
    деления времени на 20 минутные интервалы.
  '''
  (bdate, edate) = get_timestamps()
  url = f'http://10.14.16.41:8011?device_id=all&bdate={bdate}&edate={edate}'
  is_full, iter_max, iteration, timeout = False, 4, 0, 250
  while not is_full and iteration <= 3:
    logging.info(f'{is_full} - {iteration}')
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f'[{timestamp}] \t Requesting URl: {url}')
    try:
      response = requests.get(url, headers=headers, timeout=(60, 360))
      if response.status_code == 200:
        samples = parse_semos_raw_response(response.text)
        if len(samples) > 0:
          is_full = True
          for sample in samples:
            if 'Data' in sample:
              if len(sample['Data'][0]['Results']) == 0:      # TempIn
                logging.info(f'Device {sample["deviceId"]} TempIn value EMPTY!')
                is_full = False
                break
      else:
        logging.error(f'ERR. Response error, status_code: {response.status_code}')
    except Exception as e:
      logging.error(f"Exception while trying access to known JSON scheme fields .. ERR: {e}")
    if is_full:
      break
    else:
      iteration += 1
      logging.info(f'Sleeping {timeout} second...')
      time.sleep(timeout)
  if len(samples) > 0:
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    sample_path = os.path.join(samples_path, f'{timestamp}_semos_sample.txt')
    with open(sample_path, "w") as outfile:
      outfile.write(json.dumps(samples, indent=2))
    return samples
  else:
    return []


def convert_samles_to_rnox_json(samples_json):
  results = []
  for j, sample_json in enumerate(samples_json):
    #   if j < 5:
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


def execute_query(samples_json, timeout_duration=None):
  table = 'iot.iot'
  with psycopg2.connect(dbname="iot", user="postgres", password="qAyexo4MEI", host="10.14.126.166", port="5432") as conn:
  # with psycopg2.connect(dbname="iot", user="postgres", password="", host="127.0.0.1", port="65432") as conn:
    url = get_url()
    sensors_string = "{CO,NO2,SO2,O3,OPC,NH3,H2S,NO,NOx}"
    fields_string = "{c_ppb,cmg,PM25,PM10,windSpeed,windVane,c1_ppb,t_ambiant,p_ambiant,rh_ambiant}"
    query_list = []
    for sample_json in samples_json:
      device_id = sample_json['DeviceId']
      data = (url, device_id, sensors_string, fields_string, json.dumps(sample_json))
      query_list.append("('%s', '%s', '%s', '%s', '%s')" % data)
    query_full = ','.join(x for x in query_list)
    query_full =f"INSERT INTO {table} (url, device_id, sensors, fields, response_data) VALUES \n{query_full}"
    try:
      cur = conn.cursor()
      cur.execute(query_full)
      conn.commit()
      return query_full
    except Exception as e:
      print(f"Postgres Execution Error: {e}\nQUERY:\n{query_full}")
    finally:
      if cur:
        cur.close()



while True:
  timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  start_time = time.time()
  samples_json = get_semos_samples()
  if len(samples_json) > 0:
    rnox_samples_json = convert_samles_to_rnox_json(samples_json)
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    sample_path = os.path.join(samples_path, f'{timestamp}_semos_rnox_sample.txt')
    with open(sample_path, "w") as outfile:
      outfile.write(json.dumps(rnox_samples_json, indent=2))
    devices_good = []
    for item in rnox_samples_json:
      devices_good.append(item['DeviceId'][4:len(item['DeviceId'])])
    logging.info(f"Collected devices:\t{devices_good}")
    if False:
      query = construct_query(rnox_samples_json)
      run_parallel_insert(query)
    else:
      query_result = execute_query(rnox_samples_json)
      # print(query_result)
      if isinstance(query_result, str):
        print(f"[{timestamp}] INSERT Succesfull!")
        query_fpath = os.path.join(workdir, '{timestamp}_semos_insert_query.txt')
        with open(query_fpath, "w") as outfile:
          outfile.write(query_result)
  else:
    logging.error(f'Ошибка при проверке полноты данных!')
  runtime = int(time.time() - start_time)
  logging.info(f'{timestamp}   Sample path: {len(samples_json)}, runtime: {runtime} seconds.')
  if runtime >= 1200:
    logging.warning(f'Runtime is too much: {runtime}')
    continue
  else:
    time.sleep(math.floor(1200 - runtime))