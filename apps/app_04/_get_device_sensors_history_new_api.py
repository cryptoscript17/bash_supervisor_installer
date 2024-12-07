#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import json
import time
import requests
import math
import pandas as pd
import shutil
import logging
import glob

from datetime import datetime
from functools import reduce

logging.basicConfig(level=logging.INFO)

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
# workdir =

moem_messages_path = os.path.join(workdir, 'moem_messages')

if not os.path.exists(moem_messages_path):
    os.makedirs(moem_messages_path)


def difference_of_lists(list1, list2):
    return list(set(list1) - set(list2))


def dt_differ_samples_count(bdate, edate):
    bdate_ts = time.mktime(datetime.strptime(bdate, "%Y-%m-%dT%H:%M:%S").timetuple())
    edate_ts = time.mktime(datetime.strptime(edate, "%Y-%m-%dT%H:%M:%S").timetuple())
    samples_count = int((edate_ts - bdate_ts) / 1200)
    #print(int((edate_ts - bdate_ts) / 1200))
    return samples_count


def gis_get_history(device_id, paramtype_ids, bdate = None, edate = None):
    '''
        Получение данных поста с разбивкой на кусочки 1000 записей
        для получения полной истории за период. Результат таблица.
    '''

    def get_bdate_edate():
        start = time.time()
        bdate_ts = int((math.floor(start / 1200) - 1) * 1200)
        edate_ts = int((math.ceil(start / 1200) - 1) * 1200)
        bdate = str(datetime.fromisoformat(datetime.fromtimestamp(bdate_ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S'))
        edate = str(datetime.fromisoformat(datetime.fromtimestamp(edate_ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S'))
        return (bdate, edate)

    def get_ecomon_token():
        token = None
        url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/createSession'
        headers = {'Content-Type':'application/json', 'Origin': 'https://dev.ecomon.mosreg.ru'}
        body = {"jsonrpc":"2.0","method":"createSession","params":{"_config_login":"default@air.ru","_config_password":"1"},"id":1}
        response = requests.post(url, headers=headers, json=body, timeout=(5,60))
        if response.ok:
            try:
                token = response.json()['result']['token']
            except Exception as e:
                print(f"Response JSON error: {e}")
        return token

    def split_datetime_period(bdate, edate):
        from datetime import datetime, timedelta
        import math
        # bdate = '2022-01-01T00:00:00'
        # edate = '2024-01-01T00:00:00'

        chunk_size, daily_samples, sample_size = 936, 72, 1200
        bdate_obj = datetime.strptime(bdate, '%Y-%m-%dT%H:%M:%S')
        edate_obj = datetime.strptime(edate, '%Y-%m-%dT%H:%M:%S')

        delta_samples = math.ceil((edate_obj - bdate_obj).total_seconds() / sample_size)
        chunk_size_days = math.floor(chunk_size / daily_samples)
        iterations = math.ceil(delta_samples / (daily_samples * chunk_size_days))

        print(f"samples: {delta_samples}, days: {delta_samples/daily_samples}, chunk_size_days: {chunk_size_days}, iters: {iterations}")

        ddates = []

        for i in range(0, iterations):
            if i == 0:
                mdate_obj = bdate_obj
            ddates.append((i+1, mdate_obj.strftime('%Y-%m-%dT%H:%M:%S'), (mdate_obj + timedelta(days=chunk_size_days)).strftime('%Y-%m-%dT%H:%M:%S')))
            mdate_obj += timedelta(days=chunk_size_days)

        ddatestr = '\n'.join(str(x) for x in ddates)
        # print(ddatestr)
        return ddates

    def request_device_history_period(device_id, paramtype_ids, iteration, bdate, edate):
        stime = time.time()

        rounded_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
        dtnow = rounded_dt.strftime('%Y-%m-%d %H:%M:%S')
        for sym in ['-', ' ', ':']:
            dtnow = dtnow.replace(sym, '_')
        device_temp_path = os.path.join(workdir, f"{dtnow}_{device_id}")

        url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'

        token = get_ecomon_token()
        if token is None:
            print(f"Token request error!! Exit...")
            return
        else:
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}',
                'Origin': 'https://dev.ecomon.mosreg.ru'
            }

        #is_response, is_offset = False, True
        offset, limit = 0, 1000

        keys_to_keep = ["bdate", "paramtype_code", "sensorvalue", "windspd", "winddir", "tempvalue",  "pressure"]
        clear_json = []
        body = {
            "jsonrpc": "2.0",
            "method": "getData",
            "params": {
                "device_ids": [device_id],
                "paramtype_ids": paramtype_ids,
                "bdate": bdate,
                "edate": edate,
                "is_over_pdk": "false",
                "_config_serverside": "true",
                "_config_is_count": "false",
                "orderby": [
                    {
                        "selector": "bdate",
                        "desc": "true"
                    }
                ],
                "_config_dataset": "BASE.DSAIR_SENSORVALUE",
                "limit": limit,
                "offset": 0,
            },
            "id": 1
        }

        timeout = 30
        response = requests.post(url, headers=headers, json=body, timeout=(5, timeout))
        if response.status_code == 200:
            json_data = response.json()
            # print(f"JSON: {json_data}")
            try:
                total_count = len(json_data['result']['data'])

                if total_count == 0:
                    # empty response JSON
                    pass

                for item in json_data['result']['data']:
                    for key in list(item.keys()):
                        if key not in keys_to_keep:
                            del item[key]
                    clear_json.append(item)

                dataset_path = os.path.join(device_temp_path, f"{dtnow}_offset_{str(iteration)}_device_{str(device_id)}_history_json.txt")
                with open(dataset_path, "w") as outfile:
                    outfile.write(json.dumps(clear_json, indent=2))
                print(f'totalCount = {total_count}, dataset_path = {dataset_path}')

            except Exception as e:
                total_count = 0
                print(f"Response JSON parse err: {e}")
        else:
            print(f'Request to device_id [{device_id}] error! Status_code is [{response.status_code}]\nSleeeping.....{timeout} sec.\n')
            time.sleep(timeout)
        return device_temp_path

    def get_device_start_date(device_id, bdate:str = '2000-01-01T00:00:00'):
        '''
            Получение даты начала сбора данных постом 'device_id'
        '''
        url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'
        token = get_ecomon_token()
        if token is None:
            print(f"Token request error!! Exit...")
            return
        else:
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}',
                'Origin': 'https://dev.ecomon.mosreg.ru'
            }

        body = {
            "jsonrpc":"2.0",
            "method":"getData",
            "params":{
                "device_ids":[device_id],
                "bdate":bdate,
                "is_over_pdk":"false",
                "_config_serverside":"true",
                "_config_is_count":"false",
                "orderby":[
                    {
                        "selector":"bdate",
                        "asc":"true"
                    }
                ],
                "_config_dataset":"BASE.DSAIR_SENSORVALUE",
                "limit":1000,
                "offset":0
            },
            "id":1
        }

        response = requests.post(url, headers=headers, json=body, timeout=(5,60))

        if response.ok:
            json_data = response.json()
            try:
                device_start_date = json_data['result']['data'][0]['bdate']
            except Exception as e:
                device_start_date = bdate
                print(f"Response JSON parsing err: {e}")
        else:
            print(f"Response error status_code: {e}")

        if True:
            body['params']['_config_is_count'] = 'true'

        response = requests.post(url, headers=headers, json=body, timeout=(5,60))
        if response.ok:
            json_data = response.json()
            try:
                total_count = math.ceil(int(json_data['result']['data']['totalcount']) / 1000)
            except Exception as e:
                total_count = 0
                print(f"Response JSON parsing err: {e}")
        else:
            print(f"Response error status_code: {e}")
        print(f"JSON: {json_data['result']['data']}, total_count: [{total_count}x1000] chunks..")

        return (device_start_date, total_count)


    def request_device_history_by_chunk(device_id, paramtype_ids, bdate, edate):
        '''
            Сбор истории через смещение offset
        '''
        timeout = 60
        stime = time.time()
        import math
        samles_count = str(dt_differ_samples_count(bdate, edate))
        iterations = math.ceil(int(samles_count) / 1000)
        rounded_up = -(-int(samles_count) // 1000)

        rounded_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
        dtnow = rounded_dt.strftime('%Y-%m-%d %H:%M:%S')
        for sym in ['-', ' ', ':']:
            dtnow = dtnow.replace(sym, '_')

        device_temp_path = os.path.join(workdir, f"{dtnow}_{device_id}")

        if os.path.exists(device_temp_path):
            shutil.rmtree(device_temp_path)
        os.makedirs(device_temp_path)
        print(f'Working directory is..[{device_temp_path}]..\n')

        print(f'Requested bdate/edate period [{bdate} - {edate}] contains .. [{samles_count}] samples. Iterations: [{iterations}][{rounded_up}]')

        #if edate is None:
        #    (bdate_now, edate) = get_bdate_edate()
        # print('Requesting period is [bdate - edate]')

        rounded_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
        dtnow = rounded_dt.strftime('%Y-%m-%d %H:%M:%S')
        for sym in ['-', ' ', ':']:
            dtnow = dtnow.replace(sym, '_')

        device_temp_path = os.path.join(workdir, f"{dtnow}_{device_id}")

        if os.path.exists(device_temp_path):
            shutil.rmtree(device_temp_path)
        os.makedirs(device_temp_path)
        print(f'Working directory is..[{device_temp_path}]..\n')

        url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'
        token = get_ecomon_token()
        if token is None:
            print(f"Token request error!! Exit...")
            return
        else:
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}',
                'Origin': 'https://dev.ecomon.mosreg.ru'
            }

        #is_response, is_offset = False, True
        offset, limit = 0, 1000

        keys_to_keep = ["bdate", "paramtype_code", "sensorvalue", "windspd", "winddir", "tempvalue",  "pressure"]
        for i in range(0, iterations):
            clear_json = []
            offset = (iterations - i) * 1000
            body = {
                "jsonrpc": "2.0",
                "method": "getData",
                "params": {
                    "device_ids": [device_id],
                    "paramtype_ids": paramtype_ids,
                    "bdate": bdate,
                    "edate": edate,
                    "is_over_pdk": "false",
                    "_config_serverside": "true",
                    "_config_is_count": "false",
                    "orderby": [
                        {
                            "selector": "bdate",
                            "desc": "true"
                        }
                    ],
                    "_config_dataset": "BASE.DSAIR_SENSORVALUE",
                    "limit": limit,
                    "offset": offset,
                },
                "id": 1
            }
            print('Requesting offset ... ' + str(offset) + '\n')
            response = requests.post(url, headers=headers, json=body, timeout=(5,60))

            if response.status_code == 200:
                json_data = response.json()
                # print(f"JSON: {json_data}")
                try:
                    total_count = len(json_data['result']['data'])

                    if total_count == 0:
                        # empty response JSON
                        continue

                    for item in json_data['result']['data']:
                        for key in list(item.keys()):
                            if key not in keys_to_keep:
                                del item[key]
                        clear_json.append(item)

                    dataset_path = os.path.join(device_temp_path, f"{dtnow}_offset_{str(iterations * 1000 - offset)}_device_{str(device_id)}_history_json.txt")
                    with open(dataset_path, "w") as outfile:
                        outfile.write(json.dumps(clear_json, indent=2))
                    print(f'totalCount = {total_count}, dataset_path = {dataset_path}')

                except Exception as e:
                    total_count = 0
                    print(f"Response JSON parse err: {e}")
                #is_response = True
            else:
                print('Request to sensor_id = ' + str(device_id) + ' error! Status_code is ' + str(response.status_code) + '\n')
                print('Sleeeping.....' + str(timeout) + 'sec.\n')
                time.sleep(timeout)
                #is_response = False

            #if is_response:
            #    last_offset = offset
            #    offset += total_count
            #    # print(str(offset))
            #    if last_offset == 0:
            #        return device_temp_path
            #else:
            #    print('Request/Response error!\n')
        print('Runtime is ' + str(int(time.time() - stime)) + 'sec.\n')
        return device_temp_path


    def purify_sample_json_list(json_list, sample_type):
        bad_keys = []
        if sample_type == 'sensor':
            for i, json_item in enumerate(json_list):
                for json_key in json_item.keys():
                    if json_key not in ["bdate", "paramtype_code","sensorvalue"]:
                        if json_key not in bad_keys:
                            bad_keys.append(json_key)
            for i, json_item in enumerate(json_list):
                for key in bad_keys:
                    del json_list[i][key]
        elif sample_type == 'meteo':
            for i, json_item in enumerate(json_list):
                for json_key in json_item.keys():
                    if json_key not in ["bdate", "windspd", "winddir", "tempvalue",  "pressure"]:
                        if json_key not in bad_keys:
                            bad_keys.append(json_key)
            for i, json_item in enumerate(json_list):
                for key in bad_keys:
                    del json_list[i][key]
        return json_list


    def concatenate_chunks_meteo(device_id, temp_dir):
        stime = time.time()
        workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
        fpaths = glob.glob(os.path.join(temp_dir, '*_device_' + str(device_id) + '_history_json.txt'))
        result_json_fname = os.path.join(temp_dir, '_meteo_result_device_' + str(device_id) + '_history_result.txt')
        json_data = []
        for fpath in fpaths:
            with open(fpath, newline='', encoding='utf-8') as json_file:
                json_text = ''.join(x for x in json_file.readlines())
                json_pure = purify_sample_json_list(json.loads(json_text), 'meteo')
                json_data += json_pure
        with open(result_json_fname, "w") as outfile:
            outfile.write(json.dumps(json_data, indent=2))
        print('Meteo Concatenation runtime is ' + str(int(time.time() - stime)) + 'sec.\n')
        return result_json_fname


    def concatenate_chunks_sensors(device_id, temp_dir):
        stime = time.time()
        workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
        fpaths = glob.glob(os.path.join(temp_dir, '*_device_' + str(device_id) + '_history_json.txt'))
        result_json_fname = os.path.join(temp_dir, '_sensor_result_device_' + str(device_id) + '_history_result.txt')
        json_data = []
        for fpath in fpaths:
            with open(fpath, newline='', encoding='utf-8') as json_file:
                json_text = ''.join(x for x in json_file.readlines())
                json_pure = purify_sample_json_list(json.loads(json_text), 'sensor')
                json_data += json_pure
        with open(result_json_fname, "w") as outfile:
            outfile.write(json.dumps(json_data, indent=2))
        print('Sensors Concatenation runtime is ' + str(int(time.time() - stime)) + 'sec.\n')
        return result_json_fname


    def convert_concatenated_chunks_to_excel(sensors_json_fname, meteo_json_fname):
        stime = time.time()
        with open(meteo_json_fname, newline='', encoding='utf-8') as json_file:
            json_text = ''.join(x for x in json_file.readlines())
            json_data = json.loads(json_text)

        meteo_frame = pd.read_json(json.dumps(json_data), orient='records')
        meteo_frame.drop_duplicates(subset=['bdate'], inplace=True)
        meteo_frame.dropna(subset=['tempvalue', 'windspd', 'winddir', 'pressure'], inplace=True)
        meteo_frame = meteo_frame[['bdate', 'tempvalue', 'windspd', 'winddir', 'pressure']]
        meteo_frame.sort_values(by=['bdate'], inplace=True)
        print(meteo_frame.head())

        with open(sensors_json_fname, newline='', encoding='utf-8') as json_file:
            json_text = ''.join(x for x in json_file.readlines())
            json_data = json.loads(json_text)
        sensors_frame = pd.read_json(json.dumps(json_data), orient='records')

        sensors_frame.sort_values(by=['bdate'], inplace=True)
        df_pivot = sensors_frame.pivot_table(index='bdate', columns='paramtype_code', values='sensorvalue', fill_value='').sort_values(by=['bdate'])
        print(df_pivot.head())

        if len(meteo_frame) > 0:
            df_merged = pd.merge(meteo_frame, df_pivot, on='bdate', how='inner').set_index('bdate')
        else:
            df_merged = df_pivot
        df_merged.sort_values(by=['bdate'], inplace=True)

        dataset_xlsx_path = sensors_json_fname.replace('result.txt', '.xlsx')
        xls_writer = pd.ExcelWriter(dataset_xlsx_path)
        df_merged.to_excel(xls_writer, index='bdate', sheet_name='device_history')
        xls_writer.close()
        print('Convertation to XLSX runtime is ' + str(int(time.time() - stime)) + 'sec.\n')
        return dataset_xlsx_path


    def is_folder_not_empty(folder_path):
        # List directory contents
        if os.path.isdir(folder_path):
            if os.listdir(folder_path):  # Returns a list of entries in the directory
                return True  # Folder is not empty
            else:
                return False  # Folder is empty
        else:
            raise ValueError(f"The path '{folder_path}' is not a directory.")

    bdate_real, chunks = get_device_start_date(device_id, bdate)
    # for i in range(0, chunks)
    print(f"Starting to parse ... {(device_id, bdate, bdate_real, chunks)}")
    # quit()
    ddates_list = split_datetime_period(bdate_real, edate)
    # print(f"ddates_list: {ddates_list}")

    # quit()
    #   prepare temp folder for device samples
    rounded_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
    dtnow = rounded_dt.strftime('%Y-%m-%d %H:%M:%S')
    for sym in ['-', ' ', ':']:
        dtnow = dtnow.replace(sym, '_')
    device_temp_path = os.path.join(workdir, f"{dtnow}_{device_id}")
    if os.path.exists(device_temp_path):
        shutil.rmtree(device_temp_path)
    os.makedirs(device_temp_path)
    print(f'Working directory is..[{device_temp_path}]..\n')

    for ddate in ddates_list:
        temp_dir = request_device_history_period(device_id, paramtype_ids, ddate[0], ddate[1], ddate[2])

    # temp_dir = request_device_history_by_chunk(device_id, paramtype_ids, bdate, edate)
    if is_folder_not_empty(temp_dir):
        meteo_json_fname = concatenate_chunks_meteo(device_id, temp_dir)
        sensors_json_fname = concatenate_chunks_sensors(device_id, temp_dir)
        result_xlsx_fname = convert_concatenated_chunks_to_excel(sensors_json_fname, meteo_json_fname)
    else:
        print(f"Device Folder [{temp_dir}] is empty...")
        result_xlsx_fname = None
    return result_xlsx_fname

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
def calc_pdk_rel(df):
  import pandas as pd

  # Define the list of 'param_name' to filter by
  param_list = ['Аммиак (NH3)', 'Сероводород (H2S)', 'Диоксид серы (SO2)', 'Диоксид азота (NO2)', 'Оксид углерода (CO)']

  # Filter rows where 'param_name' is in the param_list
  filtered_df = df[df['param_name'].isin(param_list)].copy()

  df['param_value'] = df['param_value'].str.replace(r'^Значение: ', '', regex=True)
  df['param_value'] = df['param_value'].str.replace(r' мг/м³$', '', regex=True)
  # df['bdate'] = pd.to_datetime(df['bdate'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
  df['bdate'] = pd.to_datetime(df['bdate'], format='%Y-%m-%d %H:%M:%S', errors='coerce')  # Ensure 'bdate' in the main dataframe is datetime
  filtered_df['bdate'] = pd.to_datetime(filtered_df['bdate'], format='%Y-%m-%d %H:%M:%S', errors='coerce')  # Ensure 'bdate' in filtered_df is datetime

  df['param_value'] = pd.to_numeric(df['param_value'], errors='coerce')

  print(filtered_df.head())
  # Iterate through each param_name in the param_list
  for param in param_list:
      # Filter rows for the current param_name
      param_rows = filtered_df[filtered_df['param_name'] == param]

      # Find the rows where 'param_name' starts with 'ПДК:' and contains the current param_name
      #pdk_rows = df[df['param_name'].str.startswith('ПДК:') & df['param_name'].str.contains(param)]
      pdk_rows = df[df['param_name'].str.startswith('ПДК:') & df['param_name'].str.contains(param, regex=False)]
      # Merge the filtered rows with the PDK rows based on 'bdate' (assuming it's the common key)
      merged_df = pd.merge(param_rows, pdk_rows, on='bdate', suffixes=('_param', '_pdk'))
      # print(merged_df.head())
      # Calculate the division of 'param_value' by 'pdk_value' for the rows
      try:
        # Convert 'param_value_param' and 'param_value_pdk' to numeric values, coercing errors to NaN
        merged_df['param_value_param'] = pd.to_numeric(merged_df['param_value_param'], errors='coerce')
        merged_df['param_value_pdk'] = pd.to_numeric(merged_df['param_value_pdk'], errors='coerce')

        # Avoid division by NaN or zero; handle potential errors
        merged_df['param_value_divided'] = merged_df.apply(
            lambda row: row['param_value_param'] / row['param_value_pdk'] if pd.notna(row['param_value_pdk']) and row['param_value_pdk'] != 0 else None,
            axis=1
        )
      except Exception as e:
        print(f"CALC_ERR: {e}")
      # Optionally, you can add this calculated column to the original dataframe
      # (if you want to keep the results in the same DataFrame for further processing)
      df.loc[merged_df.index, 'param_value_divided'] = merged_df['param_value_divided']
  
  # Print the updated dataframe or the result of the merge
  print(df.head())
  quit()

def rgis_history_convert_to_table():
  df = pd.read_excel(os.path.join(workdir, 'rgis_hist.xlsx'), header=None, names=['rgis_id', 'editor', 'bdate', 'param_name', 'action_type', 'param_value'])

  #
  # Drop rows where 'param_name' is in drop_values
  #   df = df[~df['param_name'].isin(drop_values)]
  #   df = df[~df['action_type'].isin(['Добавлено значение'])]
  #   df = df[df['action_type'] != 'Добавлено значение']
  #print(f"\tLines before [{lines_before_hardcoded_drop}], after [{len(df)}], deleted [{lines_before_hardcoded_drop - len(df)}]")

  # Keep only rows where 'param_name' is in white_list_type_raw
  lines_before_whitelist_check = len(df)
  calc_pdk_rel(df)
  if False:
    white_list_type = ['Аммиак (NH3)', 'Сероводород (H2S)', 'Диоксид серы (SO2)', 'Диоксид азота (NO2)', 'Оксид углерода (CO)']
  else:
    white_list_type = ['ПДК: Аммиак (NH3)', 'ПДК: Сероводород (H2S)', 'ПДК: Диоксид серы (SO2)', 'ПДК: Диоксид азота (NO2)', 'ПДК: Оксид углерода (CO)']

  df = df[df['param_name'].isin(white_list_type)]

  df = df[df['action_type'].isin(['Добавлено значение'])]

  df['param_value'] = df['param_value'].str.replace(r'^Значение: ', '', regex=True)
  df['param_value'] = df['param_value'].str.replace(r' мг/м³$', '', regex=True)

  print(f"１\tLines before [{lines_before_whitelist_check}], after [{len(df)}], deleted [{lines_before_whitelist_check - len(df)}]")

  # Convert to given data type
  df['bdate'] = pd.to_datetime(df['bdate'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
  df['param_value'] = pd.to_numeric(df['param_value'], errors='coerce')

  pivot_df = df.pivot_table(
    index=['bdate', 'rgis_id'],          # Index for the pivot table (time-series datetime)
    columns='param_name',                # Column headers for the pivot table
    values='param_value',                # Values to be used from 'param_value' column
    aggfunc='first'                      # Use 'first' for aggregation if multiple values
  )
  pivot_df = pivot_df.sort_values(by=['rgis_id', 'bdate'], ascending=[True, False])
  # Filter to keep only the required columns in the pivot
  # pivot_df = pivot_df[['Аммиак (NH3)', 'Сероводород (H2S)', 'Диоксид серы (SO2)', 'Диоксид азота (NO2)', 'Оксид углерода (CO)']]
  dt = datetime.now().strftime('%Y-%m-%d_%H-%M-%S_')
  dataset_xlsx_path = os.path.join(workdir, f'{dt}pivot_rgis_result.xlsx')
  xls_writer = pd.ExcelWriter(dataset_xlsx_path)
  pivot_df.to_excel(xls_writer, index='bdate', sheet_name='device_history')
  xls_writer.close()
  # print(df.head(500))
  return df

if __name__ == '__main__':
    import os



    rgis_history_convert_to_table()
    quit()
    # print(request_devices_sample_dataset())
    # quit()
    device_id = 136
    paramtype_air_meteo_ids = [1,2,3,14,15]
    paramtype_aqua_ids = [37,57,58,76,77,78]

    paramtype_air_main_ids = [4,5,6,8,9,10,11,12,13,17]
    paramtype_air_more_ids = [7,16,20,21,22,39]

    # paramtype_ids = [7,16,20,21,22,36,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,59,72,73,74,75]
    #

    # bdate = "2023-05-18T08:20:00"
    # edate = "2023-05-18T10:20:00"
    # temp_dir = 'C:\\_python_services\\aiogram_gis_bot\\2022_01_01T00_00_00_temp'


    # device_id = 297
    bdate = "2000-01-01T00:00:00"
    edate = "2024-08-22T00:00:00"

    res = gis_get_history(device_id, paramtype_air_meteo_ids+paramtype_air_main_ids+paramtype_air_more_ids, bdate, edate)
    print(res)
    # meteo_json_fname = concatenate_chunks_meteo(device_id, temp_dir)
    # sensors_json_fname = concatenate_chunks_sensors(device_id, temp_dir)
    # result_xlsx_fname = convert_concatenated_chunks_to_excel(sensors_json_fname, meteo_json_fname)