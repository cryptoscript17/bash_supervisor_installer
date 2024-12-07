import os, json, time, requests, math
import glob
from datetime import datetime
from pathlib import Path
from functools import reduce
import pandas as pd
import shutil
import logging

logging.basicConfig(level=logging.INFO)

def get_bdate_edate():
    start = time.time()
    bdate_ts = int((math.floor(start / 1200) - 1) * 1200)
    edate_ts = int((math.ceil(start / 1200) - 1) * 1200)
    bdate = str(datetime.fromisoformat(datetime.fromtimestamp(bdate_ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S'))
    edate = str(datetime.fromisoformat(datetime.fromtimestamp(edate_ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S'))
    return (bdate, edate)

def dt_differ_samples_count(bdate, edate):
    bdate_ts = time.mktime(datetime.strptime(bdate, "%Y-%m-%dT%H:%M:%S").timetuple())
    edate_ts = time.mktime(datetime.strptime(edate, "%Y-%m-%dT%H:%M:%S").timetuple())
    samples_count = int((edate_ts - bdate_ts) / 1200)
    #print(int((edate_ts - bdate_ts) / 1200))
    return samples_count


def filter_dict(dictionary):
    allowed_keys = ['bdate', 'device_id', 'sensor_id', 'paramtype_code', 'paramtype_pdk', 'sensorvalue', 'device_name']
    return {k: v for k, v in dictionary.items() if k in allowed_keys}


def request_device_history_by_chunk(mode, bdate = None, edate = None):
    stime = time.time()
    iteration_timeout = 60

    if edate is None:
        (bdate, edate) = get_bdate_edate()
    logging.info(f'Requesting period is [{bdate} - {edate}]')
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    dataset_fname_preffix = str(edate).replace('-', '_').replace(':', '_') + '_'
    # temp_dir = dataset_fname_preffix + 'temp'
    temp_dir = os.path.join(workdir, 'temp', dataset_fname_preffix)
    dataset_path = os.path.join(temp_dir, dataset_fname_preffix + '_pdk_json.txt')
    last_sample_path = os.path.join(workdir, 'temp', 'last_sample.txt')

    logging.info('Working directory is ... ' + temp_dir + '\n')
    token = get_ecomon_token()
    if token is None:
        print(f"Token request error!! Exit...")
        return
    # url = 'http://10.14.126.133:8104/dataset'
    url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'
    # header = {'Content-Type':'application/json', 'Authorization':'cLOGaFJyyEt8ybCBXrUmRRfenCngOMflgYJ6xcOWhWa'}
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'Origin': 'https://dev.ecomon.mosreg.ru'
    }
    from datetime import timedelta
    logging.info(f'mode: {mode}')
    is_response = False
    if mode == 'last_edate':
        body = {"jsonrpc":"2.0","method":"BASE.DSAIR_SENSORVALUE.GETDATA","params":{"source_ids":[3,4,5],"paramtype_ids":[4,5,6,8,9,10,11,12,13,17],"device_ids":[device_id],"sensor_ids":[],"edate":edate,"is_over_pdk":"true","_config_serverside":"true","where":[],"orderby":[{"selector":"bdate","desc":"true"}],"_config_dataset":"BASE.DSAIR_SENSORVALUE","limit":1,"offset":0},"id":1}
    else:
        with open(last_sample_path) as f:
            data = json.load(f)
            if 'bdate' in data:
                bdate = data['bdate']
                bdate = datetime.fromisoformat(data['bdate']) + timedelta(seconds=1)
                bdate = bdate.isoformat()
                logging.info(f'bdate: {bdate}')
            else:
                return (False, 'Ошибка чтения файла last_sample.txt')
        body = {"jsonrpc":"2.0","method":"BASE.DSAIR_SENSORVALUE.GETDATA","params":{"source_ids":[3,4,5],"paramtype_ids":[4,5,6,8,9,10,11,12,13,17],"device_ids":[device_id],"sensor_ids":[],"bdate":bdate,"is_over_pdk":"true","_config_serverside":"true","where":[],"orderby":[{"selector":"bdate","desc":"true"}],"_config_dataset":"BASE.DSAIR_SENSORVALUE","limit":50,"offset":0},"id":1}

    bdates, result_json = [], []
    iteration, is_response = 0, False
    while not is_response and iteration < 5:
        response = requests.post(url, headers=headers, json=body, timeout=(5,60))
        if response.status_code == 200:
            json_data = response.json()
            #if len(json_data) > 0:
            if 'result' in json_data and 'data' in json_data['result']:
                total_count = len(json_data['result']['data'])
                if total_count > 0:
                    if mode != 'last_edate':
                        for item in json_data['result']['data']:
                            if item['bdate'] not in bdates:
                                bdates.append(item['bdate'])
                            result_json.append(filter_dict(item))

                        with open(dataset_path, "w") as outfile:
                            outfile.write(json.dumps(result_json, indent=2))

                        with open(last_sample_path, "w") as outfile:
                            outfile.write(json.dumps({'bdate': json_data['result']['data'][0]['bdate']}, indent=2))

                    else:
                        with open(last_sample_path, "w") as outfile:
                            outfile.write(json.dumps({'bdate': json_data['result']['data'][0]['bdate']}, indent=2))

                    is_response = True
                else:
                    is_response = False
                    logging.info(f'Requesting runtime is ... {str(int(time.time() - stime))} sec.\n')
                    return (False, "Пустой JSON ответ, json_data['result']['data']")
            else:
                total_count = 0
                logging.info('Response JSON empty!')
                return (False, "Отсутсвуют ключи 'result', 'data'")
        else:
            is_response = False
            iteration += 1
            logging.info(f'Sleeeping..... {str(iteration_timeout)} sec.\n')
            time.sleep(iteration_timeout)

    logging.info(f'Runtime is {str(int(time.time() - stime))} sec.\n')

    if 'total_count' not in locals():
        total_count = 0

    if 'bdates' in locals():
        logging.info(f'bdates: {bdates}')

    return (True, total_count, result_json)
    return







def legacy_request_device_history_by_chunk(device_id, bdate = None, edate = None):
    stime = time.time()
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    dataset_fname_preffix = str(edate).replace('-', '_').replace(':', '_')
    temp_dir = f'{dataset_fname_preffix}_temp'
    temp_dir = os.path.join(workdir, temp_dir)
    dataset_path = os.path.join(temp_dir, dataset_fname_preffix + '_pdk_json.txt')
    # last_sample_path = os.path.join(workdir, 'temp', 'last_sample.txt')

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    else:
        shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)

    workdir = os.path.join(workdir, temp_dir)

    print(f'Working directory is ... [{workdir}]')

    token = get_ecomon_token()
    if token is None:
        print(f"Token request error!! Exit...")
        return

    url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'Origin': 'https://dev.ecomon.mosreg.ru'
    }
    #is_response = False
    #if edate is None:
    #    (bdate_now, edate) = get_bdate_edate()
    print('Requesting period is [' + bdate + ' - ' + edate + ']')
    limit = 1000
    offset = 0
    is_offset = True
    keys_to_keep = ["bdate", "paramtype_code", "sensorvalue", "windspd", "winddir", "tempvalue",  "pressure"]
    samples_count = int(dt_differ_samples_count(bdate, edate))
    print(f'Requested bdate/edate period contains .. [{samples_count}] samples.')
    # samples_count = 11803
    batch_size = 1000
    batches = [range(0,samples_count)[i:i + batch_size - 1] for i in range(0, len(range(0,samples_count)), batch_size)]

    bdates = []
    for batch in batches:
        offset = batch[0]
        print(f'Requesting offset ... [{offset} / {samples_count}]')
        body = {
            "jsonrpc": "2.0",
            "method": "getData",
            "params": {
                "bdate": bdate,
                "edate": edate,
                "device_ids": [device_id],
                "orderby": [{"selector": "bdate"}],
                "_config_dataset": "BASE.DSAIR_SENSORVALUE",
                "limit": limit,
                "offset": offset
            },
            "id":1
        }

        response = requests.post(url, headers=headers, json=body, timeout=(5,60))
        print(f"{url}|{headers}|{body}|{response.text}")
        if response.status_code == 200:
            json_data = response.json()
            clear_json = []
            print(f"RESP_JSON:\n{json_data}\n{len(json_data.keys())}")
            try:
                total_count = len(json_data['result']['data'])
                dataset_path = os.path.join(workdir, f'{dataset_fname_preffix}_offset_{offset}_device_{device_id}_history_json.txt')
                for item in json_data['result']['data']:
                    if item['bdate'] not in bdates:
                        bdates.append(item['bdate'])
                    clear_json.append(filter_dict(item))
                    #for key in list(item.keys()):
                    #    if key not in keys_to_keep:
                    #        del item[key]
                    # clear_json.append(item)
                #with open(dataset_path, "w") as outfile:
                #    outfile.write(json.dumps(clear_json, indent=2))

            except Exception as e:
                print(f"Error parsing response JSON: {e} | offset: {offset}")
            finally:
                if len(clear_json) > 0:
                    with open(dataset_path, 'w') as fp:
                        json.dump(obj=clear_json, fp=fp, indent=2)
        else:
            print(f'Request to sensor_id = {device_id} error! Status_code is {response.status_code}')

    print('Runtime is ' + str(int(time.time() - stime)) + 'sec.\n')
    return workdir

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

    #data_frames = [meteo_frame, df_pivot]
    #df_merged = reduce(lambda left,right: pd.merge(left,right,on=['bdate'], how='outer'), data_frames).fillna('')
    #df_merged.insert(0, 'bdate', df_merged.pop('bdate'))
    if len(meteo_frame) > 0:
        df_merged = pd.merge(meteo_frame, df_pivot, on='bdate', how='inner').set_index('bdate')
    else:
        df_merged = df_pivot
    df_merged.sort_values(by=['bdate'], inplace=True)

    dataset_xlsx_path = sensors_json_fname.replace('result.txt', '.xlsx')
    xls_writer = pd.ExcelWriter(dataset_xlsx_path)
    df_merged.to_excel(xls_writer, index='bdate', sheet_name='device_history')
    #meteo_frame.to_excel(xls_writer, index='bdate', sheet_name='meteo')
    #sensors_frame.to_excel(xls_writer, index='bdate', sheet_name='sensors')
    xls_writer.close()
    print('Convertation to XLSX runtime is ' + str(int(time.time() - stime)) + 'sec.\n')
    return dataset_xlsx_path

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

def gis_get_history(bdate: str, edate: str, device_id: int):
    '''
    Main wrapper for device_id timedelta
        interval [bdate;edate] request device samples data

    :bdate:     Start interval | datetime
    :edate:     Finish interval | datetime
    :device_id:     Device id | integer
    :return:    Returns path to result XLSX table | str
    '''

    # samples_count = str(dt_differ_samples_count(bdate, edate))

    temp_dir = legacy_request_device_history_by_chunk(device_id, bdate, edate)
    print(f"Requesting result...[{temp_dir}]")
    meteo_json_fname = concatenate_chunks_meteo(device_id, temp_dir)
    sensors_json_fname = concatenate_chunks_sensors(device_id, temp_dir)
    result_xlsx_fname = convert_concatenated_chunks_to_excel(sensors_json_fname, meteo_json_fname)
    return result_xlsx_fname

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

def get_device_id_by_snum(snum):
    def timestamp_to_dataset_preffix(ts):
        return str(datetime.fromisoformat(datetime.fromtimestamp(ts).isoformat()).strftime('%Y_%m_%d_'))

    def get_latest_dataset_xlsx_path_local():
        workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
        local_datasets_xlsx = glob.glob(os.path.join(workdir, '*dataset.xlsx'))
        if len(local_datasets_xlsx) > 1:
            latest_dataset_xlsx = local_datasets_xlsx[len(local_datasets_xlsx) - 1]
        else:
            latest_dataset_xlsx = local_datasets_xlsx
        return latest_dataset_xlsx

    def request_devices_dataset_json():
        token = get_ecomon_token()
        if token is None:
            print(f"Token request error!! Exit...")
            return
        stime = time.time()
        source_ids = [1,2,3,4,5,6]
        this_script_path = os.path.abspath(os.path.realpath(__file__))
        workdir = os.path.dirname(this_script_path)
        dataset_fname = datetime.fromisoformat(datetime.fromtimestamp(stime).isoformat()).strftime('%Y_%m_%d_%H_') + '00_dataset_devices_response.txt'
        dataset_path = os.path.join(workdir, dataset_fname)

        url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_DEVICE'
        header = {'Content-Type':'application/json', 'Authorization': f'Bearer {token}', 'Origin': 'https://dev.ecomon.mosreg.ru'}
        body = {
            "jsonrpc": "2.0",
            "method": "getData",
            "params": {
                "_config_dataset": "BASE.DSAIR_DEVICE"
            },
            "id": 1
        }
        # {"jsonrpc":"2.0","method":"getData","params":{"bdate":"2024-04-18T00:00:00","source_ids":[5],"device_ids":[],"sensor_ids":[],"edate":"2024-04-20T00:00:00","is_over_pdk":true,"_config_serverside":true,"orderby":[{"selector":"bdate","desc":true}],"_config_dataset":"BASE.DSAIR_SENSORVALUE","limit":20,"offset":0},"id":1}
        response = requests.post(url, headers=header, json=body, timeout=(5,60)).json()
        if len(response) > 0:
            with open(dataset_path, "w") as outfile:
                outfile.write(json.dumps(response, indent=2))
            return dataset_path
        else:
            return ''

    def convert_devices_dataset_json_to_xlsx(dataset_json_path):
        this_script_path = os.path.abspath(os.path.realpath(__file__))
        workdir = os.path.dirname(this_script_path)
        with open(dataset_json_path, newline='', encoding='utf-8') as json_file:
            json_text = ''.join(x for x in json_file.readlines())
            json_data = json.loads(json_text)['result']
            frame = pd.read_json(json.dumps(json_data), orient='records').drop(columns = ['r_summary', 'r_count', 'is_good', 'ac_ids', 'inum', 'guid', 'the_geom', 'calibrationdate', 'numversion_revert', 'creator_id', 'bdate', 'creator', 'modifier', 'parent_id', 'calibrationinterval', 'state_name', 'pageparent_id', 'pagedevice_id', 'r_arrayguid', 'modifier_id', 'color_id', 'model', 'edate', 'comments'])
            frame["wgs84"] = round(frame["lat"].astype(float), 3).astype(str).str.cat(round(frame["lon"].astype(float), 3).astype(str), sep=",")
            frame = frame.drop(columns = ["lon","lat"]).sort_values(by=['source_id'])
            frame = frame[['id', 'snum', 'source_id', 'source_name', 'name', 'address', 'wgs84', 'ip', 'act_ids', 'createdate', 'modifydate', 'sensor_num', 'rgis']]
            source_ids = [1,2,3,4,5,6]
            filtered_df = frame[frame['source_id'].isin(source_ids)]
            fname_preffix = timestamp_to_dataset_preffix(time.time())
            dataset_xlsx_path = os.path.join(workdir, fname_preffix + 'dataset.xlsx')
            xls_writer = pd.ExcelWriter(dataset_xlsx_path)
            filtered_df.to_excel(xls_writer, index=False, sheet_name='dataset')
            xls_writer.close()
            return dataset_xlsx_path

    def get_dataset_xlsx_path_local():
        response_json_path = request_devices_dataset_json()
        dataset_xlsx_path_new = convert_devices_dataset_json_to_xlsx(response_json_path)
        return dataset_xlsx_path_new

    dataset_xlsx_path_new = get_dataset_xlsx_path_local()
    dataset_xlsx_path_latest = get_latest_dataset_xlsx_path_local()

    print(f"NEW: [{dataset_xlsx_path_new}]\nLAST: [{dataset_xlsx_path_latest}]\n")
    if os.path.exists(dataset_xlsx_path_new):
        excel_data_df = pd.read_excel(dataset_xlsx_path_new, sheet_name='dataset', index_col = 'name', usecols=['id', 'snum', 'name'])
        excel_data_df.query("snum == %s" % "'"+snum+"'", inplace=True)
        device_id = excel_data_df['id'].values
        if len(device_id) > 0:
            device_id = device_id[0]
        else:
            device_id = 'NA'
        return device_id
    else:
        return -1


workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
moem_messages_path = os.path.join(workdir, 'moem_messages')
if not os.path.exists(moem_messages_path):
    os.makedirs(moem_messages_path)

#   DEBUG
def save_response_json(message, suffix):
    import json
    if isinstance(message, str):
        message = json.loads(message)
    try:
        with open(os.path.join(moem_messages_path, f'{str(int(time.time()))}_message_{suffix}.txt'), "w") as outfile:
            outfile.write(json.dumps(message, indent=2))
    except Exception as error:
        logging.error(f'Ошибка сохранения ответа: {error}')


def request_devices_registry_dataset():
    url = 'http://10.14.126.133:8104/dataset'
    headers = {
        'Authorization': 'cag1KOpJ3dJOi8ZKrQ2OhDXqOfywBDURp_CAFYXny8S',
        'Content-Type': 'application/json'
    }
    payload = {
	    "jsonrpc":"2.0",
	    "method":"BASE.DSAIR_DEVICE.GETDATA",
	    "params": {
		    "_config_dataset":"BASE.DSAIR_DEVICE"
	    },
	    "id":1
    }
    url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_DEVICE'
    headers = {'Content-Type':'application/json', 'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiI1MDAyNzUiLCJpYXQiOjE3MTYyNjgwMTJ9.kI6MPOXf7kbQv1snzo-DPC3gYwRXX6VYtEMLP8aYGMM', 'Origin': 'https://dev.ecomon.mosreg.ru'}
    body = {
        "jsonrpc": "2.0",
        "method": "getData",
        "params": {
            "_config_dataset": "BASE.DSAIR_DEVICE"
        },
        "id": 1
    }
    is_response, iteration, iterations, timeout = False, 0, 3, 60
    json_keys = ['id', 'snum']

    while not is_response and iteration <= iterations:
        print(f'Requesting iteration ... {iteration} from {iterations}.\n')
        response = requests.post(url, headers=headers, json=body, timeout=(5,60))
        if response.status_code == 200:
            result_json = []
            json_data = response.json()
            if 'result' in json_data:
                for json_item in json_data['result']:
                    item_json = {}
                    for json_key in json_item.keys():
                        if json_key in json_keys:
                            item_json.update({f'{json_key}': json_item[json_key]})
                    result_json.append(item_json)

                return result_json
            #is_response = True
            #break
        else:
            iteration += 1
            print(f"Request to devices registry! Status_code is {response.status_code}\nSleeeping.....{timeout} sec.")
            time.sleep(timeout)
    else:
        print(f'Error requesting after {iteration} attemps.')
    if 'result_json' not in locals():
        return None

def difference_of_lists(list1, list2):
    return list(set(list1) - set(list2))

def request_devices_sample_dataset():
    bdate, edate = get_bdate_edate()
    from datetime import datetime, timedelta

    date_string = "2021-01-01T00:00:00"
    date_format = "%Y-%m-%dT%H:%M:%S"
    date = datetime.strptime(bdate, date_format)

    new_bdate = date - timedelta(minutes=20)
    new_bdate_string = new_bdate.strftime(date_format)
    new_edate_string = bdate

    #bdate = "2023-08-10T12:00:00"
    #edate = "2023-08-10T12:20:00"
    device_ids = [1185,1184,1183,1182,1181,1180,1179,1178,1177,1176,1175,1174,1173,1172,1171,1170,1169,1168,1167,1166,1165,1164,1163,1162,1160,1159,1154,1152,1151,1150,1149,1148,1147,1146,1144,1143,1142,1141,1140,1139,1138,1137,1136,1135,1134,1133,1132,1130,1123,1122,1121,1118,1117,1116,1115,1114,1113,1112,1111,1110,1109,1108,1107,1106,1105,1104,1103,1100,1097,1096,1095,1094,1093,1092,1091,1089,1088,1084,1083,1082,1081,1080,1079,1077,1076,1074,1065,1064,1063,1062,1061,1060,1059,1057,1048,1047,1046,1045,1044,1043,1042,1041,1040,1020,1019,1018,1017,1015,1014,1013,1012,1011,1010,1009,1008,1007,1006,1004,1003,1001,1000,999,998,996,995,994,992,991,990,989,987,985,984,983,982,981,980,979,978,977,976,975,974,973,971,970,969,968,966,964,963,962,959,958,957,956,954,953,951,950,949,948,940,939,938,937,936,935,934,933,932,930,300,299,298,297,296,295,294,293,292,291,290,289,286,285,284,283,281,280,276,274,269,268,267,266,264,262,261,260,256,250,245,244,243,242,241,240,239,238,237,163,162,159,158,157,156,155,154,153,152,151,149,147,146,145,144,143,142,141,140,139,138,137,136]
    paramtype_ids = [6,9,10,12,17]

    url = 'http://10.14.126.133:8104/dataset'
    url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'
    headers = {
        'Authorization': 'cag1KOpJ3dJOi8ZKrQ2OhDXqOfywBDURp_CAFYXny8S',
        'Content-Type': 'application/json'
    }
    headers = {'Content-Type':'application/json', 'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiI1MDAyNzUiLCJpYXQiOjE3MTYyNjgwMTJ9.kI6MPOXf7kbQv1snzo-DPC3gYwRXX6VYtEMLP8aYGMM', 'Origin': 'https://dev.ecomon.mosreg.ru'}
    payload = {
        "jsonrpc":"2.0",
        "method":"BASE.DSAIR_SENSORVALUE.GETDATA",
        "params":{
            "edate": new_edate_string,
            "bdate": new_bdate_string,
            "device_ids": device_ids,
            "paramtype_ids": paramtype_ids,
            "_config_serverside": "false",
            "where":[],
            "orderby":[{
                "selector":"bdate",
                "desc": "true"
            }],
            "_config_dataset":"BASE.DSAIR_SENSORVALUE",
            "offset": 0
        },
        "id":1
    }
    body = {
        "jsonrpc":"2.0",
        "method":"getData",
        "params":{
            "device_ids": device_ids,
            "paramtype_ids": paramtype_ids,
            # "sensor_ids":[],
            "bdate": new_bdate_string,
            "edate": new_edate_string,
            "_config_serverside":"true",
            "orderby":[
                {
                    "selector":"bdate",
                    "desc":"true"
                }
            ],
            "_config_dataset":"BASE.DSAIR_SENSORVALUE"
        },
        "id":1
    }
    #print(payload)
    is_response, iteration, iterations, timeout = False, 0, 3, 60
    while not is_response and iteration <= iterations:
        print(f'Requesting iteration (func [request_devices_sample_dataset]) ... {iteration} from {iterations}.\n')
        response = requests.post(url, headers=headers, json=body, timeout=(5,60))
        if response.status_code == 200:
            response_devices = []
            json_data = response.json()
            if 'result' in json_data:
                print(len(json_data['result']), json_data['result'].keys())
                if 'data' in json_data['result']:
                    for json_item in json_data['result']['data']:
                        if json_item['device_id'] not in response_devices:
                            response_devices.append(json_item['device_id'])
                    diff_ids = difference_of_lists(device_ids, response_devices)
                    print(f"Размеры списков: {len(device_ids)} | {len(response_devices)}")
                else:
                    iteration += 1
                    print(f"Response error, no key 'data': {json_data['result'].keys()}...")
                    time.sleep(timeout)
            return (diff_ids, json_data)
            #is_response = True
            #break
        else:
            iteration += 1
            print(f"Request to {len(device_ids)} devices! Status_code is {response.status_code}\nSleeeping.....{timeout} sec.")
            time.sleep(timeout)
    else:
        print(f'Error requesting after {iteration} attemps.')
    if 'json_data' not in locals():
        return None


def save_devices_sample_dataset_xlsx(json_data):
    #json_data = os.path.join(workdir, 'dataset (1).json')
    if 'result' in json_data and 'data' in json_data['result']:
        if len(json_data['result']['data']) > 0:
            if 'bdate' in json_data['result']['data'][0]:
                bdate = json_data['result']['data'][0]['bdate'].replace(':', '_')
    else:
        bdate = 'NA'
    keylist = ['device_id', 'device_name', 'bdate', 'paramtype_code', 'sensorvalue']

    #with open(json_data, 'r', encoding='utf8') as f:
    #    json_data = json.loads(f.read())['result']

    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    data_json_min = []
    for json_item in json_data['result']['data']:
        json_item_min = {}
        for json_key in json_item.keys():
            if json_key in keylist:
                json_item_min.update({f'{json_key}': json_item[json_key]})
        data_json_min.append(json_item_min)

    paramtypes = ['CO', 'NO2', 'H2S', 'NH3', 'SO2']
    df = pd.read_json(json.dumps(data_json_min), orient='records')

    #   MERGE SNUM
    devices_registry = request_devices_registry_dataset()
    if devices_registry is not None:
        save_response_json(devices_registry, 'devices_registry')
        df_snum = pd.read_json(json.dumps(devices_registry), orient='records')
        df_snum.rename(columns={'id': 'device_id'}, inplace=True)

    #print(df_snum.head())

    df_names = df[['device_id', 'device_name']]
    df_names.drop_duplicates(inplace=True)
    df_names['device_name'] = df_names['device_name'].str.replace('Пост: ', '')
    df_pivot = df.pivot_table(index='device_id', columns='paramtype_code', values='sensorvalue')
    df_pivot.insert(len(df_pivot.columns), 'percentage', None)

    for index, row in df_pivot.iterrows():
        vals = 0
        for col in paramtypes:
            if col in df_pivot.columns:
                if not math.isnan(row[col]):
                    #sens.append(col)
                    if row[col] != 0:
                        vals += 1
                    else:
                        vals += 0
        df_pivot.at[index, 'percentage'] = round(vals / len(paramtypes), 2) * 100

    df_pivot.sort_values(by=['percentage'], inplace=True)

    merged_df = pd.merge(df_pivot, df_names, on='device_id', how='left')

    device_name = merged_df.pop('device_name')
    merged_df.insert(1, 'device_name', device_name)

    merged_df.set_index('device_id', inplace=True)

    dataset_xlsx_path = os.path.join(moem_messages_path, f'{bdate}_device_sensor_statuses.xlsx')
    xls_writer = pd.ExcelWriter(dataset_xlsx_path)
    if 'df_snum' in locals():
        full_merged_df = pd.merge(merged_df, df_snum, on='device_id', how='left')
        snum = full_merged_df.pop('snum')
        full_merged_df.insert(1, 'snum', snum)
        full_merged_df.set_index('device_id', inplace=True)
        full_merged_df.to_excel(xls_writer, index='device_id', sheet_name='sensor_status')
    else:
        merged_df.to_excel(xls_writer, index='device_id', sheet_name='sensor_status')
    xls_writer.close()
    if not os.path.exists(dataset_xlsx_path):
        return None
    return dataset_xlsx_path



if __name__ == '__main__':
    print(request_devices_sample_dataset())
    quit()
    temp_dir = 'C:\\_python_services\\aiogram_gis_bot\\2022_01_01T00_00_00_temp'
    device_id = 297
    bdate = "2021-01-01T00:00:00"
    edate = "2023-06-22T00:00:00"
    meteo_json_fname = concatenate_chunks_meteo(device_id, temp_dir)
    sensors_json_fname = concatenate_chunks_sensors(device_id, temp_dir)
    result_xlsx_fname = convert_concatenated_chunks_to_excel(sensors_json_fname, meteo_json_fname)