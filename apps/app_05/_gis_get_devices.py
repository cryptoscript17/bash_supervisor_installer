import os, json, time, requests
import glob
from datetime import datetime
from pathlib import Path

import pandas as pd

from geopy import distance

#   url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'
#   url = 'http://10.14.126.133:8104/dataset'
header = {'Content-Type':'application/json', 'Authorization':'cLOGaFJyyEt8ybCBXrUmRRfenCngOMflgYJ6xcOWhWa'}

def request_devices_dataset_json():
    return {}

def request_device_sensor_values_json(device_ids):
    return {}


def timestamp_to_dataset_preffix(ts):
    return str(datetime.fromisoformat(datetime.fromtimestamp(ts).isoformat()).strftime('%Y_%m_%d_'))

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

def request_devices_dataset_newapi():
    token = get_ecomon_token()
    if token is None:
        print(f"Token request error!! Exit...")
        return
    url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'
    header = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'Origin': 'https://dev.ecomon.mosreg.ru'
    }
    body = {        #   ONLY COUNT !!!
        "jsonrpc":"2.0",
        "method":"getData",
        "params":{
            "source_ids":[5],
            "device_ids":[],
            "sensor_ids":[],
            "bdate":"2024-04-29T00:00:00",
            "edate":"2024-04-29T18:00:00",
            "is_over_pdk": True,
            "_config_serverside": True,
            "orderby":[
                {
                    "selector":"bdate",
                    "desc": True
                }
            ],
            "_config_dataset":"BASE.DSAIR_SENSORVALUE",
            "_config_is_count": True
        },
        "id": 1
    }
    body = {
        "jsonrpc": "2.0",
        "method": "getData",
        "params": {
            "source_ids": [5],
            "device_ids": [],
            "sensor_ids": [],
            "bdate": "2024-04-29T00:00:00",
            "edate": "2024-04-29T18:00:00",
            "is_over_pdk": True,
            "_config_serverside": True,
            "orderby": [
                {
                    "selector": "bdate",
                    "desc": True
                }
            ],
            "_config_dataset": "BASE.DSAIR_SENSORVALUE",
            "limit": 20,
            "offset": 0
        },
        "id": 1
    }
    response = requests.post(url, headers=header, json=body, timeout=(5,60))
    if response.ok:
        response_json = response.json()
        if 'result' in response_json:
            if 'data' in response_json['result']:
                result_list = response_json['result']['data']
                return result_list
            else:
                print(f"RESPONSE_ERR:\n{response.text}")
        else:
            print(f"RESPONSE_ERR:\n{response.text}")
    else:
        print(f"RESPONSE_ERR:\n{response.text}")
        # return response.status_code
    return None


def get_ecomon_paramtypes():
    token = get_ecomon_token()
    if token is None:
        print(f"Token request error!! Exit...")
        return
    import requests
    url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSPARAMTYPE'
    headers = {'Content-Type':'application/json', 'Authorization': f'Bearer {token}', 'Origin': 'https://dev.ecomon.mosreg.ru'}
    body = {"jsonrpc":"2.0","method":"getData","params":{"_config_dataset":"BASE.DSPARAMTYPE"},"id":1}
    resp = requests.post(url, headers=headers, json=body)
    if resp.ok:
        return resp.json()
    else:
        return None


def request_devices_dataset_json():
    stime = time.time()
    source_ids = [3,4,5]
    this_script_path = os.path.abspath(os.path.realpath(__file__))
    workdir = os.path.dirname(this_script_path)
    dataset_fname = datetime.fromisoformat(datetime.fromtimestamp(stime).isoformat()).strftime('%Y_%m_%d_%H_') + '00_dataset_devices_response.txt'
    dataset_path = os.path.join(workdir, dataset_fname)
    token = get_ecomon_token()
    if token is None:
        print(f"Token request error!! Exit...")
        return
    # url = 'http://10.14.126.133:8104/dataset' 2024.04.20
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
    source_ids = [3, 4, 5]
    filtered_df = frame[frame['source_id'].isin(source_ids)]
    fname_preffix = timestamp_to_dataset_preffix(time.time())
    dataset_xlsx_path = os.path.join(workdir, fname_preffix + 'dataset.xlsx')
    xls_writer = pd.ExcelWriter(dataset_xlsx_path)
    filtered_df.to_excel(xls_writer, index=False, sheet_name='dataset')
    xls_writer.close()
    return dataset_xlsx_path

def get_dataset_xlsx_path_local():
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    fname_preffix = timestamp_to_dataset_preffix(time.time())
    dataset_xlsx_path = glob.glob(os.path.join(workdir, fname_preffix + 'dataset.xlsx'))
    if len(dataset_xlsx_path) == 0:
        print('Requesting New Dataset from GIS...', '\n')
        dataset_json_path = request_devices_dataset_json()
        print('Devices Dataset JSON Saved!', '\n')
        dataset_xlsx_path = convert_devices_dataset_json_to_xlsx(dataset_json_path)
    else:
        dataset_xlsx_path = dataset_xlsx_path[0]
        print('Parsing local XLSX ... ' + dataset_xlsx_path, '\n')
    return dataset_xlsx_path

def get_device_snum_by_name(dataset_xlsx_path, device_name):
    excel_data_df = pd.read_excel(dataset_xlsx_path, sheet_name='dataset', index_col = 'id', usecols=['id', 'snum', 'name'])
    excel_data_df.query("name == %s" % "'"+device_name+"'", inplace=True)
    snum = excel_data_df['snum'].values
    if len(snum) > 0:
        snum = snum[0]
    else:
        snum = 'NA'
    return snum

def get_device_id_by_name(dataset_xlsx_path, device_name):
    excel_data_df = pd.read_excel(dataset_xlsx_path, sheet_name='dataset', index_col = 'snum', usecols=['id', 'snum', 'name'])
    # excel_data_df.query("name == %s" % "'"+device_name+"'", inplace=True)
    excel_data_df.query("name == %s" % f"'{str(device_name).strip()}'", inplace=True)
    device_id = excel_data_df['id'].values
    if len(device_id) > 0:
        device_id = device_id[0]
    else:
        device_id = 'NA'
    return device_id

def get_device_ids_by_name(dataset_xlsx_path, device_name):
    device_ids = []
    excel_data_df = pd.read_excel(dataset_xlsx_path, sheet_name='dataset', index_col = 'snum', usecols=['id', 'snum', 'name'])
    # excel_data_df.query(f"name == '{str(device_name).strip()}'", inplace=True)
    excel_data_filtered_df = excel_data_df.loc[excel_data_df['name'].str.contains(str(device_name).strip())]
    try:
        device_ids = list(excel_data_filtered_df['id'].values)
    except Exception as e:
        print(f"Dataframe values filter error: [{e}]")
    return device_ids

def get_snum_by_device_id(dataset_xlsx_path, device_id):
    excel_data_df = pd.read_excel(dataset_xlsx_path, sheet_name='dataset', index_col = 'name', usecols=['id', 'snum', 'name'])
    # excel_data_df.query("id == %s" % "'"+device_id+"'", inplace=True)
    excel_data_df.query("id == %s" % f"'{device_id}'", inplace=True)
    device_snum = excel_data_df['snum'].values
    if len(device_snum) > 0:
        device_snum = device_snum[0]
    else:
        device_snum = 'NA'
    return device_snum


def get_device_id_by_snum(dataset_xlsx_path, snum):
    excel_data_df = pd.read_excel(dataset_xlsx_path, sheet_name='dataset', index_col = 'name', usecols=['id', 'snum', 'name'])
    excel_data_df.query("snum == %s" % "'"+snum+"'", inplace=True)
    device_id = excel_data_df['id'].values
    if len(device_id) > 0:
        device_id = device_id[0]
    else:
        device_id = 'NA'
    return device_id

def request_device_sensors_dataset_json(device_id):
    stime = time.time()
    this_script_path = os.path.abspath(os.path.realpath(__file__))
    workdir = os.path.dirname(this_script_path)
    dataset_fname = datetime.fromisoformat(datetime.fromtimestamp(stime).isoformat()).strftime('%Y_%m_%d_%H_') + f'00_dataset_device_{str(device_id)}_sensors_list.txt'
    dataset_path = os.path.join(workdir, dataset_fname)
    token = get_ecomon_token()
    if token is None:
        print(f"Token request error!! Exit...")
        return
    url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSSOURCE'
    header = {'Content-Type':'application/json', 'Authorization': f'Bearer {token}'}
    body = {
        "jsonrpc": "2.0",
        "method": "getData",
        "params": {
            "device_id":device_id,
            "_config_dataset": "BASE.DSAIR_SENSOR"
        },
        "id": 1
    }

    # body = {"jsonrpc":"2.0","method":"BASE.DSAIR_SENSOR.GETDATA","params":{"device_id":device_id,"_config_dataset":"BASE.DSAIR_SENSOR"},"id":1}

    response = requests.post(url, headers=header, json=body, timeout=(5,60)).json()
    if len(response) > 0:
        with open(dataset_path, "w") as outfile:
            outfile.write(json.dumps(response, indent=2))
    else:
        return ''
    return dataset_path

def convert_device_sensors_json_to_xlsx(dataset_json_path):
    import re
    pattern = r"device_(\d+)_sensors"
    device_id = re.search(pattern, os.path.basename(dataset_json_path)).group(1)
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    with open(dataset_json_path, newline='', encoding='utf-8') as json_file:
        json_text = ''.join(x for x in json_file.readlines())
        json_data = json.loads(json_text)['result']

    frame = pd.read_json(json.dumps(json_data), orient='records').sort_values(by=['id'])
    columns_to_keep = ['id','paramtype_measurement','paramtype_name','paramtype_pdk']
    frame = frame.drop(columns=[col for col in frame.columns if col not in columns_to_keep])
    frame = frame[['id','paramtype_name','paramtype_measurement','paramtype_pdk']]
    fname_preffix = timestamp_to_dataset_preffix(time.time())
    dataset_xlsx_path = os.path.join(workdir, fname_preffix + f'_device_{device_id}_sensors.xlsx')
    xls_writer = pd.ExcelWriter(dataset_xlsx_path)
    frame.to_excel(xls_writer, index=False, sheet_name='sensors')
    xls_writer.close()
    return dataset_xlsx_path

def get_device_sensor_ids_list(dataset_json_path):
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    with open(dataset_json_path, newline='', encoding='utf-8') as json_file:
        json_text = ''.join(x for x in json_file.readlines())
        json_data = json.loads(json_text)['result']
    sensor_ids = []
    for sensor_json in json_data:
        if 'id' in sensor_json:
            sensor_ids.append(sensor_json['id'])
    if len(sensor_ids) > 0:
        return sensor_ids
    else:
        return 'NA'

def get_latest_dataset_xlsx_path_local():
    workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    local_datasets_xlsx = glob.glob(os.path.join(workdir, '*dataset.xlsx'))
    if len(local_datasets_xlsx) > 1:
        latest_dataset_xlsx = local_datasets_xlsx[len(local_datasets_xlsx) - 1]
    else:
        latest_dataset_xlsx = local_datasets_xlsx
    return latest_dataset_xlsx

def get_device_data_by_name(dataset_xlsx_path, device_name):
    excel_data_df = pd.read_excel(dataset_xlsx_path, sheet_name='devices_dataset', index_col = 'id', usecols=['id', 'snum', 'name', 'wgs84'])
    excel_data_df.query("name == %s" % "'"+device_name+"'", inplace=True)
    excel_data_df['id'] = excel_data_df.index
    id, snum, geo = excel_data_df['id'].values[0], excel_data_df['snum'].values[0], excel_data_df['wgs84'].values[0]
    return id, snum, geo

def isNaN(num):
    return num != num

def get_nearest_devices_list(dataset_xlsx_path, device_dist):
    excel_data_df = pd.read_excel(dataset_xlsx_path, sheet_name='devices_dataset', index_col = 'id', usecols=['id', 'snum', 'name', 'wgs84'], dtype={'id':int, 'snum':str, 'name':str, 'wgs84':str})
    #excel_data_df.query("name == %s" % "'"+device_name+"'", inplace=True)
    excel_data_df['id'] = excel_data_df.index
    #print(len(excel_data_df['snum'].values), type(excel_data_df['snum'].values))

    devices_linked = []
    for index, base_row in excel_data_df.iterrows():
        device_linked = []
        base_snum, base_geo = base_row['snum'], base_row['wgs84']
        #print(base_snum, base_geo, type(base_snum))
        for index, row in excel_data_df.iterrows():
            link_snum, link_geo = row['snum'], row['wgs84']
            dist = distance.distance(base_geo, link_geo).meters
            if dist <= device_dist and base_snum != link_snum and not isNaN(link_snum) and not isNaN(base_snum):
                device_linked.append(str(base_snum) + ',' + str(link_snum) + ',' + str(int(dist)))
                #print((base_snum, link_snum, int(dist)))
        if len(device_linked) > 0:
            devices_linked.append(device_linked)

    #print('\n'.join(x for x in devices_linked))
    if len() > 0:
        return devices_linked
        print(devices_linked)
    else:
        return 'NA'

if __name__ == '__main__':
    params_json = []
    paramtypes_json = get_ecomon_paramtypes()
    if 'result' in paramtypes_json:
        paramtypes_json = paramtypes_json['result']
        for paramtype_json in paramtypes_json:
            params_json.append({
                'paramtype_id': paramtype_json['id'],
                'paramtype_pdk': paramtype_json['pdk'],
                'paramtype_code': paramtype_json['code'],
                'paramtype_name': paramtype_json['name'],
                'paramtype_measurement': paramtype_json['measurement']
            })
            print(f"{paramtype_json['id']};{paramtype_json['pdk']};{paramtype_json['code']};{paramtype_json['name']};{paramtype_json['measurement']}")
        workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
        fpath = os.path.join(workdir, '_ecomon_paramtypes.txt')
        with open(fpath, 'w') as fp:
            json.dump(params_json, fp, indent=2)
    quit()
    IS_NEW_API = True
    if IS_NEW_API:
        response_json = request_devices_dataset_newapi()
    else:
        response_json = request_devices_dataset_json()
        with open(response_json, 'r') as fp:
            response_json = json.load(fp)
        response_json = response_json['result']

    response_item = response_json[0]
    response_item_keys = list(response_item.keys())
    response_item_keys.sort()
    response_item_keys_str = ', '.join(x for x in response_item_keys)
    print(f"RESPONSE_ITEM_KEYS: [{response_item_keys_str}]")
    print(f"RESPONSE_TYPE: {type(response_json)} | LEN: [{len(response_json)}]\n{response_json[0]}")