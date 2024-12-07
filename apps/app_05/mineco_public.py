import os, json
import requests
import pandas as pd
import shutil
import time
from datetime import datetime

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))

def timestamp_to_filename_prefix():
    import time
    from datetime import datetime
    return str(datetime.fromisoformat(datetime.fromtimestamp(time.time()).isoformat()).strftime('%Y_%m_%d_'))

def save_object_class_methods(obj) -> None:
    import os
    fname = str(type(obj)).split(' ')[1][1:len(str(type(obj)).split(' ')[1])-2] + '.txt'
    if not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(os.path.realpath(__file__))), fname)):
        with open(os.path.join(os.path.dirname(os.path.abspath(os.path.realpath(__file__))), fname), 'w') as f:
            print('\n'.join(x for x in dir(obj)), file=f)

def get_post_names():
    url = f'https://rgis.mosreg.ru/v3/ecomonitoring/getPostName'

    response = requests.get(url)
    if response.status_code not in [200, 204]:
        logging.error(f"Failed to send getPostName. Status code: {response.status_code}")
        return {'status_code': response.status_code}
    else:
        #logging.info(f"getPostName request sended. Status code: {response.status_code}")
        return response.json()

def get_main_info(post_id):
    url = f'https://rgis.mosreg.ru/v3/ecomonitoring/getMainInfo?post_id={post_id}'

    response = requests.get(url)
    if response.status_code not in [200, 204]:
        logging.error(f"Failed to send getPostName. Status code: {response.status_code}")
        return {'status_code': response.status_code}
    else:
        #logging.info(f"getPostName request sended. Status code: {response.status_code}")
        return response.json()

def get_chart(post_id, sensor_id):
    url = f'https://rgis.mosreg.ru/v3/ecomonitoring/getChart?post_id={post_id}&sensor_id={sensor_id}'

    response = requests.get(url)
    if response.status_code not in [200, 204]:
        logging.error(f"Failed to send getPostName. Status code: {response.status_code}")
        return {'status_code': response.status_code}
    else:
        logging.info(f"getPostName request sended. Status code: {response.status_code}")
        return response.json()

def get_public_devices_table():
    post_names_json = get_post_names()
    if len(post_names_json) > 0:
        with open(os.path.join(workdir, timestamp_to_filename_prefix()+'_post_names_json.txt'), "w") as outfile:
            outfile.write(json.dumps(post_names_json, indent=2))
        devices_published_arr = []
        devices_unpublished_arr = []
        for device in post_names_json:
            device_name, device_geo, device_id = device['name'], ','.join([str(device['lat']), str(device['lon'])]), device['post_id']
            if device['isPublished']:
                devices_published_arr.append({'device_name': device_name, 'device_geo': device_geo, 'device_id': device_id})
            else:
                devices_unpublished_arr.append({'device_name': device_name, 'device_geo': device_geo, 'device_id': device_id})
    return devices_published_arr, devices_unpublished_arr

def save_public_devices_table():
    devices_published_arr, devices_unpublished_arr = get_public_devices_table()
    result = []
    for i, device_published in enumerate(devices_published_arr):
        if i < 275:
            post_id = device_published['device_id']
            post_info_json = get_main_info(post_id)
            if len(post_info_json) > 0:
                #with open(os.path.join(workdir, timestamp_to_filename_prefix()+f'_post_info_json_{post_id}.txt'), "w") as outfile:
                #    outfile.write(json.dumps(post_info_json, indent=2))
                device_ch2o = ''
                device_so2 = ''
                device_co = ''
                device_h2s = ''
                device_pm10 = ''
                device_no2 = ''
                device_nh3 = ''
                device_pm25 = ''
                device_no = ''
                device_name = device_published["device_name"]
                for sensor in post_info_json['indicators']:
                    if sensor['sensor_id'] == '4':
                        device_pm25 = sensor['current']
                    elif sensor['sensor_id'] == '5':
                        device_pm10 = sensor['current']
                    elif sensor['sensor_id'] == '6':
                        device_co = sensor['current']
                    elif sensor['sensor_id'] == '8':
                        device_no = sensor['current']
                    elif sensor['sensor_id'] == '9':
                        device_no2 = sensor['current']
                    elif sensor['sensor_id'] == '10':
                        device_so2 = sensor['current']
                    elif sensor['sensor_id'] == '12':
                        device_h2s = sensor['current']
                    elif sensor['sensor_id'] == '13':
                        device_ch2o = sensor['current']
                    elif sensor['sensor_id'] == '17':
                        device_nh3 = sensor['current']
                    else:
                        print(f"Error! Unknown sensortype: {str(sensor['sensor_id'])}")

                result.append({'device_name': device_name, 'CH2O': device_ch2o, 'SO2': device_so2, 'CO': device_co, 'H2S': device_h2s, 'PM10': device_pm10, 'NO2': device_no2, 'NH3': device_nh3, 'PM25': device_pm25, 'NO': device_no})



    #workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
    devices_fname = os.path.join(workdir, timestamp_to_filename_prefix()+'_rgis_public_devices_table.txt')

    with open(devices_fname, "w") as outfile:
        outfile.write(json.dumps(result, indent=2))

    with open(devices_fname, newline='', encoding='utf-8') as json_file:
        json_text = ''.join(x for x in json_file.readlines())
        json_data = json.loads(json_text)

    # saving messages json to XLSX
    devices_frame_public = pd.read_json(json.dumps(json_data), orient='records')
    columns = ['CH2O', 'SO2', 'CO', 'H2S', 'PM10', 'NO2', 'NH3', 'PM25', 'NO']
    dataset_xlsx_path = devices_fname.replace('.txt', '.xlsx')
    xls_writer = pd.ExcelWriter(dataset_xlsx_path)
    devices_frame_public.to_excel(xls_writer, index='device_name', sheet_name='messages')
    xls_writer.close()
    return dataset_xlsx_path


def get_admin_token():
    url = 'https://rgis.mosreg.ru/int/login/'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    }

    data = {
        'username': 'BalagaezyanVD',
        'password': 'v9qgwt'
    }

    session = requests.Session()
    response = session.post(url, headers=headers, data=data)
    cookies = session.cookies.get_dict()
    print('Status Code:', response.status_code)
    print('Response Headers:', response.headers)
    print('Cookies:', cookies)
    try:
        session_id = json.loads(response.text)['session_id']
    except Exception as error:
        logging.error(f'Session error: {error}')
        session_id = 'NA'
    return session_id


def get_admin_session(session_id):
    url = 'https://rgis.mosreg.ru/int/swagger/auth/session/'

    data = {
        'username': 'BalagaezyanVD',
        'password': 'v9qgwt'
    }

    cookies = {
        'mojo.rgis.mosreg.ru+int': 'eyJzZXNzaW9uX2lkIjoiMDhlNTM5MmZhODE3OTg4NjE5YzcwYzljZDg2ZjQ2NjMiLCJleHBpcmVzIjoxNjgzNzM0ODgwfQ----aed1bf9463c9f50b7688dfaef94a4c5e73a91b59',
        'session_username': 'BALAGAEZYANVD',
        'debug': 'null',
        'session_id': session_id
    }
    session = requests.Session()
    response = session.put(url, data=data, cookies=cookies)# headers=headers)#, json=data)
    #print('Status Code:', response.status_code)
    #print('Response Headers:', response.headers)
    #print('Response Headers Type:', type(response.headers))

    try:
        print(response.headers['Set-Cookie'])
    except:
        print(response.text)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    }
    cookies = session.cookies.get_dict()
    old_cookies = {
        'session_username': 'BALAGAEZYANVD',
        'debug': 'null',
        'session_id': session_id
    }
    cookies.update(old_cookies)
    #print(cookies)
    data = {
        'procname': 'docs_find',
        'itid': '1666',
        'get_data_table': '1',
        'search-tid-31134': 'да',
        'offset': '0',
        'count': '400',
        'sort': ''
    }
    response = session.post('https://rgis.mosreg.ru/int/app/mio/docs/1666/', headers=headers, cookies=cookies, data=data)
    return response.text

def replace_text(df, columns):
    df.to_excel(os.path.join(workdir, 'df.xlsx'), index=False)

    print(df)
    try:
        for col in columns:
            try:
                df[col] = df[col].str.replace('Значение: ', '')
                df[col] = df[col].str.replace(' мг/м³', '')
            except Exception as err:
                logging.error(f'Replace error: {err}')
    except Exception as error:
        print(f'Ошибка обработки значений: {error}')
        #df[col] = df[col].apply(lambda x: '' if x.strip() == 'Знач' else x)
    return df

import re

def save_admin_devices_table():
    from bs4 import BeautifulSoup

    session_id = get_admin_token()
    print('session_id = ', session_id)
    html = get_admin_session(session_id)
    #print(html)
    soup = BeautifulSoup(html, 'html.parser')
    result = []
    pattern = re.compile(r'\b\d+\.\d+\b')

    # TODO TRY-EXCEPT 504 error
    try:
        dtime = soup.select('table.table > tbody > tr:nth-child(1) > td:nth-child(2) > div > div > div:nth-child(5) > span')[0].text
        logging.info(f'dtime: {dtime}') #   18.05.23 16:02:36
        rows = soup.select('table.table > tbody > tr')
        logging.info(f'rows: {len(rows)}\n{type(rows)}')
        with open(f'{str(int(time.time()))}_rnox_rows.txt', "w", encoding='utf8') as outfile:
            outfile.write(str(rows))
        for i, row in enumerate(rows):
            try:
                device_name = str(row.select('td:nth-child(3) > div.control_height > span')[0].text).strip()
                device_status = str(row.select('td:nth-child(6) > div.control_height > span')[0].text).strip()
                device_sensors = row.select('td:nth-child(9) > div.control_height > span')[0].text
                device_ch2o = str(pattern.findall(row.select('td:nth-child(8) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(8) > div.control_height > span')[0].text) else ''
                device_so2 = str(pattern.findall(row.select('td:nth-child(9) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(9) > div.control_height > span')[0].text) else ''
                device_co = str(pattern.findall(row.select('td:nth-child(10) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(10) > div.control_height > span')[0].text) else ''
                device_h2s = str(pattern.findall(row.select('td:nth-child(11) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(11) > div.control_height > span')[0].text) else ''
                device_pm10 = str(pattern.findall(row.select('td:nth-child(12) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(12) > div.control_height > span')[0].text) else ''
                device_no2 = str(pattern.findall(row.select('td:nth-child(13) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(13) > div.control_height > span')[0].text) else ''
                device_nh3 = str(pattern.findall(row.select('td:nth-child(14) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(14) > div.control_height > span')[0].text) else ''
                device_pm25 = str(pattern.findall(row.select('td:nth-child(15) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(15) > div.control_height > span')[0].text) else ''
                device_no = str(pattern.findall(row.select('td:nth-child(16) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(16) > div.control_height > span')[0].text) else ''
                print(f'{device_name}-{device_status}-{device_ch2o}-{device_so2}-{device_co}-{device_h2s}-{device_pm10}-{device_no2}-{device_nh3}-{device_pm25}-{device_no}')

                #result.append({'device_name': device_name, 'device_status': device_status, 'device_sensors': device_sensors, 'CH2O': device_ch2o, 'SO2': device_so2, 'CO': device_co, 'H2S': device_h2s, 'PM10': device_pm10, 'NO2': device_no2, 'NH3': device_nh3, 'PM25': device_pm25, 'NO': device_no})
                if device_status == 'да' or device_status != 'да':
                    result.append({'device_name': device_name, 'CH2O': device_ch2o, 'SO2': device_so2, 'CO': device_co, 'H2S': device_h2s, 'PM10': device_pm10, 'NO2': device_no2, 'NH3': device_nh3, 'PM25': device_pm25, 'NO': device_no})
            except Exception as err:
                logging.error(f'Parsing table error:  {err}')
        #workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
        devices_fname = os.path.join(workdir, timestamp_to_filename_prefix()+'_rgis_admin_devices_table.txt')

        with open(devices_fname, "w") as outfile:
            outfile.write(json.dumps(result, indent=2))

        with open(devices_fname, newline='', encoding='utf-8') as json_file:
            json_text = ''.join(x for x in json_file.readlines())
            json_data = json.loads(json_text)

        # saving messages json to XLSX
        devices_frame = pd.read_json(json.dumps(json_data), orient='records')
        columns = ['CH2O', 'SO2', 'CO', 'H2S', 'PM10', 'NO2', 'NH3', 'PM25', 'NO']
        #devices_frame_clear = replace_text(devices_frame, columns)
        devices_frame_clear = devices_frame
        dataset_xlsx_path = devices_fname.replace('.txt', '.xlsx')
        xls_writer = pd.ExcelWriter(dataset_xlsx_path)
        devices_frame_clear.to_excel(xls_writer, index=None, sheet_name='messages')
        xls_writer.close()
    except Exception as error:
        logging.error(f'Undefined RGIS page: {error}')
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        html_path = os.path.join(workdir, f'{timestamp}_rgis_html.txt')
        with open(html_path, "w") as outfile:
            outfile.write(html)
        html_title = soup.select('title')
        logging.error(f'______RGIS page title: {html_title[0].text}')
        dataset_xlsx_path = html_title[0].text
    return dataset_xlsx_path

def dev_save_admin_devices_table():
    from bs4 import BeautifulSoup

    #
    #print('session_id = ', session_id)
    #
    #print(html)
    #
    result = []
    pattern = re.compile(r'\b\d+\.\d+\b')

    if False:
        with open('1719842079_rnox_rows.txt', 'r', encoding='UTF8') as file:
            content = file.read()
        print(content)
        soup = BeautifulSoup(content, 'html.parser')
    else:
        session_id = get_admin_token()
        html = get_admin_session(session_id)
        soup = BeautifulSoup(html, 'html.parser')

    # TODO TRY-EXCEPT 504 error
    try:
        dtime = soup.select('table.table > tbody > tr:nth-child(1) > td:nth-child(2) > div > div > div:nth-child(5) > span')[0].text
        logging.info(f'dtime: {dtime}') #   18.05.23 16:02:36
        rows = soup.select('table.table > tbody > tr')
        logging.info(f'rows: {len(rows)}\n{type(rows)}')
        with open(f'{str(int(time.time()))}_rnox_rows.txt', "w", encoding='utf8') as outfile:
            html_data = '\n'.join(x.text for x in rows)
            outfile.write(html_data)
        for i, row in enumerate(rows):
            try:
                device_name = str(row.select('td:nth-child(3) > div.control_height > span')[0].text).strip()
                device_status = str(row.select('td:nth-child(6) > div.control_height > span')[0].text).strip()
                device_sensors = row.select('td:nth-child(9) > div.control_height > span')[0].text
                device_ch2o = str(pattern.findall(row.select('td:nth-child(8) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(8) > div.control_height > span')[0].text) else ''
                device_so2 = str(pattern.findall(row.select('td:nth-child(9) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(9) > div.control_height > span')[0].text) else ''
                device_co = str(pattern.findall(row.select('td:nth-child(10) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(10) > div.control_height > span')[0].text) else ''
                device_h2s = str(pattern.findall(row.select('td:nth-child(11) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(11) > div.control_height > span')[0].text) else ''
                device_pm10 = str(pattern.findall(row.select('td:nth-child(12) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(12) > div.control_height > span')[0].text) else ''
                device_no2 = str(pattern.findall(row.select('td:nth-child(13) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(13) > div.control_height > span')[0].text) else ''
                device_nh3 = str(pattern.findall(row.select('td:nth-child(14) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(14) > div.control_height > span')[0].text) else ''
                device_pm25 = str(pattern.findall(row.select('td:nth-child(15) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(15) > div.control_height > span')[0].text) else ''
                device_no = str(pattern.findall(row.select('td:nth-child(16) > div.control_height > span')[0].text)[0]) if pattern.findall(row.select('td:nth-child(16) > div.control_height > span')[0].text) else ''
                print(f'{device_name}-{device_status}-{device_ch2o}-{device_so2}-{device_co}-{device_h2s}-{device_pm10}-{device_no2}-{device_nh3}-{device_pm25}-{device_no}')

                #result.append({'device_name': device_name, 'device_status': device_status, 'device_sensors': device_sensors, 'CH2O': device_ch2o, 'SO2': device_so2, 'CO': device_co, 'H2S': device_h2s, 'PM10': device_pm10, 'NO2': device_no2, 'NH3': device_nh3, 'PM25': device_pm25, 'NO': device_no})
                if device_status == 'да' or device_status != 'да':
                    result.append({'device_name': device_name, 'CH2O': device_ch2o, 'SO2': device_so2, 'CO': device_co, 'H2S': device_h2s, 'PM10': device_pm10, 'NO2': device_no2, 'NH3': device_nh3, 'PM25': device_pm25, 'NO': device_no})
            except Exception as err:
                logging.error(f'Parsing table error:  {err}')
        #workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
        devices_fname = os.path.join(workdir, timestamp_to_filename_prefix()+'_rgis_admin_devices_table.txt')

        with open(devices_fname, "w") as outfile:
            outfile.write(json.dumps(result, indent=2))

        with open(devices_fname, newline='', encoding='utf-8') as json_file:
            json_text = ''.join(x for x in json_file.readlines())
            json_data = json.loads(json_text)

        # saving messages json to XLSX
        devices_frame = pd.read_json(json.dumps(json_data), orient='records')
        columns = ['CH2O', 'SO2', 'CO', 'H2S', 'PM10', 'NO2', 'NH3', 'PM25', 'NO']
        #devices_frame_clear = replace_text(devices_frame, columns)
        devices_frame_clear = devices_frame
        dataset_xlsx_path = devices_fname.replace('.txt', '.xlsx')
        xls_writer = pd.ExcelWriter(dataset_xlsx_path)
        devices_frame_clear.to_excel(xls_writer, index=None, sheet_name='messages')
        xls_writer.close()
    except Exception as error:
        logging.error(f'Undefined RGIS page: {error}')
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
#        html_path = os.path.join(workdir, f'{timestamp}_rgis_html.txt')
#        with open(html_path, "w") as outfile:
#            outfile.write(html)
        html_title = soup.select('title')
        logging.error(f'______RGIS page title: {html_title[0].text}')
        dataset_xlsx_path = html_title[0].text
    return dataset_xlsx_path

def request_and_compare_tables():
    #admin_dataset_xlsx_path = save_admin_devices_table()
    #public_dataset_xlsx_path = save_public_devices_table()
    regenerate_tables = False
    today_prefix = timestamp_to_filename_prefix()
    if not os.path.exists(os.path.join(workdir, today_prefix + '_rgis_admin_devices_table.xlsx')) and regenerate_tables:
        admin_dataset_xlsx_path = save_admin_devices_table()
    else:
        admin_dataset_xlsx_path = os.path.join(workdir, today_prefix + '_rgis_admin_devices_table.xlsx')

    if not os.path.exists(os.path.join(workdir, today_prefix + 'rgis_public_devices_table.xlsx')) and regenerate_tables:
        public_dataset_xlsx_path = save_public_devices_table()
    else:
        public_dataset_xlsx_path = os.path.join(workdir, today_prefix + 'rgis_public_devices_table.xlsx')

    #admin_dataset_xlsx_path = os.path.join(workdir, '1683877644_rgis_admin_devices_table.xlsx')
    #public_dataset_xlsx_path = os.path.join(workdir, '1683877767_rgis_public_devices_table.xlsx')

    df1 = pd.read_excel(admin_dataset_xlsx_path, index_col='device_name')
    df1.sort_values(by=['device_name'])

    df2 = pd.read_excel(public_dataset_xlsx_path, index_col='device_name')
    df2.sort_values(by=['device_name'])

    #print(df1.head())
    #print(df2.head())

    df_merged = df1.merge(df2, on='device_name', suffixes=('_1', '_2'))

    #print(df_merged.head())


    df_compared = df_merged[['CH2O_1', 'CH2O_2', 'SO2_1', 'SO2_2', 'CO_1', 'CO_2', 'H2S_1', 'H2S_2', 'PM10_1', 'PM10_2', 'NO2_1', 'NO2_2', 'NH3_1', 'NH3_2', 'PM25_1', 'PM25_2', 'NO_1', 'NO_2']]
    devices_fname = os.path.join(workdir, timestamp_to_filename_prefix()+'_rgis_admin_public_devices_diff.xlsx')

    df_diff = df_merged
    for col in ['CH2O', 'SO2', 'CO', 'H2S', 'PM10', 'NO2', 'NH3', 'PM25', 'NO']:
        diff_col_name = f'{col}_diff'
        df_diff[diff_col_name] = df_diff[f'{col}_1'] - df_diff[f'{col}_2']
        df_diff.drop([f'{col}_1', f'{col}_2'], axis=1, inplace=True)

    xls_writer = pd.ExcelWriter(devices_fname)
    df_compared.to_excel(xls_writer, index='device_name', sheet_name='compare')
    df_diff.to_excel(xls_writer, index='device_name', sheet_name='difference')
    xls_writer.close()





def save_admin_table_pdk_json(admin_dataset_xlsx_path):
    #admin_dataset_xlsx_path = os.path.join(workdir, '2023_05_12_rgis_admin_devices_table.xlsx')
    df = pd.read_excel(admin_dataset_xlsx_path, index_col='device_name')
    #df.reset_index(inplace=True)

    pdk_CH2O = 0.01
    pdk_SO2 = 0.05
    pdk_CO = 3
    pdk_H2S = 0.15
    pdk_PM10 = 0.06
    pdk_NO2 = 0.1
    pdk_NH3 = 0.1
    pdk_PM25 = 0.035
    pdk_NO = 0.06

    df['CH2O'] = pd.to_numeric(df['CH2O'], errors='coerce')
    df['SO2'] = pd.to_numeric(df['SO2'], errors='coerce')
    df['CO'] = pd.to_numeric(df['CO'], errors='coerce')
    df['H2S'] = pd.to_numeric(df['H2S'], errors='coerce')
    df['PM10'] = pd.to_numeric(df['PM10'], errors='coerce')
    df['NO2'] = pd.to_numeric(df['NO2'], errors='coerce')
    df['NH3'] = pd.to_numeric(df['NH3'], errors='coerce')
    df['PM25'] = pd.to_numeric(df['PM25'], errors='coerce')
    df['NO'] = pd.to_numeric(df['NO'], errors='coerce')

    new_df = df.loc[(df['CH2O'] >= pdk_CH2O) |
                    (df['SO2'] >= pdk_SO2) |
                    (df['CO'] >= pdk_CO) |
                    (df['H2S'] >= pdk_H2S) |
                    (df['PM10'] >= pdk_PM10) |
                    (df['NO2'] >= pdk_NO2) |
                    (df['NH3'] >= pdk_NH3) |
                    (df['PM25'] >= pdk_PM25) |
                    (df['NO'] >= pdk_NO)].copy()

    new_df.reset_index(inplace=True)

    new_df.loc[new_df['CH2O'] >= pdk_CH2O, 'CH2O'] = round(new_df['CH2O'].astype(float) / pdk_CH2O, 2)
    new_df.loc[new_df['SO2'] >= pdk_SO2, 'SO2'] = round(new_df['SO2'].astype(float) / pdk_SO2, 2)
    new_df.loc[new_df['CO'] >= pdk_CO, 'CO'] = round(new_df['CO'].astype(float) / pdk_CO, 2)
    new_df.loc[new_df['H2S'] >= pdk_H2S, 'H2S'] = round(new_df['H2S'].astype(float) / pdk_H2S, 2)
    new_df.loc[new_df['PM10'] >= pdk_PM10, 'PM10'] = round(new_df['PM10'].astype(float) / pdk_PM10, 2)
    new_df.loc[new_df['NO2'] >= pdk_NO2, 'NO2'] = round(new_df['NO2'].astype(float) / pdk_NO2, 2)
    new_df.loc[new_df['NH3'] >= pdk_NH3, 'NH3'] = round(new_df['NH3'].astype(float) / pdk_NH3, 2)
    new_df.loc[new_df['PM25'] >= pdk_PM25, 'PM25'] = round(new_df['PM25'].astype(float) / pdk_PM25, 2)
    new_df.loc[new_df['NO'] >= pdk_NO, 'NO'] = round(new_df['NO'].astype(float) / pdk_NO, 2)

    new_df.loc[new_df['CH2O'] < pdk_CH2O, 'CH2O'] = ''
    new_df.loc[new_df['SO2'] < pdk_SO2, 'SO2'] = ''
    new_df.loc[new_df['CO'] < pdk_CO, 'CO'] = ''
    new_df.loc[new_df['H2S'] < pdk_H2S, 'H2S'] = ''
    new_df.loc[new_df['PM10'] < pdk_PM10, 'PM10'] = ''
    new_df.loc[new_df['NO2'] < pdk_NO2, 'NO2'] = ''
    new_df.loc[new_df['NH3'] < pdk_NH3, 'NH3'] = ''
    new_df.loc[new_df['PM25'] < pdk_PM25, 'PM25'] = ''
    new_df.loc[new_df['NO'] < pdk_NO, 'NO'] = ''

    #new_df['combined_values'] = df.apply(lambda x: ", ".join([f"{col}:{x[col]}" for col in df.columns[1:] if pd.notnull(x[col])]), axis=1)
    import numpy as np

    for col in new_df.columns[1:]:
        new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
        new_df[col] = np.where(new_df[col].notnull(), new_df[col], '')

    result = []
    for index, row in new_df.iterrows():
        for i, column in enumerate(new_df.columns):
            if i > 0:
                if pd.notna(row[column]) and len(row[column]):
                    result.append({'device_name': row['device_name'], f'{column}': row[column]})
    devices_fname = os.path.join(workdir, timestamp_to_filename_prefix()+'_rgis_admin_pdk.txt')
    with open(devices_fname, "w") as outfile:
        outfile.write(json.dumps(result, indent=2))

    #print(new_df)
    devices_fname_xlsx = os.path.join(workdir, timestamp_to_filename_prefix()+'_rgis_admin_pdk.xlsx')
    xls_writer = pd.ExcelWriter(devices_fname_xlsx)
    new_df.to_excel(xls_writer, index=None, sheet_name='pdk')
    return devices_fname

def parse_admin_pdk_to_string(regenerate_tables):
    #regenerate_tables = False
    today_prefix = timestamp_to_filename_prefix()
    if not os.path.exists(os.path.join(workdir, today_prefix + '_rgis_admin_devices_table.xlsx')) or regenerate_tables:
        admin_dataset_xlsx_path = save_admin_devices_table()
    else:
        admin_dataset_xlsx_path = os.path.join(workdir, today_prefix + '_rgis_admin_devices_table.xlsx')
    if os.path.isfile(admin_dataset_xlsx_path):
        admin_table_pdk_json_path = save_admin_table_pdk_json(admin_dataset_xlsx_path)
        with open(admin_table_pdk_json_path, newline='', encoding='utf-8') as json_file:
            json_text = ''.join(x for x in json_file.readlines())
            json_data = json.loads(json_text)
        table = []
        for item in json_data:
            keys_list = list(item.keys())
            table.append(f"{item['device_name']} ⋮ {keys_list[1]} ⋮ {item[keys_list[1]]}")
        table = '\n'.join(x for x in table)
        #print(table)
        result = (True, table)
    else:
        #admin_dataset_xlsx_path == 'NA':
        #return admin_dataset_xlsx_path
        result = (False, admin_dataset_xlsx_path)
    return result

def diff(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif

async def get_rnox_is_work():
    save_result = False
    url = 'http://r-nox.eu:8089/api/group/iswork/3cd326fc-fd7e-4305-9d3a-5e50307ab90b'
    headers = {"Content-Type": "application/json"}
    #   EXCLUDED
    #   ,'AN0183','AN0184','AN0185','AN0186','AN0189','AN0190','AN0276'
    body = {'Devices': ['AN0011','AN0012','AN0013','AN0014','AN0015','AN0016','AN0017','AN0018','AN0019','AN0020','AN0021','AN0022','AN0023','AN0024','AN0025','AN0031','AN0032','AN0033','AN0034','AN0035','AN0036','AN0037','AN0038','AN0039','AN0040','AN0042','AN0043','AN0044','AN0045','AN0046','AN0047','AN0048','AN0049','AN0050','AN0051','AN0052','AN0053','AN0054','AN0055','AN0056','AN0057','AN0058','AN0059','AN0060','AN0061','AN0062','AN0063','AN0064','AN0065','AN0066','AN0067','AN0068','AN0069','AN0070','AN0071','AN0072','AN0073','AN0074','AN0075','AN0076','AN0077','AN0078','AN0079','AN0080','AN0081','AN0088','AN0089','AN0090','AN0091','AN0092','AN0093','AN0094','AN0095','AN0096','AN0097','AN0098','AN0099','AN0100','AN0101','AN0102','AN0103','AN0104','AN0105','AN0106','AN0107','AN0108','AN0109','AN0110','AN0111','AN0112','AN0113','AN0114','AN0115','AN0116','AN0117','AN0118','AN0119','AN0120','AN0121','AN0122','AN0123','AN0124','AN0125','AN0126','AN0127','AN0128','AN0129','AN0130','AN0131','AN0132','AN0133','AN0134','AN0135','AN0136','AN0137','AN0138','AN0139','AN0140','AN0141','AN0142','AN0143','AN0144','AN0145','AN0146','AN0147','AN0148','AN0149','AN0150','AN0151','AN0152','AN0153','AN0154','AN0155','AN0156','AN0157','AN0158','AN0159','AN0160','AN0161','AN0162','AN0163','AN0164','AN0165','AN0166','AN0167','AN0168','AN0169','AN0170','AN0171','AN0172','AN0178','AN0179','AN0180','AN0181','AN0182','AN0191','AN0192','AN0193','AN0194','AN0195','AN0196','AN0197','AN0198','AN0199','AN0200','AN0201','AN0202','AN0203','AN0204','AN0205','AN0206','AN0207','AN0208','AN0209','AN0210','AN0211','AN0212','AN0213','AN0214','AN0215','AN0216','AN0217','AN0218','AN0219','AN0220','AN0221','AN0222','AN0223','AN0224','AN0225','AN0226','AN0227','AN0228','AN0229','AN0230','AN0231','AN0232','AN0233','AN0234','AN0235','AN0236','AN0237','AN0238','AN0239','AN0240','AN0241','AN0242','AN0243','AN0244','AN0245','AN0246','AN0247','AN0248','AN0249','AN0250','AN0251','AN0252','AN0253','AN0254','AN0255','AN0256','AN0257','AN0258','AN0259','AN0260','AN0261','AN0262','AN0263','AN0264','AN0265','AN0266','AN0267','AN0268','AN0269','AN0270','AN0276','AN0278','AN0279','AN0280','AN0281','AN0282','AN0283','AN0284','AN0285','AN0286','AN0287','AN0288','AN0289','AN0290','AN0291','AN0292','AN0293','AN0294','AN0295','AN0296','AN0297','AN0298','AN0299','AN0300','AN0301','AN0302','AN0303','AN0304','AN0305','AN0306','AN0307','AN0308','AN0309','AN0310','AN0311','AN0312','AN0313','AN0314','AN0315','AN0316','AN0317','AN0318','AN0319','AN0320','AN0321','AN0322','AN0323','AN0324','AN0325','AN0326']}
    response = requests.post(url, headers=headers, json=body)
    devices_send = body['Devices']
    devices_false = []
    devices_true = []
    if response.status_code == 200:
        response_json = response.json()
        devices_recieved = list(response_json["Devices"].keys())
        devices_empty = diff(devices_send, devices_recieved)
        print(diff(devices_send, devices_recieved))
        for device in devices_recieved:
            if response_json["Devices"][device]:
                devices_true.append(device)
            else:
                devices_false.append(device)
        #logging.info(f"\ntrue: {devices_true}\nfalse: {devices_false}\nempty: {devices_empty}\n")
        response_json = {'true': devices_true, 'false': devices_false, 'empty': devices_empty}
        if save_result:
            with open(f'{str(int(time.time()))}_rnox_iswork.txt', "w") as outfile:
                outfile.write(json.dumps(response_json, indent=2))
    else:
        response_json = {'status_code': response.status_code}
        logging.error(f'Response status_code: {response.status_code}')
    return response_json
#get_rnox_is_work()

if __name__ == '__main__':
    dev_save_admin_devices_table()
    # save_admin_devices_table()
    #paths = get_semos_document()
    #print(paths)