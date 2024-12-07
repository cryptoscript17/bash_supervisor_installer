import json, requests, time, math, logging, asyncio
import os, shutil
import psycopg2
import asyncpg
import re

from datetime import datetime, timedelta, timezone 
from aiogram.utils import executor
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import exceptions
from aiogram.types import ChatType, ParseMode, ContentTypes

from _gis_get_devices import get_dataset_xlsx_path_local, get_device_snum_by_name, get_snum_by_device_id, get_device_id_by_snum
# from _gis_get_devices import request_devices_dataset_json
from moem_pdk_db import base_60_connection_params, factories_closest_full_query
from moem_pdk_db import get_closest_factories_by_device_id

msk_tz = timezone(timedelta(hours=3), name='Europe/Moscow')

dataset_xlsx_path_local = get_dataset_xlsx_path_local()

#tg_token = '1918973108:AAG0dsKBLIml4WDhNMmLHbdZm3l5DGbpt7g' #instagradus
tg_token = '5452457434:AAGCHSkp1WVwC8Rje2S3pEc88gA4UlJKbAo' #pdk_watchdog
chat_ids = [-828958519, -1001318085718]

IS_SORTED_DEVICES = True
IS_TEST = False

BACKWARD_TIMEOUT = 0
LOOP_TIMEOUT = 1200

IS_WINDOWS = True
IS_DEV = True

LOOP_CHECK_ECOMON_TIMEOUT = 600
LOOP_CHECK_TABLE_TIMEOUT = 1200


d = ['0010', '0301', '0303', '0337', '0328', '0330', '0333', '0304', '0342', '0349']
ISOLATION_LEVEL_AUTOCOMMIT = True
IS_NVOS_ACTIVE = False
FACTORIES_LIMIT = 8

# Initialize bot and dispatcher
bot = Bot(token=tg_token)

dp = Dispatcher(bot)

logging.basicConfig(level=logging.INFO)

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
temp_dir = os.path.join(workdir, 'pdk_data')

local_connection_params = {
    'user': 'postgres',
    'password': 'qAyexo4MEI',
    'host': '127.0.0.1',
    'port': 65432,
    'dbname': 'iot'
}
if not os.path.exists(os.path.join(workdir, temp_dir)):
    os.makedirs(os.path.join(workdir, temp_dir))
#else:
    #shutil.rmtree(os.path.join(workdir, temp_dir))
    #os.makedirs(os.path.join(workdir, temp_dir))

@dp.message_handler(commands=['start', 'help'], content_types=ContentTypes.TEXT)
async def process_commands(message: types.Message):
    chat_id = message.chat.id
    if message.text == '/start':
        response_text = "Привет! Я бот, который отправляет сообщения каждые 20 минут."
    elif message.text == '/help':
        response_text = "Список доступных команд: /start, /help"
        response_text = f"chat_id: {chat_id}\n{response_text}"
    elif str(message.text).upper().startswith('/AN'):
        device_id = get_device_id_by_snum(dataset_xlsx_path_local, str(message.text).upper())
        closest_factories = get_closest_factories_by_device_id(local_connection_params, factories_closest_full_query, device_id)
        response_text = f"Для поста [{device_id}|{str(message.text).upper()}] доступны ОНВОЗ:\n{[str(x) for x in closest_factories]}"
    await bot.send_message(chat_id=chat_id, text=response_text)
    logging.info(f"Ответ на команду '{message.text}' отправлен в чат {chat_id}")
# @dp.message_handler(content_types=ContentType.TEXT | ContentType.PHOTO | ContentType.DOCUMENT)
def get_bdate_edate():
    start = time.time()
    bdate_ts = int((math.floor(start / 1200) - 1) * 1200)
    edate_ts = int((math.ceil(start / 1200) - 1) * 1200)
    bdate = str(datetime.fromisoformat(datetime.fromtimestamp(bdate_ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S'))
    edate = str(datetime.fromisoformat(datetime.fromtimestamp(edate_ts).isoformat()).strftime('%Y-%m-%dT%H:%M:%S'))
    return (bdate, edate)

def filter_dict(dictionary):
    allowed_keys = ['bdate', 'device_id', 'sensor_id', 'paramtype_code', 'paramtype_pdk', 'sensorvalue', 'device_name']
    return {k: v for k, v in dictionary.items() if k in allowed_keys}

def remove_newlines(text):
    return text.replace('\n', ' ').replace('\r', '').replace('\t', ' ').strip()

def remove_empty_lines(text):
    return re.sub('[\n\r\t]+', ' ', text).strip()

def return_datetime_string(delim = None) -> str:
    current_date_time = datetime.now()
    short_date_time = current_date_time.strftime(f"%Y-%m-%d %H:%M:%S")
    return short_date_time

def execute_query(query, timeout_duration = None):
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
        timeout_duration=60
    
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
                            query = f"{query[0:len(query) - 1]} ON CONFLICT DO NOTHING;" if query.endswith(';') else f"{query} ON CONFLICT DO NOTHING;"
                            cursor.execute(query)
                            conn.commit()
                            return True
            return run_insert_query()

    except TimeoutError:
        print(f"{return_datetime_string()}\tERROR:\t\tВремя выполнения запроса превысило {timeout_duration} секунд. Запрос отменен.")
        return None
    except psycopg2.Error as e:
        print(f"{return_datetime_string()}\tERROR:\t\tОшибка выполнения запроса: {e}, QUERY:\n{query[0:9]}")
        return None
            
def last_samples_list_to_messages(snum_list):
    devices_samples_json = [] 
    last_samples_json = []
    table = []
    dataset_xlsx_path_local = get_dataset_xlsx_path_local()
    for snum in snum_list:
        device_sample_paramtypes, device_sample_paramtypes_bdates = [], []
        last_samples_list = select_last_samples(limit = 100, where_value = snum)
        table.append([f"Заводской номер: [{snum}]"])
        for last_sample_tuple in last_samples_list:
            last_sample_json = {
                'pdk_id': last_sample_tuple[0], 
                'send_status': last_sample_tuple[1], 
                'bdate': last_sample_tuple[2], 
                'device_id': last_sample_tuple[3], 
                'sensor_id': last_sample_tuple[4], 
                'device_name': last_sample_tuple[5], 
                'sensorvalue': last_sample_tuple[6], 
                'paramtype_pdk': last_sample_tuple[7], 
                'paramtype_code': last_sample_tuple[8]
            }
            if last_sample_json['paramtype_code'] not in device_sample_paramtypes:
                device_sample_paramtypes.append(last_sample_json['paramtype_code'])
            
            if last_sample_json not in last_samples_json:
                last_samples_json.append(last_sample_json)
        # print(device_sample_paramtypes)
        device_sample_json = []
        for paramtype_code in device_sample_paramtypes:
            paramtype_code_bdates_json = []
            device_sample_paramtypes_bdates.append({snum : {paramtype_code: []}})
            sensor_bdates = []
            table.append(f"{last_sample_json['device_name']} | [{paramtype_code}]")
            for index, last_sample_json in enumerate(last_samples_json):
                if last_sample_json['paramtype_code'] == paramtype_code:
                    paramtype_code_bdates_json.append(last_sample_json)
                    device_id = last_sample_json['device_id']
                    device_name = last_sample_json['device_name']
                    snum = get_snum_by_device_id(dataset_xlsx_path_local, device_id)
                    bdate = last_sample_json['bdate'].strftime("%Y-%m-%d %H:%M")
                    paramtype_code = last_sample_json['paramtype_code']
                    sensorvalue = float(last_sample_json['sensorvalue'])
                    sensor_bdates.append(bdate)
                if f"{bdate} | {snum} | {paramtype_code} | {sensorvalue}\n" not in table:
                    table.append(f"{bdate} | {snum} | {paramtype_code} | {sensorvalue}")
            paramtype_code_bdates_sorted_json = sorted(paramtype_code_bdates_json, key=paramtype_code_bdates_json['bdate'])
            # paramtype_code_bdates_sorted_json = sorted(paramtype_code_bdates_json, key=lambda x: datetime.strptime(x['bdate'], '%Y-%m-%d %H:%M'))
        device_sample_json.append(paramtype_code_bdates_sorted_json)
    table = '\n'.join(str(x) for x in table)
    print(device_sample_json)

create_table_query = """
    CREATE TABLE IF NOT EXISTS iot.pdk_sample

    (
        pdk_id SERIAL PRIMARY KEY,
        sample TEXT UNIQUE
    );

    COMMENT ON TABLE iot.pdk_sample
        IS 'Sensor values registry with PDK filter';
"""

select_parsed_dataset_query = """
    SELECT 
        pdk_id,
        send_status,
        replace((sample::json->'bdate')::text, '"', '')::timestamp AT TIME ZONE 'Europe/Moscow' AS bdate,
        REGEXP_REPLACE(COALESCE((sample::json->'device_id')::text, '0'), '[^0-9]*' ,'0')::integer AS device_id,
        REGEXP_REPLACE(COALESCE((sample::json->'sensor_id')::text, '0'), '[^0-9]*' ,'0')::integer AS sensor_id,
        unistr(replace((sample::json->'device_name')::text, '"', '')) AS device_name,
        to_number((sample::json->'sensorvalue')::text, '999.999') AS sensorvalue,
        to_number((sample::json->'paramtype_pdk')::text, '999.999') AS paramtype_pdk,
        unistr(replace((sample::json->'paramtype_code')::text, '"', '')) AS paramtype_code
    FROM iot.pdk_sample
        -- WHERE to_timestamp((sample::json->'bdate')::text, 'YYYY-mm-dd"T"HH:MM:SS') > '2024-06-25 00:00:00' AND to_timestamp((sample::json->'bdate')::text, 'YYYY-mm-dd"T"HH:MM:SS') < '2024-06-25 23:59:59'
        -- WHERE (sample::json->'device_id')::text = '305'
        WHERE REGEXP_REPLACE(COALESCE((sample::json->'device_id')::text, '0'), '[^0-9]*' ,'0')::integer = 305
    ORDER BY pdk_id DESC;
"""

def select_last_samples(limit, where_value, select_type = 'BY_ID'):
    
    if select_type == 'BY_ID':
        device_id = int(where_value)
        where_condition = f"WHERE REGEXP_REPLACE(COALESCE((sample::json->'device_id')::text, '0'), '[^0-9]*' ,'0')::integer IN ({device_id})"    
    elif select_type == 'BY_NAME':
        device_name = remove_empty_lines(where_value)
        name = f"unistr(replace((sample::json->'device_name')::text, '{chr(34)}', ''))"
        where_condition = f"WHERE UPPER({name}) LIKE UPPER('%{device_name}%')"
    elif select_type == 'BY_DATE':
        bdate = where_value
        where_condition = ""

    select_parsed_dataset_query = f"""
        SELECT 
            pdk_id,
            send_status,
            replace((sample::json->'bdate')::text, '"', '')::timestamp AT TIME ZONE 'Europe/Moscow' AS bdate,
            REGEXP_REPLACE(COALESCE((sample::json->'device_id')::text, '0'), '[^0-9]*' ,'0')::integer AS device_id,
            REGEXP_REPLACE(COALESCE((sample::json->'sensor_id')::text, '0'), '[^0-9]*' ,'0')::integer AS sensor_id,
            unistr(replace((sample::json->'device_name')::text, '"', '')) AS device_name,
            to_number((sample::json->'sensorvalue')::text, '9999.999') AS sensorvalue,
            to_number((sample::json->'paramtype_pdk')::text, '9999.999') AS paramtype_pdk,
            unistr(replace((sample::json->'paramtype_code')::text, '"', '')) AS paramtype_code
        FROM iot.pdk_sample
            {where_condition}
        ORDER BY pdk_id DESC LIMIT {limit};
    """
    select_parsed_dataset_result = execute_query(remove_empty_lines(select_parsed_dataset_query), 60)
    # logging.info(f"SELECT_QUERY:\n{select_parsed_dataset_query}")
    # logging.info(select_parsed_dataset_result)
    return select_parsed_dataset_result

air_pollutant_codes = """
'0301': 'NO₂',
'0303': 'NH₃',
'0304': 'NO',
'0326': 'O₃',
'0330': 'SO₂',
'0333': 'H₂S',
'0337': 'CO',
'0415': 'C1H4 - C5H12',
'0416': 'C6H14 - C10H22',
'0417': 'C₂H₆'
"""

facts_closest_full_query = """
SELECT 
    nvosobj.onv_id as nvos_id,
	nvosobj.registry_category as registry_category,
    nvosobj.name as nvos_name,
    nvosobj.address as nvos_address,
    nvosobj.oktmo as nvos_oktmo,
    nvoscrit.id as criteria_id,
    nvoscrit.name as criteria_name,
    nvoscrit.parent_id as criteria_parent_id,
    nvoscrit.title as criteria_title,
    nvoscrit.tag as criteria_tag,
    nvosstat.air_pollutant_code as air_code,
    nvosstat.sum as air_sum,
    nvosstat.name as air_name,
    nvoswast.sum as waste_sum,
    nvosdisc.water_pollutant_code,
    nvosdisc.sum as aqua_sum,
    nvosdisc.comment as aqua_comment,
    nvosdisc.name as aqua_name,
    ST_DistanceSphere(
        ST_MAKEPOINT(nvosobj.longitude, nvosobj.latitude),
        ST_MAKEPOINT(d.lon, d.lat)
    )::integer as dist_meters
FROM 
    base_60.nvos.objects nvosobj
JOIN 
    base_60.air.device d
ON
    d.id = %s AND nvosobj.registry_category IN (2, 1)
FULL JOIN nvos.criteria_nvos nvoscrit ON (nvoscrit.onv_id = nvosobj.onv_id)
FULL JOIN nvos.sources_stationary nvosstat ON (nvosstat.onv_id = nvosobj.onv_id)
FULL JOIN nvos.sources_waste nvoswast ON (nvoswast.onv_id = nvosobj.onv_id)
FULL JOIN nvos.sources_discharges nvosdisc ON (nvosdisc.onv_id = nvosobj.onv_id)
WHERE
	nvosobj.longitude IS NOT NULL AND nvosobj.latitude IS NOT NULL AND d.lat IS NOT NULL AND d.lon IS NOT NULL
	AND nvosstat.air_pollutant_code IN ('0010', '0301', '0303', '0304', '0337', '0328', '0330', '0333', '0342', '0349') 
    AND nvosstat.air_pollutant_code IS NOT NULL
    AND nvosstat.name IS NOT NULL
ORDER BY 
    dist_meters ASC
LIMIT %s;
"""

# PostgreSQL connection pool initialization
async def init_db_pool(conn_params):
    local_dsn = conn_params
    return await asyncpg.create_pool(dsn=local_dsn, min_size=5, max_size=20)
    #    user='your_user',
    #    password='your_password',
    #    database='your_database',
    #    host='your_host',
    #    min_size=5,  # Minimum number of connections in the pool
    #    max_size=20  # Maximum number of connections in the pool
    #)

async def create_poool(conn_params):
    # Create the connection pool
    pool = await asyncpg.create_pool(
        user='postgres',
        password='qAyexo4MEI',
        host='127.0.0.1',
        port=65432,
        database='iot',
        min_size=5,
        max_size=20
    )
    return pool

# Async function to execute SELECT query and fetch all results
async def select_data(pool, query, *params):
    async with pool.acquire() as connection:
        # Prepare the query execution
        result = await connection.fetch(query, *params)
        return result
    
# Async function to handle PostgreSQL operations
async def insert_data(pool, query):
    async with pool.acquire() as connection:
        query = """
            INSERT INTO iot.iot (url, device_id, response_data, is_ready)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
        """
        await connection.execute(query)
        
# Function to execute queries with auto-commit enabled
async def execute_with_autocommit(pool, query, *params):
    async with pool.acquire() as connection:
        # Enable auto-commit mode for this connection
        await connection.execute("SET LOCAL synchronous_commit TO ON;")
        result = await connection.execute(query, *params)
        return result


async def main():
    global local_connection_params
    pool = await create_poool(local_connection_params)  # Init
    select_active_samples_query = """    
    SELECT
        pdk_id as pdk_id,
        (sample::json->'bdate')::text AS bdate,
        (sample::json->'device_id')::text AS device_id,
        (sample::json->'sensor_id')::text AS sensor_id,
        sample as sample_json
    FROM iot.pdk_sample
    WHERE
        send_status = False
    ORDER BY
        bdate ASC
    LIMIT 10;"""
    # active_samples_list = await insert_data(pool, yadrovo_url)
    
    print(f"pool: {pool}")
    # async with init_db_pool(local_connection_params) as pool:
    fetch_results = []
    async with pool.acquire() as connection:
        active_samples_list = await connection.fetch(select_active_samples_query)
        for item in active_samples_list:
            item_dict = {}
            for key, val in item.items():
                item_dict.update({key: val})
            fetch_results.append(item_dict)
            # print(list(item.items()), dir(item.items()))
        # await pool.close()
    # print(f"fetch_results:\t{fetch_results}")
    # quit()
    # quit()
    # active_samples_list = await execute_with_autocommit(pool, select_active_samples_query)
    # return active_samples_list
    sample_json = {}
    samples_json = []
    for query_result in fetch_results:
        try:
            sample_json = {
                'pdk_id': query_result['pdk_id'],
                'bdate': query_result['bdate'],
                'device_id': query_result['device_id'],
                'sensor_id': query_result['sensor_id'],
                'sample_json': query_result['sample_json'],
                'factories': ''
            }
            
            factories_full_query = facts_closest_full_query % (sample_json['device_id'], FACTORIES_LIMIT)
            # print(factories_full_query)
            IS_NVOS_ACTIVE = False
            #if IS_NVOS_ACTIVE:
            #    factories_closest = get_closest_factories_by_device_id(connection_params=base_60_connection_params, query_template=factories_full_query, device_id=sample_json['device_id'])
            #    sample_json['factories'] = factories_closest if len(factories_closest) > 0 else []
            #else:
            #    factories_closest = []
        except Exception as e:
            print(f"Error executing query: {e}\nQUERY: {factories_full_query}")
        if sample_json not in samples_json and len(sample_json) > 0:
            samples_json.append(sample_json)
    else:
        print(f"RESULTS_COUNT:\n{len(samples_json)}|{type(fetch_results)}|{samples_json}")
    
    return samples_json


def select_active_samples():
    """
        SELECT - запрос записей с 'send_status' равным 'False',
        а именно последних записанных значений по превышениям,
        которые ещё не попадали в рассылку.
        
        Дополнительно добавлен вызов функции поиска ближайших
        предприятий с помощью функции 'get_closest_factories_by_device_id'
    """
    lst = []

    select_table_query = """    
    SELECT
        pdk_id as pdk_id,
        (sample::json->'bdate')::text AS bdate,
        (sample::json->'device_id')::text AS device_id,
        (sample::json->'sensor_id')::text AS sensor_id,
        sample as sample_json
    FROM iot.pdk_sample
    WHERE
        send_status = False
    ORDER BY
        bdate ASC
    LIMIT 10;"""

    with psycopg2.connect(**local_connection_params) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            cursor = conn.cursor()
            cursor.execute(select_table_query)
            query_results = cursor.fetchall()
        except Exception as err:
            print(f"Error executing query: {err}\nQUERY: {select_table_query}")
        finally:
            print(f"QUERY OK!")
    
    sample_json = {}
    samples_json = []
    for query_result in query_results:
        try:    
            sample_json = {
                'pdk_id': query_result[0],
                'bdate': query_result[1],
                'device_id': query_result[2],
                'sensor_id': query_result[3],
                'sample': query_result[4],
                'factories': ''
            }
            
            factories_full_query = facts_closest_full_query % (sample_json['device_id'], FACTORIES_LIMIT)
            # print(factories_full_query)
            if IS_NVOS_ACTIVE:
                factories_closest = get_closest_factories_by_device_id(connection_params=base_60_connection_params, query_template=factories_full_query, device_id=sample_json['device_id'])
                sample_json['factories'] = factories_closest if len(factories_closest) > 0 else []
            else:
                factories_closest = []
        except Exception as e:
            print(f"Error executing query: {e}\nQUERY: {factories_full_query}")
        if sample_json not in samples_json and len(sample_json) > 0:
            samples_json.append(sample_json)
    else:
        print(f"RESULTS_COUNT:\n{len(samples_json)}")
    return samples_json


def select_last_sample():
    timeout_duration = 60
    select_table_query = 'SELECT sample FROM iot.pdk_sample ORDER BY pdk_id DESC LIMIT 1'
    query_result = execute_query(select_table_query, timeout_duration)
    if query_result is not None:
        return query_result[0]
    else:
        return False

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

def request_pdk_sensors_history(bdate = None, edate = None):
    stime = time.time()
    token = get_ecomon_token()
    if token is None:
        print(f"Token request error!! Exit...")
        return
    
    url = 'https://dev.ecomon.mosreg.ru/api/json-rpc/BASE.DSAIR_SENSORVALUE'
    headers = {'Content-Type':'application/json', 'Authorization': f'Bearer {token}', 'Origin': 'https://dev.ecomon.mosreg.ru'}
    #   78 - ХПК
    #   77 - NO3 H2O
    body = {
        "jsonrpc": "2.0",
        "method": "getData",
        "params": {
            "bdate": bdate,
            "edate": edate,
            "source_ids": [3,4,5,6],
            "paramtype_ids": [1,2,3,14,15,4,5,6,8,9,10,11,12,13,17,77,78],
            "is_over_pdk": "true",
            "orderby": [
                {
                    "selector": "bdate",
                    "desc": "true"
                }
            ],
            "_config_dataset": "BASE.DSAIR_SENSORVALUE",
            "_config_serverside": "true",
            "_config_is_count": "false",
            "limit": 64,
            "offset": 0
        },
        "id": 1
    }

    logging.info(f'Requesting period is [{bdate} - {edate}]')
    bdates, result_json = [], []
    response = requests.post(url, headers=headers, json=body, timeout=(5,60))
    if response.status_code == 200:
        json_data = response.json()
        try:
            if 'result' in json_data:
                if 'data' in json_data['result']:
                    total_count = len(json_data['result']['data'])
                    if total_count > 0:
                        dataset_xlsx_path = get_dataset_xlsx_path_local()   #   snum import
                        for item in json_data['result']['data']:
                            if item['bdate'] not in bdates:
                                bdates.append(item['bdate'])
                            if 'device_id' in item and os.path.exists(dataset_xlsx_path):
                                item.update({
                                    'snum': get_snum_by_device_id(dataset_xlsx_path, item['device_id'])
                                })
                            else:
                                item.update({
                                    'snum': 'NA'
                                })
                            result_json.append(filter_dict(item))
                        logging.info(f"bdates_list: {bdates}")
                else:
                    logging.info(f'Requesting runtime is ... {str(int(time.time() - stime))} sec.\n')
                    return (False, f"Отсутсвуют ключи 'result', 'data': {json_data['result']}")
            else:
                logging.info('Response JSON empty!')
                print(json_data)
                return (False, f"Отсутсвуют ключи 'result': {json_data}")
        except Exception as e:
            logging.info(f"Response JSON empty! Пустой ответ: {e}")
            return (False, f"Пустой ответ: {e}")
    else:
        logging.error(f'Response status_code error: {response.status_code}')
        time.sleep(1)

    logging.info(f'Runtime is {round(time.time() - stime, 2)} sec.')
    
    return (True, result_json)

text_template = """
⌚ Проверяем интервал: [%s - %s]:
\t\t▫ ▫ ▫ ПДК превышено в [%s].

[ <b>%s</b> ]
[<b>%s</b> | <i>device_id=%s</i>]
[ %s ▫ %s ]
"""




single_template = """[▫ ▫ %s ▫ <b>%s</b> 
▫ <b>%s</b> ▫ <i>id=%s</i>] | [ %s ▫ %s ]\n"""




from aiogram.types import ChatType, ParseMode, ContentTypes

# from beautifultable import BeautifulTable


async def loop_send_last_samples():
    IS_TEST = True
    global chat_ids
    

    while True:
        stime = time.time()
        if 'dataset_xlsx_path' not in locals():
            dataset_xlsx_path = get_dataset_xlsx_path_local()   #   snum import

        #   REQUESTING NEW PDK FROM GIS ECOMON
        bdate_obj = datetime.now() - timedelta(hours=4)
        bdate = bdate_obj.replace(minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
        edate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')         
        insert_results = []
        ecomon_insert_samples_count = 0
        (response_status, result_json) = request_pdk_sensors_history(bdate, edate)
        if response_status:
            logging.info(f"✔️\tGOOD_RESPONSE =)\nresponse_ok: JSON.len: {len(result_json)}, JSON.keys: {list(result_json[0].keys())}")
            for sample_json in result_json:
                try:  
                    send_status = False
                    pdk_tuple = (chr(39)+json.dumps(sample_json)+chr(39), send_status)
                    insert_query = """INSERT INTO iot.pdk_sample (sample, send_status) VALUES (%s, %s);""" % pdk_tuple
                    insert_query_result = execute_query(insert_query, 60)
                    logging.info(f"insert_query_result: [{sample_json['device_id']} | {sample_json['sensor_id']}] {insert_query_result}")
                    insert_results.append(insert_query_result)
                except Exception as e:
                    logging.error(f"Ошибка: {type(e).__name__} - {e}")
                    ecomon_insert_samples_count = -1                
            if ecomon_insert_samples_count is not None:
                if ecomon_insert_samples_count < 0:
                    logging.error(f"Requesting sensors registry error: {ecomon_insert_samples_count}")
            if insert_query_result:
                ecomon_insert_samples_count += len(result_json)
        else:
            logging.error(f"❌\tBAD_RESPONSE !!!\nresponse_status: {response_status}\n")
        logging.info(f"\n\n💾\tFINAL_insert_results:\t{insert_results}, samples [{ecomon_insert_samples_count}|{len(result_json)}] counted")

        #   SENDING SAMPLES FROM DATABASE
        pdk_ids, bdates = [], []
        samples_json, sample_values_list = [], []
        active_samples_list = select_active_samples()

        if len(active_samples_list) > 0:
            full_text = ''
            update_results = []
            # table.column_headers = ["Время", "Пост/Номер","Сенсор/ПДК"]
            for device_sample_json in active_samples_list:
                # text = f"⌚ Проверяем интервал: [{bdate[11:len(bdate)]} - {edate[-8:]}]"        
                #   MSAAGE TEXT
                text = ''
                device_sample = json.loads(device_sample_json['sample'])
                if device_sample['bdate'] not in bdates and not IS_SORTED_DEVICES:
                    bdates.append(device_sample['bdate'])

                device_name = device_sample['device_name'][6:len(device_sample['device_name'])]
                if len(device_name) >= 32:
                    device_name = f"{device_name[0:24]}…"    
                
                short_date = device_sample['bdate'][11:len(device_sample['bdate']) - 3]   
                   
                snum = str(get_device_snum_by_name(dataset_xlsx_path, device_name))
                snum = f" [{device_sample['device_id']} | {snum}]" if 'NA' not in snum else f" [{device_sample['device_id']}]"
                
                q = device_sample['sensorvalue'] / device_sample['paramtype_pdk']
                if q >= 3 and q < 5:
                    emodji = '🔥'
                elif q >= 5:
                    emodji = '☣️'
                else:
                    emodji = ''

                devname_emodji = f"{emodji}{device_name}{snum}".strip()

                
                if 'aqua' in device_sample['paramtype_code']:
                    if 'Oxy' in device_sample['paramtype_code']:
                        paramtype_code = 'Хим.потр.кисл.'
                    elif 'NO2' in device_sample['paramtype_code']:
                        paramtype_code = 'Нитраты воды'
                    else:
                        paramtype_code = device_sample['paramtype_code']
                else:
                    #table_sorted.append(f"[{device_sample['bdate'][11:len(device_sample['bdate']) - 3]}]\t{device_name} {emodji}{snum}\n[ {device_sample['paramtype_code']} ▫ {round(device_sample['sensorvalue'] / device_sample['paramtype_pdk'], 1)} ]\n{fact_line}\n")
                    paramtype_code = device_sample['paramtype_code']

                pdk_ids.append(device_sample_json['pdk_id'])
                
                if IS_NVOS_ACTIVE:
                    fact_line = f"\nБлижайшие [{len(device_sample_json['factories'])}] предприятия:\n"
                    factories = device_sample_json['factories']
                    for factory in factories:
                        sum_round = round(float(factory['air_sum'])*1000, 4) if factory['air_sum'] is not None else '-1'
                        fact_line += f"\nДо объекта ~{factory['distance']} метров."
                        fact_line += f"\n[🏭 {factory['nvos_name']}]\nАдрес: {factory['nvos_address']}\nИмя: {factory['air_name']},\nКод: {factory['air_code']},\nМасса: {sum_round} кг/год;\n"
                    
                    text += text_template % (bdate, edate, device_sample['bdate'][11:len(device_sample['bdate']) - 3], device_sample_json['device_id'], devname_emodji, snum, paramtype_code, f"{round(device_sample['sensorvalue'] / device_sample['paramtype_pdk'], 1)} x ПДК")
                    text += fact_line
                    
                else:
                    factories, fact_line = [], ''
                    text += single_template % (device_sample['bdate'][11:len(device_sample['bdate']) - 3], devname_emodji, snum, device_sample_json['device_id'], paramtype_code, f"{round(device_sample['sensorvalue'] / device_sample['paramtype_pdk'], 1)} x ПДК")
                    full_text += f"\n▫ <i>{device_sample['bdate'][11:len(device_sample['bdate']) - 3]}</i> <b>{devname_emodji}</b>-   -   -   -   {paramtype_code} <b>{round(device_sample['sensorvalue'] / device_sample['paramtype_pdk'], 1)}</b> / ПДК\n"

                sample_values_list.append([device_sample['bdate'][11:len(device_sample['bdate']) - 3], devname_emodji, device_sample["paramtype_code"], round(device_sample['sensorvalue'] / device_sample['paramtype_pdk'], 1)])
                
                #table.append_row([device_sample['bdate'][11:len(device_sample['bdate']) - 3], devname_emodji, round(device_sample['sensorvalue'] / device_sample['paramtype_pdk'], 1)])
                #table.append_row(['-', snum, paramtype_code])
                
                
                device_sample.update({'text': text})
                if device_sample not in samples_json:
                    samples_json.append(device_sample)
                    
            else:
                #   AFTER LOOP
                with open(os.path.join(workdir, f"{datetime.now().strftime('%Y-%m-%d_%H_%M_%S')}_samples_json.txt"), 'w', encoding='utf-8') as fp:
                    json.dump(samples_json, fp=fp, ensure_ascii=False, sort_keys=True)   
                
                full_text = f"⌚ Проверяем интервал: [{bdate[11:len(bdate)]} - {edate[-8:]}]\n{full_text}" 
                
                #   Telegram
                try:
                    group_ids = [-828958519, -1001318085718]
                    chat_ids = [606301502, 873769825]
                    # full_text = table
                    print(full_text)
                    #for sample_json in samples_json:
                    #    text = sample_json['text']
                    #    full_text += f"{sample_json['text']}"
                    for chat_id in chat_ids:
                        pdk_tg_msg = await bot.send_message(chat_id=chat_id, text=full_text, disable_notification=True, parse_mode=ParseMode.HTML)
                    
                    main_pdk_tg_msg = await bot.send_message(chat_id=group_ids[1], text=full_text, disable_notification=True, parse_mode=ParseMode.HTML)
                    
                    if main_pdk_tg_msg and len(pdk_ids) > 0:
                        for pdk_id in pdk_ids:
                            # update_query = f"UPDATE iot.pdk_sample SET send_status=True WHERE sample='{sample_json['sample']}';"
                            update_query = f"UPDATE iot.pdk_sample SET send_status=True WHERE pdk_id={pdk_id};"
                            update_query_result = execute_query(update_query, 60)
                            update_results.append(update_query_result)
                            logging.info(f"update_query_result: [{sample_json['device_id']} | {update_query} | {update_query_result}")
                            #else:
                            #    print(type(main_pdk_tg_msg),  dir(main_pdk_tg_msg))
                except exceptions.BotBlocked:
                    logging.error(f"Ошибка: {chat_id} заблокировал бота")
                except exceptions.ChatNotFound:
                    logging.error(f"Ошибка: Чат {chat_id} не найден")
                except exceptions.RetryAfter as e:
                    logging.error(f"Ошибка: Достигнут лимит запросов. Повторить через {e.timeout} секунд.")
                    await asyncio.sleep(e.timeout)
                except Exception as e:
                    logging.error(f"Ошибка: {type(e).__name__} - {e}\n RESULT: {active_samples_list}")
                finally:
                    print(f"Telegram sended...")
                logging.info(f"\n\n💾\tFINAL_update_results:\t{update_results}, samples [{len(active_samples_list)}] counted")

        else:
            print(f"[{bdate}]\tПревышения не обнаружены..")
        
        if 'runtime' not in locals():
            runtime = time.time() - stime

        if runtime >= LOOP_CHECK_TABLE_TIMEOUT:
            logging.warning(f"Database Check! Skiping loop iteration, runtime = [{runtime}]")
            continue
        else:
            logging.info(f'Database Check! Sleeping {LOOP_CHECK_TABLE_TIMEOUT - runtime} seconds...')
            await asyncio.sleep(LOOP_CHECK_TABLE_TIMEOUT - runtime)
        print(f"RUNTIME: {runtime}, LEN: {len(samples_json)}")


async def loop_iter():
    task1 = asyncio.create_task(loop_send_last_samples())
    return await asyncio.gather(task1)

async def main():
    task1 = asyncio.create_task(loop_send_last_samples())
    task2 = asyncio.create_task(dp.start_polling())
    return await asyncio.gather(task1, task2)

if __name__=='__main__':
    loop = asyncio.get_event_loop()
    #loop.run_until_complete(loop_iter())
    print(asyncio.run(main()))
    
    quit()