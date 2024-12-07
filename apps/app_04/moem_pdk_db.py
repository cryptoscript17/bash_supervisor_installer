#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import psycopg2


# from moem_pdk import select_last_samples, remove_newlines, 
# from moem_pdk import remove_empty_lines
# from moem_pdk import select_last_samples, last_samples_list_to_messages

from _gis_get_devices import get_dataset_xlsx_path_local, get_snum_by_device_id, get_device_ids_by_name, get_device_id_by_name

from datetime import datetime
import pandas as pd

limit = 10

select_parsed_dataset_query = f"""
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
        WHERE REGEXP_REPLACE(COALESCE((sample::json->'device_id')::text, '0'), '[^0-9]*' ,'0')::integer = 306
    ORDER BY pdk_id DESC LIMIT {limit};
"""

def last_samples_to_json(last_samples_list):
    last_samples_json = []
    for last_sample_tuple in last_samples_list:
        last_sample_json = {}
        for index, col in enumerate(['pdk_id', 'send_status', 'bdate', 'device_id', 'sensor_id', 'device_name', 'sensorvalue', 'paramtype_pdk', 'paramtype_code']):
            if not isinstance(last_sample_tuple[index], (str, int)) and not isinstance(last_sample_tuple[index], datetime):
                # if last_sample_tuple[index].isnumeric():
                # last_sample_tuple[index] = float(last_sample_tuple[index])
                last_sample_json.update({col: float(last_sample_tuple[index])})
            elif isinstance(last_sample_tuple[index], datetime):
                last_sample_json.update({col: last_sample_tuple[index].strftime('%Y-%m-%d %H:%M:%S')})
            else:
                last_sample_json.update({col: last_sample_tuple[index]})
                
        if last_sample_json not in last_samples_json:
            last_samples_json.append(last_sample_json)
    return last_samples_json

# Postgres [db-167.base_60] settings
base_60_connection_params = {
    'host': '10.14.126.167',
    'port': 5432,
    'user': 'postgres',
    'password': 'qAyexo4MEI',
    'dbname': 'base_60'
}

local_connection_params = {
    'user': 'postgres',
    'password': 'qAyexo4MEI',
    'host': '127.0.0.1',
    'port': 65432,
    'dbname': 'iot'
}

factories_closest_full_query =  """
SELECT 
    nvosobj.onv_id as nvos_id,
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
    nvosdisc.water_pollutant_code as aqua_code,
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
    d.id = %s
FULL JOIN nvos.criteria_nvos nvoscrit ON (nvoscrit.onv_id = nvosobj.onv_id)
FULL JOIN nvos.sources_stationary nvosstat ON (nvosstat.onv_id = nvosobj.onv_id)
FULL JOIN nvos.sources_waste nvoswast ON (nvoswast.onv_id = nvosobj.onv_id)
FULL JOIN nvos.sources_discharges nvosdisc ON (nvosdisc.onv_id = nvosobj.onv_id)
WHERE 
	nvosobj.longitude IS NOT NULL AND nvosobj.latitude IS NOT NULL AND d.lat IS NOT NULL AND d.lon IS NOT NULL
--    AND air_pollutant_code NOT IN ('0010', '0301', '0303', '0337', '0328', '0330', '0333', '0304', '0342', '0349', '0408', '0410', '0417', '0620', '0627', '1728', '4001', '0827',  '1061', '1240', '0938', '1051', '1849', '2908', '1325', '0616', '3714', '2917', '0621')
    AND nvss.name IS NOT NULL
	AND registry_category IN (2, 1)
	AND nvosstat.air_pollutant_code IN %s
ORDER BY 
    dist_meters ASC
LIMIT %s;"""

factories_closest_short_query = """
    SELECT 
        o.onv_id,
        o.name,
        o.address,
        o.oktmo,
            ST_DistanceSphere(
                ST_MAKEPOINT(o.longitude, o.latitude),
                ST_MAKEPOINT(d.lon, d.lat)
            )::integer as dist_meters
    FROM 
        base_60.nvos.objects o
    JOIN 
        base_60.air.device d
    ON 
        d.id = %s
    WHERE 
        o.longitude IS NOT NULL AND o.latitude IS NOT NULL AND d.lat IS NOT NULL AND d.lon IS NOT NULL
    ORDER BY 
        dist_meters ASC
    LIMIT %s;"""





def get_closest_factories_by_device_id(connection_params: dict = None, query_template: str = 'SELECT * FROM base_60.nvos.objects LIMIT 5;', device_id: int = 148):
    ISOLATION_LEVEL_AUTOCOMMIT = True
    limit = 5
    factories_json = []
    with psycopg2.connect(**connection_params) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()    
        try:
            # query_template = query_template % (device_id, limit)
            cursor.execute(query_template)
            fetch_results = cursor.fetchall()
            for fetch_result in fetch_results:
                # print(len(fetch_result), fetch_result)
                try:
                    factories_json.append({
                        'nvos_id': fetch_result[0],
                        'nvos_state': fetch_result[1],
                        'nvos_name': fetch_result[2],
                        'nvos_address': fetch_result[3],
                        'nvos_oktmo': fetch_result[4],
                        'criteria_id': fetch_result[5],
                        'criteria_name': fetch_result[6],
                        'criteria_parent_id': fetch_result[7],
                        'criteria_title': fetch_result[8],
                        'criteria_tag': fetch_result[9],
                        'air_code': fetch_result[10],
                        'air_sum': fetch_result[11],
                        'air_name': fetch_result[12],
                        'waste_sum': fetch_result[13],
                        'aqua_code': fetch_result[14],
                        'aqua_sum': fetch_result[15],
                        'aqua_comment': fetch_result[16],
                        'aqua_name': fetch_result[17],
                        'distance': fetch_result[18]
                    })
                except Exception as e:
                    print(f"Error executing query: {e}")
        except Exception as err:
            print(f"Error executing query: {err}\nQUERY: query_template % (device_id, limit)")
        finally:
            print(f"QUERY OK!")
    return factories_json


get_device_ids_array_for_sending_query = """
SELECT
    ARRAY_AGG((sample::json->'device_id')::text) AS device_id
FROM iot.pdk_sample
WHERE
	send_status = False;"""



def get_device_ids_array_for_sending(connection_params):
    global get_device_ids_array_for_sending_query
    ISOLATION_LEVEL_AUTOCOMMIT = True
    try:
        factories_json = []
        with psycopg2.connect(**connection_params) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            cursor.execute(get_device_ids_array_for_sending_query)
            fetch_results = cursor.fetchall()
            try:
                print(f"device [#{0}]\t{[int(x) for x in fetch_results[0][0]]}")
                device_ids_for_sending = [int(x) for x in fetch_results[0][0]]
                device_ids_for_sending_unique_list = list(set(device_ids_for_sending))
                return device_ids_for_sending_unique_list
            except Exception as e:
                print(f"Error executing query: {e}")
    except Exception as err:
        print(f"Error executing query: {err}\nQUERY: {get_device_ids_array_for_sending_query}")
    finally:
        print(f"QUERY OK!")
    return []





if __name__ == '__main__':
    device_ids_for_sending = get_device_ids_array_for_sending(local_connection_params)
    for device_id in device_ids_for_sending:
        print(f"device [#{device_id}]\t")
    quit()

    factories_closest = get_closest_factories_by_device_id(connection_params=base_60_connection_params, query_template=factories_closest_full_query, device_id=148)
    # print(factories_closest)
    if factories_closest:
        for i, factory in enumerate(factories_closest):
            #print(f"[Factory #{i}]: {[str(x) for x in factory]}")
            print(f"[Factory #{i}]: {factory}")
            
    quit()



    
    
    snum_list = [1028]
    last_samples_list_to_messages(snum_list)
    quit()
    dataset_xlsx_path = get_dataset_xlsx_path_local()
    snum_list = get_device_ids_by_name(dataset_xlsx_path, 'Часцы')
    print(f"DATASET: [{dataset_xlsx_path}]\nSNUM_LIST: {snum_list}")
    # quit()
    # select_last_samples(limit, where_value, select_type = 'BY_ID')
    # last_samples_list = select_last_samples(limit = 50, where_value = 'часцы', select_type='BY_NAME')
    last_samples_list = select_last_samples(limit = 10, where_value = 307)
    last_samples_json = last_samples_to_json(last_samples_list)
    print(last_samples_json)
    
    quit()
    last_samples_df = pd.DataFrame(last_samples_list, columns=['pdk_id', 'send_status', 'bdate', 'device_id', 'sensor_id', 'device_name', 'sensorvalue', 'paramtype_pdk', 'paramtype_code'])
    print(last_samples_df)
    # select_last_samples(10)