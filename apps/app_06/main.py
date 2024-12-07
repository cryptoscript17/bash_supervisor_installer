import pandas as pd
import glob
import re
import os, socket, time, json, math, patoolib, csv
from time import gmtime, strftime
from datetime import datetime

import ftplib
from ftplib import FTP, FTP_TLS, all_errors

import shutil
import psycopg2
from psycopg2 import pool

HOST = '95.167.243.113'
PORT = 21025
USER = "ftp-user"
PASSWORD = "iPqnFSFB"
temp_file_data = None

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))

def get_printable_datetime_now():
    from datetime import datetime
    return f"📆 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
def is_json(test_json):
  try:
    json.loads(test_json)
  except ValueError as e:
    return False
  return True

def getFTP():
    _old_makepasv = FTP_TLS.makepasv

    def _new_makepasv(self):
        host, port = _old_makepasv(self)
        host = self.sock.getpeername()[0]
        return host, port

    FTP_TLS.makepasv = _new_makepasv
    ftp = FTP_TLS()
    try:
        ftp.connect(HOST, PORT)
        ftp.sendcmd('USER ' + USER)
        ftp.sendcmd('PASS ' + PASSWORD)
    except ftplib.all_errors as e:
        print("%s" % e)
    return ftp

def getFilesNames(ftp):
    last_file = None
    try:
        filesnames = ftp.nlst("-t *.rar")
        filesnames.reverse()
        filesnames.pop(0)

        if last_file is None:
            result = filesnames
        else:
            index = filesnames.index(last_file)
            result = filesnames[index + 1:]
    except Exception as e:
        print(f"{get_printable_datetime_now()}\tFTP 'getFileNames' err: {e}")
    return result

def get_last_timestamp_from_folders():
    folders = os.listdir(os.path.join(workdir, 'temp'))
    folder_names = [name for name in folders if os.path.isdir(os.path.join(workdir, 'temp', name))]
    folder_names.sort()
    return int(folder_names[-1])

def timestamp_to_filename_preffix(ts):
    from datetime import datetime
    if isinstance(ts, int):
        readable = datetime.fromtimestamp(ts).isoformat()
        #result = datetime.fromisoformat(readable).strftime('%Y-%m-%dT%H:%M:%S')
        result = datetime.fromisoformat(readable).strftime('%Y_%m_%d_')
    else:
        return 0
    return result

def get_lastime():
    from datetime import datetime
    start = time.time()
    time_gmt = 20 * 60
    timestamp_to = int((math.ceil(start / 1200) + 0.5) * 1200 - time_gmt - 1)
    timestamp_from = int((math.floor(start / 1200) + 0.5) * 1200 - time_gmt)
    #print('timestamp_from = ',timestamp_from, 'timestamp_to = ', timestamp_to)
    timestamp_to_dt = datetime.fromtimestamp(timestamp_to)
    timestamp_from_dt = datetime.fromtimestamp(timestamp_from)
    #return [timestamp_from_dt, timestamp_to_dt]
    return [timestamp_from, timestamp_to]

def last_time_to_timestamp(last_time):
    from datetime import datetime
    ddate = str(last_time).split('_')[0]
    ttime = str(last_time).split('_')[1]
    ddate_arr = str(ddate).split('-')
    ttime_arr = str(ddate).split('-')
    last_timestamp = time.mktime(datetime.strptime(last_time, "%Y-%m-%d_%H-%M").timetuple())
    return int(last_timestamp)

def filename2timestamp(filename):
    if len(filename) > 0:
        filename = filename[0:-4]
        datetime = str(filename.split('[')[3])[0:-1]
        #print(filename)
    else:
        datetime = 'err'
    return datetime

def timestamp_to_msk_time(ts):
    from datetime import datetime
    readable = datetime.fromtimestamp(ts).isoformat()
    ts = datetime.fromisoformat(readable).strftime('%Y-%m-%dT%H:%M:%S')
    return ts

def mem_time_to_timestamp(mem_time):
    from datetime import datetime
    last_timestamp = time.mktime(datetime.strptime(mem_time, "%d-%m-%Y %H:%M").timetuple())
    return int(last_timestamp)

def append_file(file_name, text_to_append):
    #  Функция для записи в журнал (текстовый файл)
    try:
        with open(file_name, "a+") as file_object:
            file_object.seek(0)
            data = file_object.read(100)
            if len(data) > 0:
                #file_object.write("\n")
                file_object.write("")
            file_object.write(text_to_append)
        file_object.close()
    except IOError as e:
        file_name = 'error!'+str(e)
    return file_name

def run_parallel_insert(responses_total):
    table = 'iot.iot'
    url = "'https://egfdm.mos.ru/'"
    #fields_string = "'{c_ppb,cmg,PM25,PM10,windSpeed,windVane,c1_ppb,t_ambiant,p_ambiant,rh_ambiant}'"

    try:
        #postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user="postgres", password="", host="127.0.0.1", port="5432", database="air_dev")
        postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user="postgres", password="qAyexo4MEI", host="10.14.126.166", port="5432", database="iot")
        if (postgreSQL_pool):
            print("Connection pool created successfully")

        # Use getconn() to Get Connection from connection pool
        ps_connection = postgreSQL_pool.getconn()

        if (ps_connection):
            print("successfully recived connection from connection pool ")
            ps_cursor = ps_connection.cursor()
            for response_single in responses_total:
                #print('response_single = ', response_single)
                device_id = chr(39)+response_single['DeviceId']+chr(39)
                sensors_string = "'{"+','.join(str(x) for x in list(response_single['Sensors'].keys()))+"}'"
                if sensors_string is None:
                    sensors_string = "'{CO,NO2,SO2,O3,OPC,NH3,H2S}'"
                dev_fields = []
                for dev in response_single['Devices']:
                    if dev['Field'] not in dev_fields:
                        dev_fields.append(str(dev['Field']).lower())
                dev_fields.extend(['PM25','PM10','C1_PPB'])
                #dev_fields.append('PM25','PM10','C1_PPB')
                fields_string = "'{"+','.join(str(x) for x in list(dev_fields))+"}'"
                if fields_string is None:
                    fields_string = "'{c_ppb,cmg,PM25,PM10,windSpeed,windVane,c1_ppb,t_ambiant,p_ambiant,rh_ambiant}'"
                
                data = (url, device_id, sensors_string, fields_string, chr(39)+json.dumps(response_single)+chr(39))
                select_query = f"""
                        SELECT response_data FROM iot
                        WHERE response_data::json ->> 'DeviceId' = 'AN99933'
                        ORDER BY createdate DESC LIMIT 1;
                    """
                
                query = f"""INSERT INTO {table} (url, device_id, sensors, fields, response_data) VALUES (%s, %s, %s, %s, %s);""" % data
                #print('query = ', query)

                ps_cursor.execute(query)

            ps_connection.commit()
            ps_cursor.close()

            # Use this method to release the connection object and send back to connection pool
            postgreSQL_pool.putconn(ps_connection)
            print("Put away a PostgreSQL connection")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        postgreSQL_pool = None

    finally:
        # closing database connection.
        # use closeall() method to close all the active connection if you want to turn of the application
        if postgreSQL_pool:
            postgreSQL_pool.closeall
        print("PostgreSQL connection pool is closed")
    
def dataframe_to_rnox_json(df):
    P_AMBIANT = False
    RH_AMBIANT = False
    T_AMBIANT = False
    WINDVANE = False
    WINDSPEED = False
    CH4 = False
    CO = False
    H2S = False
    H2O = False
    NH3 = False
    NO = False
    NO2 = False
    NOX = False
    O3 = False
    SO2 = False
    OPC10 = False
    OPC25 = False
    response_devices_arr = []   #   response.json {"Devices": [response_devices_arr]}
    opc_arr = []   #      #   response.json {"OPC": [opc_arr]}
    samples_arr = []
    sensordata_arr = []
    sensortypes_mem = [1,2,4,5,6,9,10,11,13,17,18,19,20,21,22,23,34,40,53,61]
    for row in df.iterrows():
        if int(row[1].sensortype_id) in sensortypes_mem:
            vendor_id = row[1].vendor_id
            vendor_sensortype_id = row[1].sensortype_id
            sensor_value = row[1].sensorvalue
            
            bdate_ts = int(mem_time_to_timestamp(row[1].bdate))   #   START
            bdate = timestamp_to_msk_time(bdate_ts)
            
            edate_ts = int(mem_time_to_timestamp(row[1].bdate)) + 1200
            edate = timestamp_to_msk_time(edate_ts)
            
            if vendor_sensortype_id == 1:  #  CO
                CO = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 6
                CO['SampleStartTime'] = str(bdate)+'+03:00'
                CO['SampleEndTime'] = str(edate)+'+03:00'
                CO['Data'] = sensor_value
                co_json = {"CO": [CO]}
                sensortypes_mem.remove(1)

            elif vendor_sensortype_id == 2:  #  SO2
                SO2 = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 10
                url = 'https://egfdm.mos.ru/'
                SO2['SampleStartTime'] = str(bdate)+'+03:00'
                SO2['SampleEndTime'] = str(edate)+'+03:00'
                SO2['Data'] = sensor_value
                so2_json = {"SO2": [SO2]}
                sensortypes_mem.remove(2)

            elif vendor_sensortype_id == 3:  #  NH3
                NH3 = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 17
                NH3['SampleStartTime'] = str(bdate)+'+03:00'
                NH3['SampleEndTime'] = str(edate)+'+03:00'
                NH3['Data'] = float(str(sensor_value).replace(',','.'))*1000
                nh3_json = {"NH3": [NH3]}
                sensortypes_mem.remove(3)

            elif vendor_sensortype_id == 4:  #  H2S
                H2S = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 12
                H2S['SampleStartTime'] = str(bdate)+'+03:00'
                H2S['SampleEndTime'] = str(edate)+'+03:00'
                H2S['Data'] = float(str(sensor_value).replace(',','.'))*1000
                h2s_json = {"H2S": [H2S]}
                sensortypes_mem.remove(4)

            elif vendor_sensortype_id == 5:  #  O3
                O3 = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 11
                O3['SampleStartTime'] = str(bdate)+'+03:00'
                O3['SampleEndTime'] = str(edate)+'+03:00'
                O3['Data'] = sensor_value
                o3_json = {"O3": [O3]}
                sensortypes_mem.remove(5)

            elif vendor_sensortype_id == 6:  #  CH4
                CH4 = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 20
                CH4['SampleStartTime'] = str(bdate)+'+03:00'
                CH4['SampleEndTime'] = str(edate)+'+03:00'
                CH4['Data'] = float(str(sensor_value).replace(',','.'))*1000
                ch4_json = {"CH4": [CH4]}
                sensortypes_mem.remove(6)

            elif vendor_sensortype_id == 9:  #  CH-
                CH_ = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 21
                CH_['SampleStartTime'] = str(bdate)+'+03:00'
                CH_['SampleEndTime'] = str(edate)+'+03:00'
                CH_['Data'] = float(str(sensor_value).replace(',','.'))*1000
                ch__json = {"CH-": [CH_]}
                sensortypes_mem.remove(9)

            elif vendor_sensortype_id == 10:  #  NO2
                NO2 = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 9
                NO2['SampleStartTime'] = str(bdate)+'+03:00'
                NO2['SampleEndTime'] = str(edate)+'+03:00'
                NO2['Data'] = float(str(sensor_value).replace(',','.'))*1000
                no2_json = {"NO2": [NO2]}
                sensortypes_mem.remove(10)

            elif vendor_sensortype_id == 11:  #  CHX
                CHX = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 22
                CHX['SampleStartTime'] = str(bdate)+'+03:00'
                CHX['SampleEndTime'] = str(edate)+'+03:00'
                CHX['Data'] = float(str(sensor_value).replace(',','.'))*1000
                chx_json = {"CHX": [CHX]}
                sensortypes_mem.remove(11)

            elif vendor_sensortype_id == 13:  #  NO
                NO = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 8
                NO['SampleStartTime'] = str(bdate)+'+03:00'
                NO['SampleEndTime'] = str(edate)+'+03:00'
                NO['Data'] = float(str(sensor_value).replace(',','.'))*1000
                no_json = {"NO": [NO]}
                sensortypes_mem.remove(13)

            elif vendor_sensortype_id == 18:  #  P
                P_AMBIANT = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"P_AMBIANT","Data":""}
                gis_sensortype_id = 2
                P_AMBIANT['SampleStartTime'] = str(bdate)+'+03:00'
                P_AMBIANT['SampleEndTime'] = str(edate)+'+03:00'
                P_AMBIANT['Data'] = round(0.1*float(str(sensor_value).replace(',','.'))/0.75006156)
                response_devices_arr.append(P_AMBIANT)
                sensortypes_mem.remove(18)

            elif vendor_sensortype_id == 19:  #  H
                RH_AMBIANT = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"RH_AMBIANT","Data":""}
                gis_sensortype_id = 3
                RH_AMBIANT['SampleStartTime'] = str(bdate)+'+03:00'
                RH_AMBIANT['SampleEndTime'] = str(edate)+'+03:00'
                RH_AMBIANT['Data'] = sensor_value
                response_devices_arr.append(RH_AMBIANT)
                sensortypes_mem.remove(19)

            elif vendor_sensortype_id == 20:  #  PM10
                OPC10 = {"SampleStartTime":"2022-08-04T16:20:00+03:00","SampleEndTime":"2022-08-04T16:40:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 5
                OPC10['SampleStartTime'] = str(bdate)+'+03:00'
                OPC10['SampleEndTime'] = str(edate)+'+03:00'
                OPC10['Data'] = sensor_value
                #opc_arr.append(OPC10)
                sensortypes_mem.remove(20)

            elif vendor_sensortype_id == 21:  #  WS
                WINDSPEED = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"WINDSPEED","Data":""}
                gis_sensortype_id = 15
                WINDSPEED['SampleStartTime'] = str(bdate)+'+03:00'
                WINDSPEED['SampleEndTime'] = str(edate)+'+03:00'
                WINDSPEED['Data'] = sensor_value
                response_devices_arr.append(WINDSPEED)
                sensortypes_mem.remove(21)

            elif vendor_sensortype_id == 22:  #  WD
                WINDVANE = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"WINDVANE","Data":""}
                gis_sensortype_id = 14
                WINDVANE['SampleStartTime'] = str(bdate)+'+03:00'
                WINDVANE['SampleEndTime'] = str(edate)+'+03:00'
                WINDVANE['Data'] = sensor_value
                response_devices_arr.append(WINDVANE)
                sensortypes_mem.remove(22)

            elif vendor_sensortype_id == 23:  #  T
                T_AMBIANT = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"T_AMBIANT","Data":""}
                gis_sensortype_id = 1
                T_AMBIANT['SampleStartTime'] = str(bdate)+'+03:00'
                T_AMBIANT['SampleEndTime'] = str(edate)+'+03:00'
                T_AMBIANT['Data'] = sensor_value
                response_devices_arr.append(T_AMBIANT)
                sensortypes_mem.remove(23)

            elif vendor_sensortype_id == 34:  #  NOx
                NOX = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 39
                NOX['SampleStartTime'] = str(bdate)+'+03:00'
                NOX['SampleEndTime'] = str(edate)+'+03:00'
                NOX['Data'] = sensor_value
                nox_json = {"NOx": [NOX]}
                sensortypes_mem.remove(34)

            elif vendor_sensortype_id == 40:  #  H2O
                H2O = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 16
                H2O['SampleStartTime'] = str(bdate)+'+03:00'
                H2O['SampleEndTime'] = str(edate)+'+03:00'
                H2O['Data'] = sensor_value
                h2o_json = {"H2O": [H2O]}
                sensortypes_mem.remove(40)

            elif vendor_sensortype_id == 53:  #  CO2
                CO2 = {"SampleStartTime":"2022-08-04T16:00:00+03:00","SampleEndTime":"2022-08-04T16:20:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 7
                CO2['SampleStartTime'] = str(bdate)+'+03:00'
                CO2['SampleEndTime'] = str(edate)+'+03:00'
                CO2['Data'] = sensor_value
                no_json = {"CO2": [CO2]}
                sensortypes_mem.remove(53)

            elif vendor_sensortype_id == 61:  #  PM2.5
                OPC25 = {"SampleStartTime":"2022-08-04T16:20:00+03:00","SampleEndTime":"2022-08-04T16:40:00+03:00","Field":"C1_PPB","Data":""}
                gis_sensortype_id = 4
                OPC25['SampleStartTime'] = str(bdate)+'+03:00'
                OPC25['SampleEndTime'] = str(edate)+'+03:00'
                OPC25['Data'] = sensor_value
                #opc_arr.append(OPC25)
                sensortypes_mem.remove(61)

            response_json, sensors_json = {}, {}
            if 'ch4_json' in locals():
                    sensors_json.update(ch4_json)

            if 'co_json' in locals():
                    sensors_json.update(co_json)

            if 'h2s_json' in locals():
                    sensors_json.update(h2s_json)
                    
            if 'h2o_json' in locals():
                    sensors_json.update(h2o_json)
                    
            if 'nh3_json' in locals():
                    sensors_json.update(nh3_json)
                    
            if 'no_json' in locals():
                    sensors_json.update(no_json)
                    
            if 'no2_json' in locals():
                    sensors_json.update(no2_json)
                    
            if 'nox_json' in locals():
                    sensors_json.update(nox_json)
                    
            if 'o3_json' in locals():
                    sensors_json.update(o3_json)

            if 'so2_json' in locals():
                    sensors_json.update(so2_json)

            if 'ch__json' in locals():
                    sensors_json.update(ch__json)
                    
            if 'chx_json' in locals():
                    sensors_json.update(chx_json)
                
            if OPC25 != False and OPC10 != False:
                sensors_json.update({"OPC": [OPC25, OPC10]})
                    
            response_json.update({"DeviceId": 'AN89'+str(vendor_id)})
            response_json.update({"Devices": response_devices_arr})
            response_json.update({"Sensors": sensors_json})

    if 'response_json' in locals():
        if len(response_json['Devices']) > 0 and len(response_json['Sensors']) > 0:
            sensordata_arr.append(response_json)

    #print(row[1].vendor_id,row[1].sensortype_id,row[1].bdate,row[1].sensorvalue)

    return sensordata_arr

def get_dataframe_from_csv_path(csv_path):
    all_files = os.listdir(csv_path)    
    csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
    li = []
    for filename in csv_files:
        df = pd.read_csv(os.path.join(csv_path, filename), delimiter=';', names=["vendor_id", "vendor_name", "sensortype_id", "bdate", "sensorvalue", "bdate_min", "min_sensorvalue", "bdate_max", "max_sensorvalue"])
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=False)
    return frame



def ping():
    # to ping a particular IP
    try:
        socket.setdefaulttimeout(3)
         
        # if data interruption occurs for 3
        # seconds, <except> part will be executed
 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # AF_INET: address family
        # SOCK_STREAM: type for TCP
 
        host = "95.167.243.113"   #   MOEM RTK FTP
        port = 21025
 
        server_address = (host, port)
        s.connect(server_address)
 
    except OSError as error:
        return False
 
    else:
        s.close()
        return True

def get_rar_filenames_after(ftp):
    #last_time = '2022-08-07_02-50'
    global temp_path
    filenames_after = []
    last_timestamp = get_lastime()[0]   #   LAST 20 MIN PERIOD
    # last_timestamp = get_last_timestamp_from_folders()  #   CONTINUE DOWNLOAD MODE 2024.06.21
    print(last_timestamp, type(last_timestamp))
    filenames = getFilesNames(ftp)   #   UF
    for filename in filenames:
        if len(filename) > 0:
            filename_timestamp = int(last_time_to_timestamp(filename2timestamp(filename)))
            if filename_timestamp > (last_timestamp-200):
                filenames_after.append(filename)
    return filenames_after


def download_files(ftp, filenames):
    global temp_path

    # last_timestamp = int(get_lastime()[0])
    last_timestamp = get_last_timestamp_from_folders()  #   CONTINUE DOWNLOAD MODE 2024.06.21
        
    new_path = os.path.join(temp_path, str(last_timestamp))
    
    if os.path.exists(new_path):
        shutil.rmtree(new_path, ignore_errors=True)
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    
    for filename in filenames:
        path_to_rar_file = os.path.join(new_path, filename)
        print('types = ', type(ftp), type(filename), 'path = ', path_to_rar_file)
        with open(path_to_rar_file, 'wb') as f:
            try:
                print('INFO:    ', 'ftp.retrbinary - exception!', ' filename = ', filename, 'f.write = ', f.write)
                ftp.retrbinary("RETR %s" % filename, f.write)
            except all_errors as error:
                print('ERROR:    ', 'ftp.retrbinary - exception!', ' filename = ', filename, 'f.write = ', f.write)
    return new_path

def unrar_files(new_path, filenames):
    global temp_path
    last_timestamp = get_lastime()[0]
    
    #outdir = temp_path + '\\' + str(last_timestamp) + '\\' + 'csv'
    outdir = os.path.join(str(new_path), 'csv')
    
    if os.path.exists(outdir):
        shutil.rmtree(outdir, ignore_errors=True)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    for filename in filenames:
        path_to_rar_file = os.path.join(new_path, filename)
        try:
            patoolib.extract_archive(path_to_rar_file, outdir=outdir)

        except Exception as error:
            print(f"ERROR: CAN'T OPEN ARCHIVE (SKIPPING) - {path_to_rar_file} - ", repr(error), strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    return outdir


def filenames_list_to_dates_list(fnames):
    dtstrings, tslist = [], []
    pattern = re.compile(r'(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})')
    for fname in fnames:
        match = pattern.search(fname)
        if match:
            if match.group(1) not in dtstrings:  # '2024-06-21_09-30'
                date_string = match.group(1)
                dt = datetime.strptime(date_string, '%Y-%m-%d_%H-%M')
                timestamp = int(dt.timestamp())
                dtstrings.append(date_string)
                tslist.append(timestamp)
        else:
            print('Дата и время не найдены.')
    return dtstrings, tslist

def filter_from_filenames_list_by_date(fnames, date_string):
    filtered_fnames = []
    for fname in fnames:
        if date_string in fname and fname not in filtered_fnames:
            filtered_fnames.append(fname)
    return filtered_fnames


def save_list(fpath, list_to_save):
    file = open(fpath, 'w')
    file.writelines(str(item) + '\n' for item in list_to_save)
    file.close()
    if os.path.exists(fpath):
        return True
    else:
        return False


#csv_path = 'C:\\Users\\ZubkovEO\\Documents\\2022_05_31_measure_air\\app\\_temp\\2022_09_30_15_50\\csv' # use your path

base_dir = os.getcwd()
temp_path = os.path.join(base_dir, 'temp')
#temp_path = 'C:\\Users\\ZubkovEO\\Documents\\2022_05_31_measure_air\\app\\temp'

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))


while True:
    start_time = time.time()
    last_time = get_lastime()
    # last_timestamp = get_last_timestamp_from_folders()
    # last_time = int(last_timestamp) + 1200
    print(f"get_lastime = [{datetime.fromtimestamp(last_time[0]).strftime('%Y-%m-%d %H:%M:%S')} | {datetime.fromtimestamp(last_time[1]).strftime('%Y-%m-%d %H:%M:%S')}]", )
    ftp = getFTP()
    
    if ftp.sock is not None:
        print('INFO:   ', 'START = ', timestamp_to_msk_time(start_time))
        fnames = get_rar_filenames_after(ftp)
        # print('get_rar_filenames_after_fnames = ', fnames)
        fpath = os.path.join(workdir, '_get_rar_filenames_after.txt')
        if save_list(fpath, fnames):
            print(f"Saved: [{fpath}]")
        if fnames is not None:
            date_string_list, timestamp_list = filenames_list_to_dates_list(fnames)
            # print(f"date_string_list, timestamp_list = {date_string_list}, {timestamp_list}")
            for index, date_string in enumerate(date_string_list):
                new_path = os.path.join(workdir, str(timestamp_list[index]))
                if os.path.exists(new_path):
                    shutil.rmtree(new_path, ignore_errors=True)
                # print(f"(fnames, date_string) = ({fnames}, {date_string})")
                fnames_filtered = filter_from_filenames_list_by_date(fnames, date_string)
                new_path = download_files(ftp, fnames_filtered)
                # print('download_files_fnames = ', fnames_filtered)
                if len(fnames_filtered) > 0:
                    outdir = unrar_files(new_path, fnames_filtered)
                    # print('outdir = ', outdir)
                    try:
                        frame = get_dataframe_from_csv_path(outdir)
                    except Exception as e:
                        print(f"Concatenation error: {e}")
                        continue
                    bdates = list(frame.bdate.unique())
                    vendor_ids = list(frame.vendor_id.unique())
                    print(f"Workdir path: [{outdir}]\nDates interval: [{bdates[-1]} - {bdates[0]}]\nDevice IDs detected: {vendor_ids}\nSensortype IDs detected: {list(frame.sensortype_id.unique())}")
                    #print('bdates = ', bdates, '| item type = ', type(bdates[0]))
                    #print('sensortype_id = ', list(frame.sensortype_id.unique()))
                    #print('vendor_ids = ', vendor_ids, '| item type = ', type(vendor_ids[0]))
                    #vendor_ids = [46,47,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,930,932,933]
                    responses_total = []
                    good_vendor_ids, bad_vendor_ids = [], []

                    for bdate in bdates:
                        for vendor_id in vendor_ids:
                            df = frame[(frame['vendor_id']==vendor_id) & (frame['sensortype_id']<62) & frame['bdate'].str.startswith(bdate)][['vendor_id','sensortype_id', 'bdate', 'sensorvalue']]
                            response_single = dataframe_to_rnox_json(df)
                            if len(response_single) > 0:
                                responses_total.append(response_single[0])
                                good_vendor_ids.append(vendor_id)
                                print(f"vendor_id = {vendor_id} | JSON|len = {response_single[0]['DeviceId']} | {len(response_single[0]['Devices'])}")
                            else:
                                bad_vendor_ids.append(vendor_id)

                    print(f"good vendor_ids = [{good_vendor_ids}], bad vendor_ids = [{bad_vendor_ids}]")
                    # print('responses_total = ', responses_total)

                    if os.path.isfile(str(timestamp_to_filename_preffix(last_time[0]))+'__'+'_response_json.txt'):
                        os.remove(str(timestamp_to_filename_preffix(last_time[0]))+'__'+'_response_json.txt')
                    append_file(str(timestamp_to_filename_preffix(last_time[0]))+'__'+'_response_json.txt', json.dumps(responses_total))
                    
                    # print('\n'.join(str(x) for x in list(responses_total)))
                    run_parallel_insert(responses_total)
                else:
                    print('INFO:   ', 'Waiting While Files Refreshed!')
            runtime = time.time() - start_time
        else:
            runtime = time.time() - start_time
            print('HA FTP NONE!')
            time.sleep(300)
    else:
        print("Socket is None.")
    if ftp:
        ftp.quit()
    if runtime >= 1200:
        print(f"Skiping loop iteration, runtime = [{runtime}]")
        continue
    else:
        time.sleep(math.floor(1200 - runtime))
        # time.sleep(int(1200 - runtime))


# if __name__ == '__main__':
    # last_timestamp = get_last_timestamp_from_folders()
#    print(get_last_timestamp_from_folders(), get_lastime()[0])

    
    