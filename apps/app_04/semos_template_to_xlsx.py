import os, logging, csv
from datetime import datetime
import pandas as pd

logging.basicConfig(level=logging.INFO)

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))

def filename_today_prefix() -> str:
    current_date = datetime.now().strftime('%Y_%m_%d_')
    return current_date

def reorder_columns(df_columns) -> list:
    new_order = []
    cols_to_move = ['P', 'WD', 'H', 'WS', 'T']
    for col in df_columns:
        if col not in cols_to_move:
            new_order.insert(0, col)
        else:
            new_order.append(col)
    order = ['NO2', 'NO', 'NOx', 'NH3', 'CO', 'H2S', 'SO2', 'PM', 'PM1', 'PM2.5', 'PM10', 'P', 'WD', 'H', 'WS', 'T']
    sorted_data = sorted([i for i in order if i in new_order], key=lambda x: order.index(x))
    return sorted_data

def convert_semos_template_csv(win1251_template_csv_file) -> str:
    dataset_arr = []
    with open(win1251_template_csv_file, newline='', encoding='cp1251') as f:
        #try:
        for i in range(5):  # Пропускаем первые 4 строки
            next(f)
        reader = csv.reader(f, delimiter=';')
        for j, row in enumerate(reader):
            if len(row) == 13:   #   13 COLUMNS IN CSV HEADER
                if len(row[0]) > 0:   #   device parent row
                    if ':' in row[2]:
                        device_name = row[2].split(':')[0]
                    else:
                        device_name = row[2]
                    sample_index = row[0]
                    bdate, edate = row[4], row[5]
                    row[2], row[4], row[5] = '', '', ''
                    sensortype_name, sensor_value, sensor_pdk, sensor_measure_unit  = row[7], row[9], row[11], row[12].replace('?', '')
                else:   #   device children rows
                    sensortype_name, sensor_value, sensor_pdk, sensor_measure_unit = row[7], row[9], row[11], row[12].replace('?', '')
                if sensortype_name.strip() == 'Углерода оксид':
                    sensortype_name = 'CO'
                elif sensortype_name.strip() == 'Азота оксид':
                    sensortype_name = 'NO'
                elif sensortype_name.strip() == 'Азота диоксид':
                    sensortype_name = 'NO2'
                elif sensortype_name.strip() == 'Аммиак':
                    sensortype_name = 'NH3'
                elif sensortype_name.strip() == 'Азота оксиды':
                    sensortype_name = 'NOx'
                elif sensortype_name.strip() == 'Атмосферное давление':
                    sensortype_name = 'P'
                elif sensortype_name.strip() == 'Направление ветра':
                    sensortype_name = 'WD'
                elif sensortype_name.strip() == 'Относительная влажность':
                    sensortype_name = 'H'
                elif sensortype_name.strip() == 'Сера диоксид':
                    sensortype_name = 'SO2'
                elif sensortype_name.strip() == 'Сероводород':
                    sensortype_name = 'H2S'
                elif sensortype_name.strip() == 'Скорость ветра':
                    sensortype_name = 'WS'
                elif sensortype_name.strip() == 'Температура воздуха':
                    sensortype_name = 'T'
                elif sensortype_name.strip() == 'Взвешенные частицы':
                    sensortype_name = 'PM'
                elif sensortype_name.strip() == 'Взвешенные частицы PM1':
                    sensortype_name = 'PM1'
                elif sensortype_name.strip() == 'Взвешенные частицы PM2.5':
                    sensortype_name = 'PM2.5'
                elif sensortype_name.strip() == 'Взвешенные частицы PM10':
                    sensortype_name = 'PM10'
                sensor_value = sensor_value.split(' ')[0].replace(',', '.')
                if sensor_value == 'Ш':
                    sensor_value = '0'
                if len(sensor_value) > 0:

                    sensor_value = float(sensor_value)
                    if sensor_value < 0 and sensortype_name != 'T':
                        sensor_value = 0
                result_row = f"{str(j)};{sample_index};{device_name};{bdate};{edate};{sensortype_name};{sensor_value};{sensor_measure_unit}"
                dataset_arr.append(result_row)
            else:
                print('ERROR:   ','incorrect row length, is ... ', len(row[0]), ', index_row is .. ', j, ', file ... ', os.path.basename(win1251_template_csv_file))
                pass

        #except:
        #    dataset_path = 'NA'
        #    return dataset_path
    dataset_text = '\n'.join(str(x) for x in dataset_arr)
    #   TODO 2024.03
    #   Add data prefix
    dataset_path = os.path.join(workdir, f"{filename_today_prefix()}{os.path.basename(win1251_template_csv_file).replace('csv', 'txt')}")

    with open(dataset_path, "w", encoding="utf-8") as outfile:
        outfile.write(dataset_text)
    return dataset_path

def converted_template_to_xlsx(dataset_path) -> str:
    # print(f"dataset_path: {dataset_path}")
    df = pd.read_csv(dataset_path, encoding='utf-8', decimal=',', delimiter=';', skiprows=1, names = ['index','sample_index','device_name','bdate','edate','sensor_name','sensor_value','sensor_pdk'], index_col='index')
    df['sensor_value'] = df['sensor_value'].str.replace(',', '.').astype(float)
    df['bdate'] = pd.to_datetime(df['bdate'], format='%d.%m.%Y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M')

    # print(df.head())
    unique_devices = df['device_name'].unique()
    for device in unique_devices:
        if device == 'Москва':
            msk_df = df[df['device_name'] == device].copy()
        elif 'Волоколамск' in device:
            volok_df = df[df['device_name'] == device].copy()
        elif device == 'Дмитров':
            dmitrov_df = df[df['device_name'] == device].copy()
        elif device == 'Домодедово':
            domoded_df = df[df['device_name'] == device].copy()
        elif device == 'Егорьевск':
            egor_df = df[df['device_name'] == device].copy()
        elif device == 'Ногинск':
            noginsk_df = df[df['device_name'] == device].copy()
        elif device == 'Орехово-Зуево':
            oreh_df = df[df['device_name'] == device].copy()
        elif device == 'Пушкино':
            pushkino_df = df[df['device_name'] == device].copy()
        elif device == 'Раменское':
            ramen_df = df[df['device_name'] == device].copy()
        elif device == 'Сергиев Пасад':
            serposad_df = df[df['device_name'] == device].copy()
        elif device == 'Шатура':
            shatura_df = df[df['device_name'] == device].copy()
        elif device == 'Солнечногорск':
            solar_df = df[df['device_name'] == device].copy()
        elif device == 'г. Лосино-Петровский':
            losino_df = df[df['device_name'] == device].copy()
        elif device == 'Тестовое место':  #   KOLOMNA
            kotelniki_df = df[df['device_name'] == device].copy()
    del df

    if 'noginsk_df' in locals():
        noginsk_pivot = noginsk_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del noginsk_df
        new_order = reorder_columns(noginsk_pivot.columns.tolist())
        noginsk_pivot = noginsk_pivot.reindex(columns=new_order)
        try:
            noginsk_pivot.reset_index(inplace=True)
            noginsk_pivot['bdate'] = pd.to_datetime(noginsk_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            noginsk_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting noginsk_pivot 'bdate' error: {e}")

    if 'ramen_df' in locals():
        ramen_pivot = ramen_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del ramen_df
        new_order = reorder_columns(ramen_pivot.columns.tolist())
        ramen_pivot = ramen_pivot.reindex(columns=new_order)
        try:
            ramen_pivot.reset_index(inplace=True)
            ramen_pivot['bdate'] = pd.to_datetime(ramen_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            ramen_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting ramen_pivot 'bdate' error: {e}")

    if 'domoded_df' in locals():
        domoded_pivot = domoded_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del domoded_df
        new_order = reorder_columns(domoded_pivot.columns.tolist())
        domoded_pivot = domoded_pivot.reindex(columns=new_order)
        try:
            domoded_pivot.reset_index(inplace=True)
            domoded_pivot['bdate'] = pd.to_datetime(domoded_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            domoded_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting domoded_pivot 'bdate' error: {e}")

    if 'egor_df' in locals():
        egor_pivot = egor_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del egor_df
        new_order = reorder_columns(egor_pivot.columns.tolist())
        egor_pivot = egor_pivot.reindex(columns=new_order)
        try:
            egor_pivot.reset_index(inplace=True)
            egor_pivot['bdate'] = pd.to_datetime(egor_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            egor_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting egor_pivot 'bdate' error: {e}")

    if 'volok_df' in locals():
        volok_pivot = volok_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del volok_df
        new_order = reorder_columns(volok_pivot.columns.tolist())
        volok_pivot = volok_pivot.reindex(columns=new_order)
        try:
            volok_pivot.reset_index(inplace=True)
            volok_pivot['bdate'] = pd.to_datetime(volok_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            volok_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting volok_pivot 'bdate' error: {e}")

    if 'oreh_df' in locals():
        oreh_pivot = oreh_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del oreh_df
        new_order = reorder_columns(oreh_pivot.columns.tolist())
        oreh_pivot = oreh_pivot.reindex(columns=new_order)
        try:
            oreh_pivot.reset_index(inplace=True)
            oreh_pivot['bdate'] = pd.to_datetime(oreh_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            oreh_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting oreh_pivot 'bdate' error: {e}")

    if 'pushkino_df' in locals():
        pushkino_pivot = pushkino_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del pushkino_df
        new_order = reorder_columns(pushkino_pivot.columns.tolist())
        pushkino_pivot = pushkino_pivot.reindex(columns=new_order)
        try:
            pushkino_pivot.reset_index(inplace=True)
            pushkino_pivot['bdate'] = pd.to_datetime(pushkino_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            pushkino_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting pushkino_pivot 'bdate' error: {e}")

    if 'serposad_df' in locals():
        serposad_pivot = serposad_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del serposad_df
        new_order = reorder_columns(serposad_pivot.columns.tolist())
        serposad_pivot = serposad_pivot.reindex(columns=new_order)
        try:
            serposad_pivot.reset_index(inplace=True)
            serposad_pivot['bdate'] = pd.to_datetime(serposad_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            serposad_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting serposad_pivot 'bdate' error: {e}")

    if 'dmitrov_df' in locals():
        dmitrov_pivot = dmitrov_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del dmitrov_df
        new_order = reorder_columns(dmitrov_pivot.columns.tolist())
        dmitrov_pivot = dmitrov_pivot.reindex(columns=new_order)
        try:
            dmitrov_pivot.reset_index(inplace=True)
            dmitrov_pivot['bdate'] = pd.to_datetime(dmitrov_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            dmitrov_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting dmitrov_pivot 'bdate' error: {e}")

    if 'shatura_df' in locals():
        shatura_pivot = shatura_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del shatura_df
        new_order = reorder_columns(shatura_pivot.columns.tolist())
        shatura_pivot = shatura_pivot.reindex(columns=new_order)
        try:
            shatura_pivot.reset_index(inplace=True)
            shatura_pivot['bdate'] = pd.to_datetime(shatura_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            shatura_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting shatura_pivot 'bdate' error: {e}")

    if 'solar_df' in locals():
        solar_pivot = solar_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del solar_df
        new_order = reorder_columns(solar_pivot.columns.tolist())
        solar_pivot = solar_pivot.reindex(columns=new_order)
        try:
            solar_pivot.reset_index(inplace=True)
            solar_pivot['bdate'] = pd.to_datetime(solar_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            solar_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting Solnechnogorsk_pivot 'bdate' error: {e}")

    if 'losino_df' in locals():
        losino_pivot = losino_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del losino_df
        new_order = reorder_columns(losino_pivot.columns.tolist())
        losino_pivot = losino_pivot.reindex(columns=new_order)
        try:
            losino_pivot.reset_index(inplace=True)
            losino_pivot['bdate'] = pd.to_datetime(losino_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            losino_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting Solnechnogorsk_pivot 'bdate' error: {e}")

    if 'kotelniki_df' in locals():
        kotelniki_pivot = kotelniki_df.pivot_table(index='bdate', columns='sensor_name', values='sensor_value')#, aggfunc='mean')
        del kotelniki_df
        new_order = reorder_columns(kotelniki_pivot.columns.tolist())
        kotelniki_pivot = kotelniki_pivot.reindex(columns=new_order)
        try:
            kotelniki_pivot.reset_index(inplace=True)
            kotelniki_pivot['bdate'] = pd.to_datetime(kotelniki_pivot['bdate'], format='%Y-%m-%d %H:%M').dt.strftime('%d.%m.%Y %H:%M')
            kotelniki_pivot.set_index(keys='bdate', inplace=True, drop=True)
        except Exception as e:
            print(f"Converting Solnechnogorsk_pivot 'bdate' error: {e}")



    devices_fname_xlsx = os.path.join(workdir, os.path.basename(dataset_path).replace('txt', 'xlsx'))
    xls_writer = pd.ExcelWriter(devices_fname_xlsx, engine='xlsxwriter')
    if 'dmitrov_pivot' in locals():
        dmitrov_pivot.to_excel(xls_writer, index='bdate', sheet_name='Дмитров')
    if 'domoded_pivot' in locals():
        domoded_pivot.to_excel(xls_writer, index='bdate', sheet_name='Домодедово')
    if 'volok_pivot' in locals():
        volok_pivot.to_excel(xls_writer, index='bdate', sheet_name='Волоколамск')
    if 'egor_pivot' in locals():
        egor_pivot.to_excel(xls_writer, index='bdate', sheet_name='Егорьевск')
    if 'noginsk_pivot' in locals():
        noginsk_pivot.to_excel(xls_writer, index='bdate', sheet_name='Ногинск')
    if 'oreh_pivot' in locals():
        oreh_pivot.to_excel(xls_writer, index='bdate', sheet_name='Орехово-Зуево')
    if 'pushkino_pivot' in locals():
        pushkino_pivot.to_excel(xls_writer, index='bdate', sheet_name='Пушкино')
    if 'ramen_pivot' in locals():
        ramen_pivot.to_excel(xls_writer, index='bdate', sheet_name='Раменское')
    if 'serposad_pivot' in locals():
        serposad_pivot.to_excel(xls_writer, index='bdate', sheet_name='Сергиев-Посад')
    if 'shatura_pivot' in locals():
        shatura_pivot.to_excel(xls_writer, index='bdate', sheet_name='Шатура')
    if 'solar_pivot' in locals():
        solar_pivot.to_excel(xls_writer, index='bdate', sheet_name='Солнечногорск')
    if 'losino_pivot' in locals():
        losino_pivot.to_excel(xls_writer, index='bdate', sheet_name='Лосино-Петровский')
    if 'kotelniki_pivot' in locals():
        kotelniki_pivot.to_excel(xls_writer, index='bdate', sheet_name='Котельники')
    xls_writer.close()
    return devices_fname_xlsx

if __name__ == '__main__':
    print(converted_template_to_xlsx('C:\\_python_services\\aiogram_gis_bot\\2024_06_05_Template.txt'))
    quit()
    import asyncio
    loop = asyncio.get_event_loop()
    #win1251_template_csv_file = os.path.join(workdir, 'Template.csv')
    from get_documents_csv import get_semos_document, get_bdate_edate_today
    (bdate, edate) = get_bdate_edate_today()
    win1251_template_csv_file = loop.run_until_complete(get_semos_document(bdate, edate))
    win1251_template_csv_file = win1251_template_csv_file[0] if len(win1251_template_csv_file) > 0 else win1251_template_csv_file
    # win1251_template_csv_file = get_semos_document(bdate, edate)
    dataset_path = convert_semos_template_csv(win1251_template_csv_file)
    logging.info(f'dataset_path: {dataset_path}')
    if os.path.isfile(dataset_path):
        logging.info(f'Новый датасет: {dataset_path}')
        result_xlsx_path = converted_template_to_xlsx(dataset_path)
        logging.info(f'XLSX датасет: {result_xlsx_path}')
    else:
        logging.error(f'Некорректный путь к файлу [{dataset_path}]')
        result = 'NA'