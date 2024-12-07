import os, json, time
import logging
import re
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.dispatcher.filters import CommandStart
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor
from aiogram.dispatcher.handler import CancelHandler

from datetime import datetime, timedelta
#import asyncio
import yaml

from get_documents_csv import (get_bdate_edate_today,
                               get_semos_document)

from semos_template_to_xlsx import (convert_semos_template_csv,
                                    converted_template_to_xlsx)

from _gis_get_devices import (request_devices_dataset_json,
                                convert_devices_dataset_json_to_xlsx,
                                get_dataset_xlsx_path_local,
                                get_device_snum_by_name,
                                get_device_id_by_name,
                                request_device_sensors_dataset_json,
                                convert_device_sensors_json_to_xlsx,
                                get_device_sensor_ids_list,
                                request_device_sensor_values_json,
                                get_latest_dataset_xlsx_path_local,
                                get_snum_by_device_id,
                                get_device_id_by_snum)

from _get_device_sensors_history import (gis_get_history,
                                        request_devices_sample_dataset,
                                        save_devices_sample_dataset_xlsx)
from mineco_public import (save_admin_devices_table,
                            save_admin_table_pdk_json,
                            parse_admin_pdk_to_string,
                            get_rnox_is_work)

from rgis_requests import (insert_device_sensors_data,
                           check_operation_in_progress,
                           return_datetime_string,
                           over_pdk_data_json_to_telegram_text,
                           select_device_ids,
                           get_file_minutes_age)

# Configure logging
logging.basicConfig(level=logging.INFO)

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))
config_path = os.path.join(workdir, 'asyncio_gis_bot.yml')

with open(config_path, 'rb') as f:
    config = yaml.safe_load(f)

tg_token = config["tg_token"]

# initialize bot and dispatcher
bot = Bot(token=tg_token)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# define states for the conversation flow
class RegistryQueryStates(StatesGroup):
    waiting_for_dates = State()
    waiting_for_device_name = State()
    waiting_for_snum = State()
    waiting_semos_report_date = State()
    waiting_semos_upload = State()


# список разрешённых user_id
'''
Малащук К;213556354
Балагёзян В;@Moem_it;873769825
Зубков Е;@cryptoscript17;606301502
Гольцов А;1015801463
Фёдоров А;@AlexanderF1977;1036226946
Калюжный А;@anton_jet1;1627515798
Доос К;@DoosKM;979838445
Козырева Ю;@Jkozyreva;993653171
ле Алексей;274785203
Рогова Т;@bangro_ws;732415610
Баймачева К;@kibiiim;605595425
Катерина;@katerinkaa16;404067938
Родионов В;1785253081
Юля;@whoisyu1;766176122
Aik;@Budilnik01;1028586520
Irina;7607841641
'''
#   ALLOWED_USERS = [213556354,606301502,1939135390,873769825,993653171,979838445,1036226946,732415610,480720006,736471511,793998044,1015801463,1405432689,1627515798,391726088,5553713408,605595425,1785253081,782123706,274785203]
#   ALLOWED_USERS = [,,,,,,,,,,,,,,480720006,793998044,1028586520]
ALLOWED_USERS = [213556354,873769825,606301502,1015801463,1036226946,1627515798,979838445,993653171,274785203,732415610,605595425,404067938,1785253081,766176122,1028586520,7607841641]

def restrict_access(func):
    async def wrapper_restricted(message: types.Message):
        user_id = message.from_user.id
        if user_id not in ALLOWED_USERS:
            await message.answer("У вас нет прав для выполнения данного действия. Напишите @cryptoscript17")
            raise CancelHandler()
        return await func(message)

    return wrapper_restricted




async def main_menu(message):
    keyboard_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [types.KeyboardButton("🔌 Р-НОКС"),
               types.KeyboardButton("☠️ РГИС ПДК"),
               types.KeyboardButton("☠️ ПУБЛИЧКА"),
               types.KeyboardButton("🕗 РОСГИДРОМЕТ"),
               types.KeyboardButton("💾 Реестр постов"),
               types.KeyboardButton("💾 Реестр показаний"),
               types.KeyboardButton("🔍 Данные поста"),
               types.KeyboardButton("📊 Последний сэмпл"),
               types.KeyboardButton("📋 Сканер сенсоров")]
    keyboard_markup.add(*buttons)
    await message.reply("🔮 Добро пожаловать в главное меню. Выберите опцию:", reply_markup=keyboard_markup)

async def nav_buttons(message, context):
    keyboard_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [types.KeyboardButton("🔙 Назад"),
               types.KeyboardButton("🔚 Стоп")]
    keyboard_markup.add(*buttons)
    await message.reply(f"{context}", reply_markup=keyboard_markup)


#   ОБРАБОТЧИК КОМАНДЫ /start
@dp.message_handler(CommandStart())
@restrict_access
async def send_welcome(message: types.Message):
    await main_menu(message)



#   ОБРАБОТЧИК ДЛЯ КНОПКИ "🔌 Р-НОКС" button is pressed
#
#
@dp.message_handler(lambda message: message.text == "🔌 Р-НОКС")
@restrict_access
async def get_devices_registry(message: types.Message):
    await bot.send_chat_action(chat_id=message.chat.id, action=types.ChatActions.TYPING)
    rnox_json = await get_rnox_is_work()
    if 'status_code' not in rnox_json:
        text = f"\n✔️ Посты работают [{len(rnox_json['true'])}]:\n{', '.join(x for x in rnox_json['true'])}\n❌ Посты не работают [{len(rnox_json['false'])}]:\n{', '.join(x for x in rnox_json['false'])}\n❓ Посты не отвечают [{len(rnox_json['empty'])}]:\n{', '.join(x for x in rnox_json['empty'])}"
    else:
        text = f"Ошибка! Сервер вернул код ответа: {rnox_json['status_code']}"
    await message.reply(text)


#   ОБРАБОТЧИК ДЛЯ КНОПКИ "🕗 РОСГИДРОМЕТ" button is pressed
#
#
@dp.message_handler(lambda message: message.text == "🕗 РОСГИДРОМЕТ")
@restrict_access
async def get_devices_registry(message: types.Message):
    await bot.send_chat_action(chat_id=message.chat.id, action=types.ChatActions.TYPING)
    keyboard_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [types.KeyboardButton("Сегодня"),
               types.KeyboardButton("Загрузить CSV"),
               types.KeyboardButton("Отмена")]
    keyboard_markup.add(*buttons)
    await message.reply(f"Введите дату в формате YYYY-mm-dd\nНапример 2023-05-30\n\nОтчёт будет сформирован с предыдущего дня по указанную дату!", reply_markup=keyboard_markup)
    await RegistryQueryStates.waiting_semos_report_date.set()

#   ПРИНИМАЮЩАЯ АРГУМЕНТ ФУНКЦИЯ ДЛЯ КНОПКИ "🕗 РОСГИДРОМЕТ"
@dp.message_handler(lambda message: message.text != "Загрузить CSV", state=RegistryQueryStates.waiting_semos_report_date)
async def get_history_query(message: types.Message, state: FSMContext):
    is_file_sended = False
    if message.text == 'Сегодня':
        (bdate, edate) = get_bdate_edate_today()
        await message.reply(text=f'{bdate} - {edate}', reply_markup=types.ReplyKeyboardRemove())
        template_cp1251_path = await get_semos_document(bdate, edate)
        logging.info(f'Template path: {template_cp1251_path[0]}')
        if len(template_cp1251_path) > 0:
            dataset_path = convert_semos_template_csv(template_cp1251_path[0])
            logging.info(f'convert_semos_template_csv: {dataset_path}')
            if os.path.isfile(dataset_path):
                logging.info(f'Новый датасет: {dataset_path}')
                result_xlsx_path = converted_template_to_xlsx(dataset_path)
                logging.info(f'XLSX датасет: {result_xlsx_path}')
            else:
                logging.error('Некорректный путь к файлу')
                result_xlsx_path = 'NA'
            if result_xlsx_path != 'NA':
                text = f"Файл '{os.path.basename(template_cp1251_path[0])}' успешно обработан!"
                if os.path.isfile(result_xlsx_path):
                    with open(result_xlsx_path, "rb") as outfile:
                        if await bot.send_document(message.chat.id, outfile, caption=text):
                            is_file_sended = True
                else:
                    text = 'Ошибка доступа к локальному файлу!'
            else:
                text = f'Валидация файла {os.path.basename(template_cp1251_path[0])} завершилась ошибкой!'
        else:
            text = 'Получение данных из СЭМОС завершилось ошибкой.\nВозможно сеть недоступна. Попробуйте позднее...'
    elif message.text == 'Отмена':
        text = 'Отмена операции.\nНажмите /start для начала.'

    else:
        #regex = r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$'
        regex = r'\d{4}[-/.]\d{2}[-/.]\d{2}'
        if re.match(regex, message.text):
            text = "Дата соответствует формату YYYY-mm-dd"
            matches = re.search(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', message.text)

            #   получаем bdate, edate
            edate = f'{matches.group(1)}-{matches.group(2)}-{matches.group(3)} 08:00'
            edate = f'{matches.group(3)}.{matches.group(2)}.{matches.group(1)} 08:00'
            edate_timestamp_obj = datetime.strptime(edate, '%d.%m.%Y %H:%M')

            bdate_timestamp_obj = edate_timestamp_obj - timedelta(days=1)
            bdate = bdate_timestamp_obj.strftime('%d.%m.%Y %H:%M')
            text += f'\n{bdate} - {edate}'

            template_cp1251_path = await get_semos_document(bdate, edate)
            logging.info(f'Template path: {template_cp1251_path}')
            if len(template_cp1251_path) > 0:
                dataset_path = convert_semos_template_csv(template_cp1251_path[0])
                logging.info(f'convert_semos_template_csv: {dataset_path}')
                if os.path.isfile(dataset_path):
                    logging.info(f'Новый датасет: {dataset_path}')
                    result_xlsx_path = converted_template_to_xlsx(dataset_path)
                    logging.info(f'XLSX датасет: {result_xlsx_path}')
                else:
                    logging.error('Некорректный путь к файлу')
                    result_xlsx_path = 'NA'
                if result_xlsx_path != 'NA':
                    text = f"Файл '{os.path.basename(template_cp1251_path[0])}' успешно обработан!"
                    if os.path.isfile(result_xlsx_path):
                        with open(result_xlsx_path, "rb") as outfile:
                            if await bot.send_document(message.chat.id, outfile, caption=text):
                                is_file_sended = True
                    else:
                        text = 'Ошибка доступа к локальному файлу!'
                else:
                    text = f'Валидация файла {os.path.basename(template_cp1251_path[0])} завершилась ошибкой!'
            else:
                text = 'Получение данных из СЭМОС завершилось ошибкой.\nВозможно сеть недоступна. Попробуйте позднее...'


        else:
            text = "Дата не соответствует формату YYYY-mm-dd"
    if not is_file_sended:
        await message.reply(text=text, reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


@dp.message_handler(lambda message: message.text == "Загрузить CSV", state=RegistryQueryStates.waiting_semos_report_date)
async def get_history_query(message: types.Message, state: FSMContext):
    await message.answer(text='Ожидаю получения выгрузки в формате СЭМОС.', reply_markup=types.ReplyKeyboardRemove())
    await RegistryQueryStates.waiting_semos_upload.set()

# Создаем обработчик сообщений с файлами
@dp.message_handler(content_types=['document'], state=RegistryQueryStates.waiting_semos_upload)
async def process_file(message: types.Message, state: FSMContext):
    if message.text == 'stop':
        await state.finish()
    document = message.document
    logging.info(f'document: {document}')
    if document.mime_type == 'text/csv':
        logging.info(f'file: {document}')
        download_path = os.path.join(workdir, f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}_{document.file_name}")
        with open(document.file_name, 'wb') as f:
            await message.document.download(destination_file=download_path)
        dataset_path = convert_semos_template_csv(download_path)
        logging.info(f'convert_semos_template_csv: {dataset_path}')
        if os.path.isfile(dataset_path):
            logging.info(f'Новый датасет: {dataset_path}')
            result_xlsx_path = converted_template_to_xlsx(dataset_path)
            logging.info(f'XLSX датасет: {result_xlsx_path}')
        else:
            logging.error('Некорректный путь к файлу')
            result_xlsx_path = 'NA'
        if result_xlsx_path != 'NA':
            text = f"Файл '{document.file_name}' успешно обработан!"
            if os.path.isfile(result_xlsx_path):
                with open(result_xlsx_path, "rb") as outfile:
                    await bot.send_document(message.chat.id, outfile)
            else:
                text = 'Ошибка доступа к локальному файлу!'
        else:
            text = f'Валидация файла {document.file_name} завершилась ошибкой!'
    else:
        text = f"Содержимое {document.mime_type} не соответствует 'text/csv'"
    await bot.send_message(chat_id=message.chat.id, text=text, reply_markup=types.ReplyKeyboardRemove())
    await state.finish()




#   ОБРАБОТЧИК ДЛЯ КНОПКИ "☠️ РГИС ПДК" button is pressed
#
#
@dp.message_handler(lambda message: message.text == "☠️ РГИС ПДК")
@restrict_access
async def get_devices_registry(message: types.Message):
    await bot.send_chat_action(chat_id=message.chat.id, action=types.ChatActions.TYPING)
    table_pdk = parse_admin_pdk_to_string(True)

    if table_pdk[0]:
        if len(table_pdk[1]) > 3300:
            sered = table_pdk[1].find('\n', 3200)
            table_first = table_pdk[1][0:sered-1]
            table_second = table_pdk[1][sered:len(table_pdk[1])]
            await message.reply(f'💥 Данные о превышениях ПДКсс из админки РГИС:\n{table_first}')
            await message.reply(f'{table_second}')
        else:
            await message.reply(f'💥 Данные о превышениях ПДКсс из админки РГИС:\n{table_pdk[1]}')
    else:
        await message.reply(f'❗ Ошибка! Получение данных с РГИС не увенчалось успехом.\n{table_pdk[1]}')



#   ОБРАБОТЧИК ДЛЯ КНОПКИ "☠️ ПУБЛИЧКА" button is pressed
#
#
@dp.message_handler(lambda message: message.text == "☠️ ПУБЛИЧКА")
@restrict_access
async def get_devices_registry(message: types.Message):
    await bot.send_chat_action(chat_id=message.chat.id, action=types.ChatActions.TYPING)
    device_ids = select_device_ids()

    # status_file_path = os.path.join(workdir, 'operation_status.txt')
    over_pdk_data_json_path = os.path.join(workdir, '_over_pdk_data_json.txt')
    if os.path.exists(over_pdk_data_json_path):
        # if get_file_minutes_age(over_pdk_data_json_path) > 5:
        os.remove(over_pdk_data_json_path)
    # with open(status_file_path, "w") as outfile:
    #    outfile.write('+')
    bot_msg = await bot.send_message(chat_id=message.chat.id, text=f"⌛ Scanning [{len(device_ids)}] devices. Please wait for 2-3 minutes..")

    msg = f'💥 Данные о превышениях ПДКсс из публички РГИС:\n'
    msg += f"{return_datetime_string()}\n"

    fpath = insert_device_sensors_data([message.chat.id], True)
    if os.path.exists(fpath):
        msg += over_pdk_data_json_to_telegram_text(fpath)
    else:
        msg = 'Not found. Wait 5 minutes and try again...'
    # bot_msg = await bot.send_message(chat_id=message.chat.id, text=msg)
    await bot_msg.edit_text(msg)

#   ПРИНИМАЮЩАЯ АРГУМЕНТ ФУНКЦИЯ ДЛЯ КНОПКИ "💾 Реестр постов"
@dp.message_handler(lambda message: message.text == "💾 Реестр постов")
@restrict_access
async def get_devices_registry(message: types.Message):
    #dataset_xlsx_path = get_dataset_xlsx_path_local()
    #if len(dataset_xlsx_path) < 1:
    await bot.send_chat_action(chat_id=message.chat.id, action=types.ChatActions.TYPING)
    dataset_json_path = request_devices_dataset_json()
    dataset_xlsx_path = convert_devices_dataset_json_to_xlsx(dataset_json_path)
    if os.path.exists(dataset_xlsx_path):
        with open(dataset_xlsx_path, 'rb') as file:
            await message.reply_document(file, caption='📋 Реестр постов ГИС.')
    else:
        await message.reply('❗ Ошибка! Файл XLSX отсутствует!')

#   ПРИНИМАЮЩАЯ АРГУМЕНТ ФУНКЦИЯ ДЛЯ КНОПКИ "📋 Сканер сенсоров"
@dp.message_handler(lambda message: message.text == "📋 Сканер сенсоров")
@restrict_access
async def get_devices_sensors(message: types.Message):
    await bot.send_chat_action(chat_id=message.chat.id, action=types.ChatActions.TYPING)
    json_data = request_devices_sample_dataset()
    if json_data is not None:
        devices_sensors_xlsx = save_devices_sample_dataset_xlsx(json_data[1])
        if devices_sensors_xlsx is not None:
            if os.path.exists(devices_sensors_xlsx):
                with open(devices_sensors_xlsx, 'rb') as file:
                    if len(json_data[0]) > 0:
                        devices = f"\nПосты без ответа: {','.join(str(x) for x in json_data[0])}"
                    else:
                        devices = '\nКоличество постов в запросе соответствует количеству в ответе.'
                    await message.reply_document(file, caption=f"📋 Выгрузка значений сенсоров ['CO', 'NO2', 'H2S', 'NH3', 'SO2'] по списку постов RNOX.{devices}")
            else:
                await message.reply('❗ Ошибка! Файл XLSX отсутствует!')
        else:
            await message.reply('❗ Ошибка обработки ответа API.')
    else:
        await message.reply('❗ Ошибка получения ответа API.')


#   ОБРАБОТЧИК ДЛЯ КНОПКИ "💾 Реестр показаний"
#
#
@dp.message_handler(lambda message: message.text == "💾 Реестр показаний")
@restrict_access
async def get_history_registry(message: types.Message):
    context = "Введите идентификатор поста и диапазон дат в формате 'ID|YYYY-MM-DD|YYYY-MM-DD'"
    await nav_buttons(message, context)
    await RegistryQueryStates.waiting_for_dates.set()

#   ПРИНИМАЮЩАЯ АРГУМЕНТ ФУНКЦИЯ ДЛЯ КНОПКИ "💾 Реестр показаний"

@dp.message_handler(lambda message: message.text != "🔚 Стоп", state=RegistryQueryStates.waiting_for_dates)
async def get_history_query(message: types.Message, state: FSMContext):
    await bot.send_chat_action(chat_id=message.chat.id, action=types.ChatActions.TYPING)
    if message.text == '🔙 Назад':
        await main_menu(message)
        await state.finish()
    else:
        dates_regex = r'^\d+\|\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}'
        if not re.match(dates_regex, message.text):
            await message.reply("😕 Неправильный формат запроса.\nВведите идентификатор поста и диапазон дат в формате 'ID|YYYY-MM-DD|YYYY-MM-DD'\nНапример: 139|2023-05-17|2023-05-18")
            return

        date_range = message.text.split('|')
        device_id = date_range[0]
        start_date = str(datetime.strptime(date_range[1], '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S'))
        #end_date = str(datetime.strptime(date_range[2], '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S'))
        end_date = str(datetime.strptime(date_range[2], '%Y-%m-%d').strftime('%Y-%m-%dT'))
        cur_time = datetime.now().strftime("%H:%M:%S")
        logging.info(f'bdate: {start_date}, edate: {end_date}, {type(start_date)}')

        dataset_xlsx_path = gis_get_history(start_date, f'{end_date}{cur_time}', device_id)  # call the function to get history registry
        if os.path.exists(dataset_xlsx_path):
            with open(dataset_xlsx_path, 'rb') as file:
                await message.reply_document(file, caption=f'📡 💾 Реестр показаний поста {device_id}.')
        else:
            await message.reply('❗ Ошибка! Файл XLSX отсутствует!')
        await state.finish()


#   ОБРАБОТЧИК ДЛЯ КНОПКИ "🔍 Данные поста"
#
#
@dp.message_handler(lambda message: message.text == "🔍 Данные поста")
@restrict_access
async def get_device_info_query(message: types.Message):
    keyboard_markup = types.ReplyKeyboardRemove()
    context = "Введите название поста (как в ГИС):"
    await nav_buttons(message, context)
    #await message.reply("Введите название поста (как в ГИС):", reply_markup=keyboard_markup)
    await RegistryQueryStates.waiting_for_device_name.set()

#   ПРИНИМАЮЩАЯ АРГУМЕНТ ФУНКЦИЯ ДЛЯ КНОПКИ "🔍 Данные поста"
@dp.message_handler(lambda message: message.text != "🔚 Стоп", state=RegistryQueryStates.waiting_for_device_name)
async def get_device_info(message: types.Message, state: FSMContext):
    if message.text == '🔙 Назад':
        await main_menu(message)
        await state.finish()
    else:
        logging.info(len(message.text.split('-')))
        if len(message.text.split('-')) == 3:
            splitted_device_name = message.text.split('-')
            device_name = f'{splitted_device_name[0].capitalize()}-{splitted_device_name[1].capitalize()}-{splitted_device_name[2]}'
        elif len(message.text.split('-')) == 2:
            splitted_device_name = message.text.split('-')
            device_name = f'{splitted_device_name[0].capitalize()}-{splitted_device_name[1].capitalize()}'
        else:
            device_name = message.text.strip().capitalize()
        dataset_xlsx_path_local = get_dataset_xlsx_path_local()
        snum, device_id = get_device_snum_by_name(dataset_xlsx_path_local, device_name), get_device_id_by_name(dataset_xlsx_path_local, device_name)
        await message.reply(f'Пост: {device_name}\nИдентификатор поста: {device_id}\nЗаводской номер: {snum}')

        async with state.proxy() as data:
            data['device_name'] = device_name
            data['snum'] = snum
            data['device_id'] = device_id
            logging.info(f'waiting_for_device_name data: {data}')

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        btn_info = [types.KeyboardButton('Сэмпл'), types.KeyboardButton('Ближайшие'), types.KeyboardButton('🔚 Стоп')]
        markup.add(*btn_info)
        await message.answer(f"Операции с постом: {message.text}", reply_markup=markup)
        #await state.finish()
        await RegistryQueryStates.waiting_for_snum.set()

#   РАСШИРЕННОЕ МЕНЮ ИНФОРМАЦИИ О ПОСТЕ "🔍 Данные поста" - "Сэмпл"
#
#

#   ОБРАБОТЧИК ДЛЯ КНОПКИ "Сэмпл"
@dp.message_handler(text="Сэмпл", state=RegistryQueryStates.waiting_for_snum)
async def info_button_handler(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        device_id = data['device_id']
        snum = data['snum']
        device_name = data['device_name']
    if str(device_id).isdigit():
        dataset_json_path = request_device_sensors_dataset_json(int(device_id))
        sensor_ids = get_device_sensor_ids_list(dataset_json_path)
        logging.info(f'sensors:  {sensor_ids}')
        sensors_dataset_path = request_device_sensor_values_json(sensor_ids)
        with open(sensors_dataset_path, newline='', encoding='utf-8') as json_file:
            json_text = ''.join(x for x in json_file.readlines())
            json_data = json.loads(json_text)
        table = []
        for item in json_data:
            values = list(item.values())
            table.append(' ⋮ '.join(str(x) for x in values))
        table = '\n'.join(str(x) for x in table)
        bdate_raw = str(os.path.basename(sensors_dataset_path))[0:19]
        bdate = f'{bdate_raw[0:10].replace("_", "-")} {bdate_raw[11:19].replace("_", ":")}'
        await message.reply(f"Название поста: {device_name}\nИдентификатор поста: {device_id}\nЗаводской номер: {snum}\nСписок сенсоров: [{', '.join(str(x) for x in sensor_ids)}]\nДанные сенсоров на ⌚ {bdate}:\n{table}", reply_markup=types.ReplyKeyboardRemove())
    else:
        logging.info(f'device_info sample data: {data}')
        await bot.send_message(message.chat.id, "Произошла ошибка.", reply_markup=types.ReplyKeyboardRemove())
    await state.finish()



#   ОБРАБОТЧИК ДЛЯ КНОПКИ "📊 Последний сэмпл"
#
#
@dp.message_handler(lambda message: message.text == "📊 Последний сэмпл")
@restrict_access
async def get_last_sample_query(message: types.Message):
    context = "Введите номер устройства ANxxxx, идентификатор поста или наименование как указано ГИС:\nНапример: AN0020, 137, Химки-5 (выбрать одно из)"
    await nav_buttons(message, context)
    await RegistryQueryStates.waiting_for_snum.set()

#   ПРИНИМАЮЩАЯ АРГУМЕНТ ФУНКЦИЯ ДЛЯ КНОПКИ "📊 Последний сэмпл"
@dp.message_handler(lambda message: message.text != "🔚 Стоп", state=RegistryQueryStates.waiting_for_snum)
async def get_last_sample(message: types.Message, state: FSMContext):
    if message.text == '🔙 Назад':
        await main_menu(message)
        await state.finish()
    else:
        async with state.proxy() as data:
            logging.info(f'waiting_for_snum data: {data}')
        if message.text.isdigit() and 1 <= int(message.text) <= 1000:
            dataset_xlsx_path = get_latest_dataset_xlsx_path_local()
            if len(dataset_xlsx_path) < 1:
                dataset_xlsx_path = convert_devices_dataset_json_to_xlsx(request_devices_dataset_json())

            snum = get_snum_by_device_id(dataset_xlsx_path, message.text)
            await message.reply(f"Получен идентификатор поста: {message.text}, соответствующий заводскому номеру: {snum}")
        elif message.text.upper().startswith('AN'):
            dataset_xlsx_path = get_latest_dataset_xlsx_path_local()
            if len(dataset_xlsx_path) < 1:
                dataset_xlsx_path = convert_devices_dataset_json_to_xlsx(request_devices_dataset_json())
            device_id = get_device_id_by_snum(dataset_xlsx_path, message.text.upper())
            logging.info(f"dataset_xlsx_path: {dataset_xlsx_path}, device_id: {device_id}, snum: {message.text.upper()}")
            await message.reply(f"Получен заводской номер поста: {message.text.upper()}, соответствующий идентификатору: {device_id}")
        elif re.match('^[а-яА-Я0-9]', message.text):
            dataset_xlsx_path = get_latest_dataset_xlsx_path_local()
            if len(dataset_xlsx_path) < 1:
                dataset_xlsx_path = convert_devices_dataset_json_to_xlsx(request_devices_dataset_json())
            device_id = get_device_id_by_name(dataset_xlsx_path, message.text)
            await message.reply(f"Получено наименование поста: {message.text}, соответствующее идентификатору: {device_id}")
        else:
            print("Запрос не соответствует требованиям.")
            await message.reply("😕 Запрос не соответствует требованиям.")
            return

        if str(device_id).isdigit():
            dataset_json_path = request_device_sensors_dataset_json(int(device_id))
            sensor_ids = get_device_sensor_ids_list(dataset_json_path)
            logging.info(f'sensors:  {sensor_ids}')
            sensors_dataset_path = request_device_sensor_values_json(sensor_ids)
            with open(sensors_dataset_path, newline='', encoding='utf-8') as json_file:
                json_text = ''.join(x for x in json_file.readlines())
                json_data = json.loads(json_text)
            table = []
            for item in json_data:
                values = list(item.values())
                table.append(' ⋮ '.join(str(x) for x in values))
            table = '\n'.join(str(x) for x in table)
            bdate_raw = str(os.path.basename(sensors_dataset_path))[0:19]
            bdate = f'{bdate_raw[0:10].replace("_", "-")} {bdate_raw[11:19].replace("_", ":")}'
            await message.reply(f"Идентификатор поста: {device_id}\nСписок сенсоров: [{', '.join(str(x) for x in sensor_ids)}]\nДанные сенсоров на ⌚ {bdate}:\n{table}")
            await state.finish()
        else:
            await message.reply(f"Идентификатор поста: {device_id} содержит ошибку")
            return

#   ОБРАБОТЧИК КНОПКИ "🔚 Стоп"
#
#
@dp.message_handler(lambda message: message.text == "🔚 Стоп", state='*')
async def stop_button_handler(message: types.Message, state: FSMContext):
    await bot.send_message(message.chat.id, "Вы завершили взаимодействие с ботом. Для начала работы введите команду /start", reply_markup=types.ReplyKeyboardRemove())
    await state.finish()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)