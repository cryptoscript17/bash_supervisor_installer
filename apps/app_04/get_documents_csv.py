import os, glob, time, logging
from datetime import datetime
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import NoSuchElementException, TimeoutException

logging.basicConfig(level=logging.INFO)

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

workdir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)))

microtime = 1

def get_bdate_edate_today():
    import math
    bdate_ts = int(1200 * math.floor(-1 + time.time() / 1200))
    edate_ts = int(1200 * math.ceil(-1 + time.time() / 1200))

    short_dates = True
    if not short_dates:
        #result_format = '%d.%m.%Y %H:%M'
        result_format = '%d.%m.%Y 08:00'
    else:
        bdate_ts = bdate_ts - 24 * 60 * 60
        result_format = '%d.%m.%Y 08:00'
    (bdate, edate) = (datetime.fromisoformat(datetime.fromtimestamp(bdate_ts).isoformat()).strftime(result_format), datetime.fromisoformat(datetime.fromtimestamp(edate_ts).isoformat()).strftime(result_format))
    return (bdate, edate)

async def get_semos_document(bdate, edate):
    logging.info(f'PERIOD:  {bdate} - {edate}')
    current_date = datetime.now().strftime('%Y_%m_%d_')

    temp_workdir = os.path.join(workdir, f'{current_date}temp_workdir')
    if not os.path.exists(temp_workdir):
        os.makedirs(temp_workdir)
        logging.info(f"Папка {temp_workdir} успешно создана!")

    url = 'http://10.14.16.41:80/Report/Index/eco.Reports.Std.SamplesByRows.Report_c97a1fce1436f6cb56cc0ea3c7365163'

    removed = recycle_csv_files(temp_workdir)
    #removed = "\n".join(x for x in removed)
    logging.info(f'Removing ALL in folder {temp_workdir}...\n{removed}')

    options = Options()
#    options.add_argument("--private-window")
    options.add_argument("--width=1024")
    options.add_argument("--height=768")

    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", temp_workdir)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
    # options.binary_location = os.path.join(workdir, 'geckodriver.exe')
    options.binary_location = "C:\\Program Files\\Firefox Developer Edition\\firefox.exe"
    try:
        driver = webdriver.Firefox(options=options)
        driver.get(url)

        #   AUTHORIZATION
        login_field = WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#UserName_I")))
        login_field.send_keys("Администратор")

        pwd_field = WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#Password_I")))
        pwd_field.send_keys("")

        login_button = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#LogOnButton_CD")))
        login_button.click()

        time.sleep(microtime*3)
        #   //AUTHORIZATION


        #   PARSING | SETTING EXPORT FORMAT AND REPORT PARAMETERS
        try:
            filter_inputs_el = WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#FilterSplitter")))
            if isinstance(filter_inputs_el, object):
                period_input = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#cbFilterPeriodMode_I")))
                driver.execute_script("arguments[0].setAttribute('value','Произвольный')", period_input)

                bdate_el = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#deFilterStartDate_I")))
                bdate_el.clear()
                bdate_keys = str(bdate).strip()
                bdate_el.send_keys(bdate_keys)
                #bdate_el.send_keys('12.12.2022 08:00')
                time.sleep(microtime)

                edate_el = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#deFilterEndDate_I")))
                edate_el.clear()
                edate_keys = str(edate).strip()
                edate_el.send_keys(edate_keys)
                #edate_el.send_keys('18.12.2022 08:00')
                time.sleep(microtime)
                #print((bdate_el.get_attribute('value'), edate_el.get_attribute('value')))
            else:
                print('period input listbox web element not found','')
            time.sleep(microtime)

        except NoSuchElementException:
            #print('exception  case', '')
            period_input = WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '#cbFilterPeriodMode_I')))
            period_input.click()
            time.sleep(microtime/5)
            sutki_input = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#cbFilterPeriodMode_DDD_L_LBI4T0")))
            sutki_input.click()
            time.sleep(microtime/5)
        #cities.find_elements(By.XPATH, './*')
        checkboxes_parent = WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#filterPlacesTreeView")))
        #checkboxes_parent_count = len(checkboxes_parent.find_elements(by=By.CSS_SELECTOR, value='.dxtv-ln')) - 1
        checkboxes_parent_count = len(checkboxes_parent.find_elements(by=By.CSS_SELECTOR, value='#filterSamplesListBox_LBT > tr.dxeListBoxItemRow_Office2010Blue')) - 1
        if checkboxes_parent_count > 0:
            logging.info(f'Found {checkboxes_parent_count} checkbox elements, iterating...')
            for item_pos in range(0, checkboxes_parent_count):

                try:
                    station_element = checkboxes_parent.find_elements(by=By.CSS_SELECTOR, value=f'#filterPlacesTreeView_N{item_pos}')[item_pos]
                    # station_status = checkboxes_parent.find_elements(by=By.CSS_SELECTOR, value=f'#filterPlacesTreeView_N{item_pos} > span:nth-child(1)')[0].get_attribute('class')
                    station_status = checkboxes_parent.find_elements(by=By.CSS_SELECTOR, value=f'#filterPlacesTreeView_N{item_pos}_D')[0].get_attribute('class')

                    station_caption = checkboxes_parent.find_elements(by=By.CSS_SELECTOR, value=f'#filterPlacesTreeView_N{item_pos} > span:nth-child(2)')[0].text
                    print(type(station_element),type(station_status), type(station_caption))
                    if "СЭП-Ш" in station_caption and 'CheckBoxUnchecked' in station_status:
                        station_element[0].click()
                        time.sleep(microtime)
                    if "ПЭЛ" in station_caption and 'CheckBoxChecked' in station_status:
                        station_element[0].click()
                        time.sleep(microtime)
                except Exception as err:
                    logging.error(f'Error while parsing station_element HTML-Node:\n{err}')
        time.sleep(microtime)
        refresh_button = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#btnSubmit_CD")))
        refresh_button.click()
        time.sleep(microtime*8)

        export_type_input = WebDriverWait(driver, 35).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '#dvReportViewer_Splitter_Toolbar_Menu_ITCNT11_SaveFormat_I')))
        export_type_input.click()
        time.sleep(microtime*2)

        csv_type_input = WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '#dvReportViewer_Splitter_Toolbar_Menu_ITCNT11_SaveFormat_DDD_L_LBI8T0')))
        csv_type_input.click()
        time.sleep(microtime)

        save_export_btn = WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '#dvReportViewer_Splitter_Toolbar_Menu_DXI9_')))
        save_export_btn.click()
        time.sleep(microtime*4)

        templates_csv = glob.glob(temp_workdir + '\\Template*.csv')
    except Exception as e:
        logging.info(f'Selenium error: {e}')
        driver.quit()
        return []
    finally:
        #   await asyncio.sleep(3)
        #if driver:
        #    driver.quit()
        pass
    return templates_csv



def recycle_csv_files(workdir):
  #   REMOVING TEMP FILES
  removed = []
  remove_raw_templates, remove_prep_templates,remove_pivot_templates = True, True, True

  templates_csv = glob.glob(workdir + '\\Template*.csv')
  prepared_csv = glob.glob(workdir + '\\*_Semos_UTF8_Prepared_*.csv')
  pivot_csv = glob.glob(workdir + '\\*_Semos_UTF8_Pivot_Report_*.csv')

  if remove_raw_templates:
      for i, f in enumerate(templates_csv):
        removed.append(os.path.basename(f))
        os.remove(f)

  if remove_prep_templates:
      for i, f in enumerate(prepared_csv):
        removed.append(os.path.basename(f))
        os.remove(f)

  if remove_pivot_templates:
      for i, f in enumerate(pivot_csv):
        removed.append(os.path.basename(f))
        os.remove(f)

  return '\n'+'\n'.join(str(x) for x in list(removed))

if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    (bdate, edate) = get_bdate_edate_today()
    res = loop.run_until_complete(get_semos_document(bdate, edate))

    logging.info(f'INFO:   DATE_TIME PERIOD is ...{(bdate, edate)}')
    logging.info(res)