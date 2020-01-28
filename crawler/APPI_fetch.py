from bs4 import BeautifulSoup
from PIL import Image
from subprocess import check_output

import os
CUR_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.dirname(CUR_DIR)
REPO_DIR = os.path.dirname(ROOT_DIR)

import sys
sys.path.insert(0, ROOT_DIR)

import errno
import pytesseract
import cv2

import argparse
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, SessionNotCreatedException, TimeoutException, WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys

import requests
import time
import unittest
import datetime
from libtech_lib.generic.commons import logger_fetch
from libtech_lib.wrappers.sn import driverInitialize, driverFinalize, displayInitialize, displayFinalize
from libtech_lib.generic.aws import get_aws_parquet, upload_s3
from libtech_lib.generic.html_functions import get_dataframe_from_html
from libtech_lib.rayatubarosa.models import RBCrawler, RBLocation
from libtech_lib.generic.api_interface import api_get_tag_id

import psutil
import pandas as pd
import json

# For crawler.py

from slugify import slugify
import csv
import urllib.parse as urlparse


#######################
# Global Declarations
#######################

timeout = 3
directory = '/home/mayank/libtech/src/scripts/AllDistricts'
#directory = '/home/mayank/libtech/src/scripts/Vishakhapatnam'
base_url = 'https://meebhoomi.ap.gov.in/'

village_list = [('విశాఖపట్నం', 'అచ్యుతాపురం', 'జోగన్నపాలెం'), ('విశాఖపట్నం', 'అనంతగిరి', 'నిన్నిమామిడి'), ('విశాఖపట్నం', 'అనందపురం', 'ముచ్చెర్ల')]
skip_district = ['3',]
is_visible = True  # Make False before commmitting
is_mac = True      # Make False before commmitting
is_mynk = True

#############
# Functions
#############
def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This script will crawl',
                                                  'locations for ryatu barosa '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-lc', '--locationCode', help='Location Code for input', required=False)
    parser.add_argument('-sn', '--sampleName', help='Tag ID to retrieve the codes', required=False)
    parser.add_argument('-lt', '--locationType',
                        help='Location type that needs tobe instantiated', required=False)
    parser.add_argument('-fn', '--func_name', help='Name of the function', required=False)
    parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args


def on_terminate(proc):
    print("process {} terminated with exit code {}".format(proc, proc.returncode))

def process_cleanup(logger):
    logger.info('Process Cleanup Begins')
    children = psutil.Process().children(recursive=True)
    for p in children:
        logger.info('Terminating the subproces[%s]' % p.pid)
        try:
            p.terminate()
            p.wait()
        except Exception as e:
            logger.error('Kill failed with Exception[%s]' % e)
    gone, alive = psutil.wait_procs(children, timeout=10, callback=on_terminate)

    logger.info('Processes still alive [%s]' % alive)
    for p in children: # alive:
        logger.info('Killing the subproces[%s]' % p.pid)
        try:
            p.kill()
            p.wait()
        except Exception as e:
            logger.error('Kill failed with Exception[%s]' % e)
    logger.info('Cleaning up /tmp')
    os.system('cd /tmp; pkill firefox; pkill Xvfb; rm -rf rust_mozprofile.* tmp*')
    logger.info('Process Cleanup Ends')


def fetch_parent(logger, driver, url=None):
    if not url:
        url = base_url + 'ROR.aspx'
    filename = '%s/parent.html' % (directory)
    logger.info("Fetching...[%s]" % url)

    try:
        driver.get(url)
        # time.sleep(5)

        logger.info('Waiting for the base page to load...')
        elem = WebDriverWait(driver, 10).until(
          EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_txtCaptcha"))
        )
    except Exception as e:
        logger.critical('Exception on WebDriverWait(10) - EXCEPT[%s:%s]' % (type(e), e))
        return 'ABORT'

    html_source = driver.page_source.replace('<head>',
                                                 '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>')
    logger.debug("HTML Fetched [%s]" % html_source)

    with open(filename, 'w') as html_file:
        logger.info('Writing [%s]' % filename)
        html_file.write(html_source)

    return BeautifulSoup(html_source, 'html.parser')


def fetch_lookup(logger, filename, url=None, cookies=None, headers=None, data=None):
    if os.path.exists(filename):
        logger.info('File already downloaded. Reading [%s]...' % filename)
        with open(filename) as json_file:
            lookup = json.load(json_file)
        return lookup

    logger.info('Fetching URL[%s]...' % url)
    response = requests.post(url, headers=headers, cookies=cookies, data=data)
    logger.info(response.content)

    data = pd.read_json(response.content)
    if data['d'].empty:
        logger.error('Why empty?')
        #exit(0)
    df = pd.DataFrame(data['d'].tolist(), columns=['name', 'value'])

    lookup = df.set_index('value').to_dict('dict')['name']
    logger.debug(lookup)

    with open(filename, 'w') as json_file:
        logger.info('Writing [%s]' % filename)
        json_file.write(json.dumps(lookup))

    return lookup


def fetch_captcha(logger, cookies=None, url=None):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:70.0) Gecko/20100101 Firefox/70.0',
        'Accept': 'image/webp,*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Referer': 'https://meebhoomi.ap.gov.in/ROR.aspx',
        'DNT': '1',
    }

    #response = requests.get('https://meebhoomi.ap.gov.in/CaptchaImage.axd', headers=headers, params=params, cookies=cookies)
    response = requests.get(url, headers=headers, cookies=cookies)

    filename = 'captcha.jpg'
    with open(filename, 'wb') as html_file:
        logger.info('Writing [%s]' % filename)
        html_file.write(response.content)

    check_output(['convert', filename, '-resample', '35', filename])

    return pytesseract.image_to_string(Image.open(filename), lang='eng', config='--psm 10 --oem 3 -c tessedit_char_whitelist=0123456789')
    #return pytesseract.image_to_string(Image.open(filename), lang='eng', config='-c tessedit_char_whitelist=0123456789')


def fetch_appi_gram1b_report(logger, driver, cookies=None, dirname=None, url=None, district_no=None, mandal_no=None, village_no=None, filename=None):
    logger.info('Verify District[%s] > Mandal[%s] > Village[%s]' % (district_no, mandal_no, village_no))
    if not dirname:
        dirname = directory

    if not url:
        url = base_url + 'ROR.aspx'

    captcha_text = ''

    logger.info('Verifying File [%s]' % filename)
    if os.path.exists(filename):
        logger.info('File already downloaded. Skipping [%s]' % filename)
        return 'SUCCESS'

    bs = fetch_parent(logger, driver)

    #timeout = 2
    try:
        select = Select(driver.find_element_by_id('ContentPlaceHolder1_ddlDist'))
        select.select_by_value(district_no)
        logger.info('Selected District [%s]' % district_no)
        time.sleep(timeout)

        select = Select(driver.find_element_by_id('ContentPlaceHolder1_ddlMandals'))
        select.select_by_value(mandal_no)
        logger.info('Selected Mandal [%s]' % mandal_no)
        time.sleep(timeout)

        select = Select(driver.find_element_by_id('ContentPlaceHolder1_ddlVillageName'))
        select.select_by_value(village_no)
        logger.info('Selected Village [%s]' % village_no)
        time.sleep(timeout)

        imgs = bs.findAll("img")
        logger.debug('imgs[%s]' % imgs)
        logger.info('no of images = %s!' % len(imgs))
        if len(imgs) < 4:
            img = imgs[1]
        else:
            img = imgs[3]
        logger.debug('Yippie [%s]' % img.attrs)

        url = base_url + img['src']
        logger.info('Fetching URL[%s]' % url)

        captcha_text = fetch_captcha(logger, cookies, url)
        time.sleep(timeout)
        logger.info('Captcha Text[%s]' % captcha_text)
        if len(captcha_text) != 5 or not captcha_text.isdigit():
            logger.warning('Incorrect Captcha length[%s] or non digit data' % len(captcha_text))
            return 'FAILURE'

        elem = driver.find_element_by_id('ContentPlaceHolder1_txtCaptcha')
        elem.send_keys(captcha_text)
        elem.click()

        logger.info('Clicking Submit')
        elem = driver.find_element_by_id('ContentPlaceHolder1_btn_go')
        elem.click()

    except Exception as e:
        logger.error('Exception for Captcha[%s] - EXCEPT[%s:%s]' % (captcha_text, type(e), e))
        time.sleep(timeout)
        return 'FAILURE'

    try:
        WebDriverWait(driver, timeout).until(EC.alert_is_present(),
                                   'Timed out testing if alert is there')

        alert = driver.switch_to.alert
        alert.accept()
        logger.warning('Handled Alert!')
        return 'FAILURE'
    except TimeoutException:
        logger.debug('Time Out waiting for ALERT')
    except Exception as e:
        logger.error('Exception during wait for alert captcha_id[%s] - EXCEPT[%s:%s]' % (captcha_text, type(e), e))

    parent_handle = driver.current_window_handle
    #logger.info("Handles : %s" % driver.window_handles + "Number : %d" % len(driver.window_handles))
    logger.info("Handles : [%s]    Number : [%d]" % (driver.window_handles, len(driver.window_handles)))

    if len(driver.window_handles) == 2:
        logger.info('Switching Window...')
        driver.switch_to.window(driver.window_handles[1])
        logger.info('Switched!!!')
        #time.sleep(2)
    else:
        logger.error("Handlers gone wrong [" + str(driver.window_handles) + 'captcha_id %s' % captcha_text + "]")
        driver.save_screenshot('./button_'+captcha_text+'.png')
        return 'FAILURE'
    try:
        logger.info('Waiting for the dialog box to open')
        elem = WebDriverWait(driver, timeout).until(
          EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_lbl_village"))
        )
    except (WebDriverException) as e:
        logger.critical('Not found for captcha_id[%s] - EXCEPT[%s:%s]' % (captcha_text, type(e), e))
        driver.close()
        driver.switch_to.window(parent_handle)
        return 'ABORT'
    except TimeoutException as e:
        logger.error('Timeout waiting for dialog box - EXCEPT[%s:%s]' % (type(e), e))
        driver.close()
        driver.switch_to.window(parent_handle)
        return 'ABORT'
    except Exception as e:
        logger.error('Exception on WebDriverWait(10) - EXCEPT[%s:%s]' % (type(e), e))
        driver.save_screenshot('./button_'+captcha_text+'.png')
        driver.close()
        driver.switch_to.window(parent_handle)
        return 'ABORT'

    try:
        html_source = driver.page_source.replace('<head>',
                                                 '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>')
        logger.debug("HTML Fetched [%s]" % html_source)
        with open(filename, 'w') as html_file:
            logger.info('Writing [%s]' % filename)
            html_file.write(html_source)

        driver.close()
        driver.switch_to.window(parent_handle)
    except WebDriverException:
        logger.critical('Aborting the current attempt')
        return 'ABORT'
    except SessionNotCreatedException:
        logger.critical('Aborting the current attempt')
        return 'ABORT'
    except Exception as e:
        logger.error('Exception for captcha_id[%s] - EXCEPT[%s:%s]' % (captcha_text, type(e), e))
        return 'FAILURE'

    return 'SUCCESS'


def fetch_gram_1b_reports(logger, dirname=None, url=None):
    logger.info('Fetch the Gram 1B reports into dir[%s]' % dirname)

    display = displayInitialize(is_visible)
    driver = driverInitialize(timeout=3, options='--headless') # driverInitialize(path='/opt/firefox/', timeout=3)
    #driver = driverInitialize(timeout=3) # driverInitialize(path='/opt/firefox/', timeout=3)

    try:
        os.makedirs(dirname)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    #driver.get(base_url)
    fetch_parent(logger, driver)
    '''
    url = base_url

    try:
        logger.info('Requesting URL[%s]' % url)
        response = requests.get(url, timeout=timeout, cookies=cookies)
    except Exception as e:
        logger.error('Caught Exception[%s]' % e)
    '''

    cookies = driver.get_cookies()
    logger.info('Cookies[%s]' % cookies)
    logger.debug('Cookie -> Session ID[%s]' % cookies[0]['value'])

    cookies = {
        'hibext_instdsigdipv2': '1',
        'ASP.NET_SessionId': cookies[0]['value'],
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:70.0) Gecko/20100101 Firefox/70.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/json; charset=utf-8',
        'Origin': 'https://meebhoomi.ap.gov.in',
        'Connection': 'keep-alive',
        'Referer': 'https://meebhoomi.ap.gov.in/ROR.aspx',
        'DNT': '1',
    }

    data = '{"knownCategoryValues":"","category":"District"}'

    filename = '%s/district.json' % directory
    district_url = base_url + 'UtilityWebService.asmx/GetDistricts'
    district_lookup = fetch_lookup(logger, filename, url=district_url, cookies=cookies, headers=headers, data=data)
    logger.info(district_lookup)

    mandal_url = base_url + 'UtilityWebService.asmx/GetMandals'
    for district_no, district_name in district_lookup.items():
        district_name = district_name.strip()

        if district_no in skip_district:
            continue
        logger.info('Fetch Mandals for District[%s] = [%s]' % (district_name, district_no))
        data = '{"knownCategoryValues":"District:%s;","category":"Mandal"}' % (district_no)
        logger.info('With Data[%s]' % data)

        filename = '%s/%s_%s_mandal_list.json' % (directory, district_no, district_name)
        mandal_lookup = fetch_lookup(logger, filename, url=mandal_url, cookies=cookies, headers=headers, data=data)
        logger.info(mandal_lookup)

        village_url = base_url + 'UtilityWebService.asmx/GetVillages'
        for mandal_no, mandal_name in mandal_lookup.items():
            mandal_name = mandal_name.strip()
            logger.info('Fetch Villages for Mandal[%s] = [%s]' % (mandal_name, mandal_no))
            data = '{"knownCategoryValues":"District:%s;Mandal:%02s;","category":"Mandal"}' % (district_no, mandal_no)
            logger.info('With Data[%s]' % data)

            filename = '%s/%s_%s_village_list.json' % (directory, district_name, mandal_name)
            village_lookup = fetch_lookup(logger, filename, url=village_url, cookies=cookies, headers=headers, data=data)
            logger.info(village_lookup)

            for village_no, village_name in village_lookup.items():
                village_name = village_name.strip()
                logger.info('Fetch Gram 1B Report for District[%s] > Mandal[%s] > Village[%s]' % (district_name, mandal_name, village_name))
                filename = '%s/%s_%s_%s_rejected_payments.html' % (dirname, district_name, mandal_name, village_name)
                result = fetch_appi_gram1b_report(logger, driver, cookies=cookies,
                                                  district_no=district_no,
                                                  mandal_no=mandal_no,
                                                  village_no=village_no,
                                                  filename=filename)
                if result == 'ABORT':
                    logger.critical('Aborting Mission!')
                    driverFinalize(driver)
                    displayFinalize(display)
                    return result

    driverFinalize(driver)
    displayFinalize(display)
    return result # 'SUCCESS'


def fetch_gram_1b_reports_for(logger, dirname=None, url=None, villages=None):
    logger.info('Fetch the Gram 1B reports into dir[%s]' % dirname)

    display = displayInitialize(is_visible)
    driver = driverInitialize(timeout=3) # driverInitialize(path='/opt/firefox/', timeout=3)

    try:
        os.makedirs(dirname)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    fetch_parent(logger, driver)

    cookies = driver.get_cookies()
    logger.info('Cookies[%s]' % cookies)
    logger.debug('Cookie -> Session ID[%s]' % cookies[0]['value'])

    cookies = {
        'hibext_instdsigdipv2': '1',
        'ASP.NET_SessionId': cookies[0]['value'],
    }

    for (district_name, mandal_name, village_name) in villages:
        logger.info('Fetch Gram 1B Report for District[%s] > Mandal[%s] > Village[%s]' % (district_name, mandal_name, village_name))
        result = fetch_appi_gram1b_report_orig(logger, driver, cookies=cookies,
                                          district_name=district_name.strip(),
                                          mandal_name=mandal_name.strip(),
                                          village_name=village_name.strip())
    driverFinalize(driver)
    displayFinalize(display)
    return result # 'SUCCESS'


def parse_gram_1b_report(logger, filename=None, panchayat_name=None, village_name=None, captcha_text=None):
    logger.info('Parse the 1B HTML file')

    try:
        with open(filename, 'r') as html_file:
            logger.info('Reading [%s]' % filename)
            html_source = html_file.read()
    except Exception as e:
        logger.error('Exception when opening file for captcha_id[%s] - EXCEPT[%s:%s]' % (captcha_text, type(e), e))
        raise e

    data = pd.DataFrame([], columns=['S.No', 'Mandal Name', 'Gram Panchayat', 'Village', 'Job card number/worker ID', 'Name of the wageseeker', 'Credited Date', 'Deposit (INR)', 'Debited Date', 'Withdrawal (INR)', 'Available Balance (INR)', 'Diff. time credit and debit'])
    try:
        df = pd.read_html(filename, attrs = {'id': 'ctl00_MainContent_dgLedgerReport'}, index_col='S.No.', header=0)[0]
        # df = pd.read_html(filename, attrs = {'id': 'ctl00_MainContent_dgLedgerReport'})[0]
    except Exception as e:
        logger.error('Exception when reading transaction table for captcha_id[%s] - EXCEPT[%s:%s]' % (captcha_text, type(e), e))
        return data
    logger.info('The transactions table read:\n%s' % df)

    bs = BeautifulSoup(html_source, 'html.parser')

    # tabletop = bs.find(id='ctl00_MainContent_PrintContent')
    # logger.info(tabletop)
    table = bs.find(id='tblDetails')
    logger.debug(table)

    account_no = table.find(id='ctl00_MainContent_lblAccountNo').text.strip()
    logger.debug('account_no [%s]' % account_no)

    bo_name = table.find(id='ctl00_MainContent_lblBOName').text.strip()
    logger.debug('bo_name [%s]' % bo_name)

    captcha_id_id = table.find(id='ctl00_MainContent_lblCaptcha_IdPensionID').text.strip()
    logger.debug('captcha_id_id [%s]' % captcha_id_id)

    if captcha_id_id != captcha_text:
        logger.critical('Something went terribly wrong with [%s != %s]!' % (captcha_id_id, captcha_text))

    so_name = table.find(id='ctl00_MainContent_lblSOName').text.strip()
    logger.debug('so_name [%s]' % so_name)

    account_holder_name = table.find(id='ctl00_MainContent_lblName').text.strip()
    logger.debug('account_holder_name [%s]' % account_holder_name)

    mandal_name = table.find(id='ctl00_MainContent_lblMandalName').text.strip()
    logger.debug('mandal_name [%s]' % mandal_name)

    table = bs.find(id='ctl00_MainContent_dgLedgerReport')
    logger.debug(table)
    try:
        tr_list = table.findAll('tr')
    except:
        logger.info('No Transactions')
        return 'SUCCESS'
    logger.debug(tr_list)

    # desired_columns =  [1, ]
    # for row in df.itertuples(index=True, name='Pandas'):
    debit_timestamp = pd.to_datetime(0)

    df = df.iloc[::-1] # Reverse the order for calculating diff time Debit dates are easier to record in this order
    for index, row in df.iterrows():
        logger.debug('%d: %s' % (index, row))

        serial_no = index
        logger.debug('serial_no[%s]' % serial_no)

        transaction_date = row['Transaction Date']
        logger.debug('transaction_date[%s]' % transaction_date)

        transaction_ref = row['Transaction Reference']
        logger.debug('transaction_ref[%s]' % transaction_ref)

        withdrawn_at = row['Withdrawn at']
        logger.debug('withdrawn_at[%s]' % withdrawn_at)

        deposit_inr = row['Deposit (INR)']
        logger.debug('deposit_inr[%s]' % deposit_inr)

        withdrawal_inr = row['Withdrawal (INR)']
        logger.debug('withdrawal_inr[%s]' % withdrawal_inr)

        availalbe_balance = row['Available Balance (INR)']
        logger.debug('availalbe_balance[%s]' % availalbe_balance)

        if deposit_inr == 0:
            (credited_date, debited_date, diff_days, debit_timestamp) = (0, transaction_date, 0, pd.to_datetime(transaction_date, dayfirst=True)) #  datetime.strptime(transaction_date, "%d/%m/%Y").timestamp())
        else:
            (credited_date, debited_date, diff_days) = (transaction_date, 0, (debit_timestamp - pd.to_datetime(transaction_date, dayfirst=True)).days) # datetime.strptime(transaction_date, "%d/%m/%Y").timestamp())
        logger.info('credited_date[%s]' % credited_date)
        logger.info('debited_date[%s]' % debited_date)
        logger.info('diff_days[%s]' % diff_days)

        if diff_days < 0:
            diff_days = 0
            continue
        logger.info('After Reset diff_days[%s]' % diff_days)

        #csv_buffer.append('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' %(serial_no, mandal_name, bo_name, so_name, captcha_id_id, account_holder_name, credited_date, debited_date, withdrawal_inr, availalbe_balance, diff_time))
        data = data.append({'S.No': serial_no, 'Mandal Name': mandal_name, 'Gram Panchayat': panchayat_name, 'Village': village_name, 'Job card number/worker ID': captcha_id_id, 'Name of the wageseeker': account_holder_name, 'Credited Date': credited_date, 'Deposit (INR)': deposit_inr, 'Debited Date': debited_date, 'Withdrawal (INR)': withdrawal_inr, 'Available Balance (INR)': availalbe_balance, 'Diff. time credit and debit': diff_days}, ignore_index=True)

    data = data.set_index('S.No')
    data = data.iloc[::-1]  # Reverse the order back to normal
    logger.info('The final table:\n%s' % data)

    return data

def dump_gram_1b_reports(logger, dirname=None):
    #from datetime import datetime

    logger.info('Dump the RN6 HTMLs into [%s]' % dirname)

    if not isGM:
        state = None
        district_name = 'MAHABUBNAGAR'
        block_name = 'Damaragidda'
        # block_id = block_lookup[block_name]
        block_id = '4378'

        block_name = 'Maddur'
        block_code = '3614006'
        block_name = 'Koilkonda'
        block_code = '3614007'
        block_name = 'Hanwada'
        block_code = '3614008'
    else:
        state = 'ap'
        district_name = 'VISAKHAPATNAM'
        block_name = 'Gangaraju Madugula'
        block_id = None

    #dirname = block_name
    if False:
        # filename = 'captcha_ids/Gangaraju Madugula_G.Madugula_030291104271010017-01_ledger_details.html'
        filename = 'captcha_ids/Gangaraju Madugula_Gaduthuru_030291116195010015-04_ledger_details.html'
        #csv_buffer = ['S.No,Mandal Name,Gram Panchayat,Village,Job card number/worker ID,Name of the wageseeker,Credited Date,Deposit (INR),Debited Date,Withdrawal (INR),Available Balance (INR),Diff. time credit and debit\n']
        return 'SUCCESS'

    if block_code:
        panchayats = Panchayat.objects.filter(block__code=block_code)
    else:
        panchayats = Panchayat.objects.filter(block__name=block_name)
    logger.info(panchayats)
    for panchayat in panchayats:
        panchayat_name = panchayat.name
        logger.info('Panchayat[%s]' % panchayat_name)
        if needed and (panchayat_name not in needed):
            logger.info('Not interested in [%s]' % panchayat_name)
            continue
        if skip and (panchayat_name in skip):
            logger.info('To skip [%s]' % panchayat_name)
            continue
        workers = Worker.objects.filter(captcha_id__panchayat=panchayat)
        logger.info(workers)
        for worker in workers:
            logger.debug('WorkerID[%s]' % worker.id)
            tcaptcha_id = worker.captcha_id.tcaptcha_id
            if tcaptcha_id:
                captcha_text = (tcaptcha_id + '-0' + str(worker.applicantNo))
            else:
                logger.error('tcaptcha_id is NULL for [%s]' % worker)
                continue
            logger.debug('Parse HTML for captcha_text[%s]' % captcha_text)

            filename = '%s/%s_%s_%s_ledger_details.html' % (dirname, block_name, panchayat_name, captcha_text)
            village_name = worker.captcha_id.village.name
            try:
                data = parse_appi_report(logger, filename=filename, panchayat_name=panchayat_name, village_name=village_name, captcha_text=captcha_text)
            except Exception as e:
                logger.error('Caught Exception[%s]' % e)
                csv_filename = filename.replace('.html','.XXX')
                open(csv_filename, 'a').close()
                logger.info('Marking the file [%s]' % csv_filename)
                continue # break

            csv_filename = filename.replace('.html','.csv')
            logger.info('Writing to CSV [%s]' % csv_filename)
            data.to_csv(csv_filename)
            '''
            with open(csv_filename, 'w') as csv_file:
                logger.info('Writing to CSV [%s]' % csv_filename)
                csv_file.write(data.to_csv())
            '''
            # break
        # break

    '''

    url = 'http://b.libtech.info:8000/api/panchayats/?bid=%s' % block_id

    try:
        logger.info('Requesting URL[%s]' % url)
        response = requests.get(url, timeout=timeout) # , cookies=cookies)
    except Exception as e:
        logger.error('Caught Exception[%s]' % e)

    panchayats_json = response.json()
    logger.debug('Panchayats JSON[%s]' % panchayats_json)

    is_panchayat = True
    for panchayat_object in panchayats_json:
        panchayat_name = panchayat_object['name'].strip()
        panchayat_code = panchayat_object['code'].strip()
        logger.info('Fetch captcha_ids for Panchayat[%s, %s]' % (panchayat_name, panchayat_code))
        url = 'http://b.libtech.info:8000/api/getworkers/?pcode=%s' % panchayat_code
        try:
            logger.info('Requesting URL[%s]' % url)
            response = requests.get(url, timeout=timeout)
        except Exception as e:
            logger.error('Caught Exception[%s]' % e)
        captcha_ids_json = response.json()
        logger.debug('Captcha_Ids JSON[%s]' % captcha_ids_json)
        is_downloaded = True
        for captcha_id_object in captcha_ids_json:
            #logger.info(captcha_id_object)
            captcha_text = '%s-0%s' % (captcha_id_object['captcha_id']['tcaptcha_id'], captcha_id_object['applicantNo'])
            if False and (panchayat_name == 'VITHALAPUR' and is_downloaded and (captcha_id != '142000501002010385-01')):
                logger.debug('Skipping[%s]' % captcha_id)
                continue
            is_downloaded = False
            logger.debug('Parse HTML for captcha_text[%s]' % captcha_text)

            filename = '%s/%s_%s_%s_ledger_details.html' % (dirname, block_name, panchayat_name, captcha_text)
            # village_name = captcha_id_object['village']['name']
            # logger.debug('Village Name[%s]' % village_name)
            try:
                data = parse_appi_report(logger, filename=filename, panchayat_name=panchayat_name, captcha_text=captcha_text) # Village Name
            except Exception as e:
                logger.error('Exception when reading transaction table for captcha_id - EXCEPT[%s:%s]' % (type(e), e))
                csv_filename = filename.replace('.html','.XXX')
                open(csv_filename, 'a').close()
                logger.info('Marking the file [%s]' % csv_filename)
                continue

            csv_filename = filename.replace('.html','.csv')
            with open(csv_filename, 'w') as csv_file:
                logger.info('Writing to CSV [%s]' % csv_filename)
                csv_file.write(data.to_csv())
    '''
    tarball_filename = '%s_%s.bz2' % (block_name, pd.Timestamp.now())
    tarball_filename = tarball_filename.replace(' ','-').replace(':','-')
    cmd = 'tar cjf %s %s/*.csv' % (tarball_filename, dirname)
    logger.info('Running cmd[%s]' % cmd)
    os.system(cmd)

    with open(tarball_filename, 'rb') as tarball_file:
        tarball_content = tarball_file.read()
    cloud_filename='media/temp/rn6/%s' % tarball_filename
    session = Session(aws_access_key_id=LIBTECH_AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=LIBTECH_AWS_SECRET_ACCESS_KEY)
    s3 = session.resource('s3',config=Config(signature_version='s3v4'))
    s3.Bucket(AWS_STORAGE_BUCKET_NAME).put_object(ACL='public-read',Key=cloud_filename, Body=tarball_content)
    public_url='https://s3.ap-south-1.amazonaws.com/libtech-nrega1/%s' % cloud_filename
    logger.info('CSV File written on AWS[%s]' % public_url)

    return 'SUCCESS'


class Crawler():
    def __init__(self):
        self.status_file = 'status.csv'
        self.status_file = 'rejected_at_village_10_sample.csv'
        self.dir = 'data/csv'
        self.mandal = 'జి.మాడుగుల'
        self.district = 'విశాఖపట్నం'
        self.logged_in = False
        try:
            os.makedirs(self.dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        self.vars = {}
        self.district_url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/RBDistrictPaymentAbstract'
        self.payment_url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/PaymentvillReport'
        self.display = displayInitialize(isDisabled = is_mac, isVisible = is_visible)
        self.driver = driverInitialize(timeout=3)
        #self.driver = driverInitialize(path='/opt/firefox/', timeout=3)
        self.land_type_dict = [{'name': 'Webland', 'value': '1'},
                               {'name':'ROFR','value':'2'},
                               {'name': 'Tenant', 'value': '3'},
                               {'name': 'UnseededWebland','value':'4'}]
    def __del__(self):
        driverFinalize(self.driver)
        displayFinalize(self.display)

    def wait_for_window(self, timeout = 2):
        time.sleep(round(timeout / 1000))
        wh_now = self.driver.window_handles
        wh_then = self.vars["window_handles"]
        if len(wh_now) > len(wh_then):
            return set(wh_now).difference(set(wh_then)).pop()

    def login(self, logger, auto_captcha=False):
        if self.logged_in:
            return
        url = 'https://ysrrythubharosa.ap.gov.in/RBApp/RB/Login'
        logger.info('Fetching URL[%s]' % url)
        self.driver.get(url)
        time.sleep(3)

        user = '9959905843'
        elem = self.driver.find_element_by_xpath('//input[@type="text"]')
        logger.info('Entering User[%s]' % user)
        elem.send_keys(user)

        password = '9959905843'
        elem = self.driver.find_element_by_xpath('//input[@type="password"]')
        logger.info('Entering Password[%s]' % password)
        elem.send_keys(password)
        if auto_captcha:
            retries = 0
            while True and retries < 3:
                logger.info('Waiting for Captcha...')
                #input()
                WebDriverWait(self.driver, 15).until(EC.element_to_be_clickable((By.ID, "captchdis")))
                time.sleep(3)

                captcha_text = '12345'
                fname = 'captcha.png'
                self.driver.save_screenshot(fname)
                img = Image.open(fname)
                # box = (815, 455, 905, 495)   Captcha Box
                if is_mynk:
                    box = (830, 470, 905, 485)   # Mynk Desktop
                    if is_mac:
                        box = (1170, 940, 1315, 965)   # Mynk Mac
                else:
                    box = (1025, 940, 1170, 965)   # Goli Mac

                area = img.crop(box)
                filename = 'cropped_' + fname
                area.save(filename, 'PNG')
                img = cv2.imread(filename, cv2.IMREAD_GRAYSCALE)
                img = cv2.resize(img, None, fx=10, fy=10, interpolation=cv2.INTER_LINEAR)
                img = cv2.medianBlur(img, 9)
                th, img = cv2.threshold(img, 185, 255, cv2.THRESH_BINARY)
                filename = 'processed_captcha.png'
                cv2.imwrite(filename, img)
                fname = 'converted_captcha.png'
                check_output(['convert', filename, '-resample', '10', fname])
                captcha_text = pytesseract.image_to_string(Image.open(fname), lang='eng', config='--psm 8  --dpi 300 -c tessedit_char_whitelist=ABCDEF0123456789')

                elem = self.driver.find_element_by_xpath('(//input[@type="text"])[2]')
                logger.info('Entering Captcha_Text[%s]' % captcha_text)
                elem.send_keys(captcha_text)

                login_button = '(//button[@type="button"])[2]'
                elem = self.driver.find_element_by_xpath(login_button)
                logger.info('Clicking Login Button')
                elem.click()
                try:
                    WebDriverWait(self.driver, 2).until(EC.element_to_be_clickable((By.XPATH, "//button[@class ='swal2-confirm swal2-styled']"))).click()
                    logger.info(f'Invalid Captcha [{captcha_text}]')
                    retries += 1
                    WebDriverWait(self.driver, 2).until(EC.element_to_be_clickable((By.LINK_TEXT, 'Refresh'))).click()
                    continue
                except TimeoutException:
                    logger.info(f'Valid Captcha [{captcha_text}]')
                    break
                except Exception as e:
                    logger.error(f'Error guessing [{captcha_text}] - EXCEPT[{type(e)}, {e}]')

            if retries == 3:
                return 'FAILURE'
            else:
                self.logged_in = True
                return 'SUCCESS'
        else:
            logger.info('Please ennter the catpcha on the webpage and hit any key...')
            input()
            self.logged_in = True

    def print_current_window_handles(self, logger, event_name=None):
        """Debug function to print all the window handles"""
        handles = self.driver.window_handles
        logger.info(f"Printing current window handles after {event_name}")
        for index, handle in enumerate(handles):
            logger.info(f"{index}-{handle}")

    def run_crawl(self,logger, sample_name=None, village_df=None,
                 district=None, mandal=None, report_type=None,
                 auto_captcha=False):
        self.login(logger, auto_captcha=auto_captcha)
        if report_type == "rb_payment":
            self.download_payment_report(logger, village_df,
                                         report_type=report_type,
                                         sample_name=sample_name)
        elif report_type == 'status':
            retries = 0
            while(True and retries < 10):
                result = sefl.crawlStatusUpdateReport(logger, district, mandal)
                if result == 'SUCCESS':
                    return result
                retries += 1
                logger.warning(f'Attempt {retries} after a FAILURE...')
                time.sleep(3)

            return 'FAILURE'

    def download_payment_report(self, logger, base_df, report_type=None, sample_name=None):
        today_date = datetime.date.today().strftime("%d_%m_%Y")
        if sample_name == "on_demand":
            statusfilepath = f"data/samples/{sample_name}/rayatubarosa/{report_type}/{district_code}/{block_code}/status.csv"
        else:
            statusfilepath = f"data/samples/{sample_name}/rayatubarosa/{report_type}/{today_date}/status.csv"
        upload_s3(logger, statusfilepath, base_df)
        village_df = base_df[base_df['status'] == "pending"]
        logger.info(f"lengt of village df is {len(village_df)}")
        village_df = village_df[village_df['block_code'] != 4851]
        logger.info(f"lengt of village df is {len(village_df)}")
        status_array = []
        logger.info("Downloading payment report")
        logger.info("First lets group districts and mandals")
        logger.info(f"Length of village_df is {len(village_df)}")
        dfgrouped = village_df.groupby(["district_name_telugu",
                            "block_name_telugu"]).size().reset_index(name='counts')
        dfgrouped.to_csv("~/thrash/grouped.csv")
        status_columns = ['district_name_telugu', 'block_name_telugu',
                          'village_name_telugu', 'district_code',
                          'block_code', 'village_code',
                          'data_rows', 'error']
        logger.info(f"Length of rouped_df is {len(dfgrouped)}")
        self.login(logger, auto_captcha=True)
        dfgrouped_len = len(dfgrouped)
        for index, row in dfgrouped.iterrows():
            nindex = dfgrouped_len - index
            district_name = row['district_name_telugu']
            block_name = row['block_name_telugu']
            logger.info(f"Processing group {index} with{district_name}-{block_name}")
            self.driver.get(self.district_url)
            ##Lets get the current window and save it as district_window
            self.vars["window_handles"] = self.driver.window_handles
            self.vars['district_window'] = self.driver.current_window_handle
            self.print_current_window_handles(logger,
                                              event_name="After district page fetch")
            time.sleep(3)
            #Now we will click the district which will open another window for
            self.driver.switch_to.window(self.vars["district_window"])
            #block
            self.vars["window_handles"] = self.driver.window_handles
            WebDriverWait(self.driver,
                          5).until(EC.element_to_be_clickable((By.LINK_TEXT,
                                                               district_name))).click()

            logger.info("after district click")
            self.print_current_window_handles(logger)

            self.vars["mandal_window"] = self.wait_for_window(5)
            self.vars["window_handles"] = self.driver.window_handles
            logger.info("after waiting district click")
            self.print_current_window_handles(logger)
            ##Now we shall click the mandal and it will open another village
            logger.info("Will sleep for 5 seconds")
            time.sleep(3)
            self.driver.switch_to.window(self.vars["mandal_window"])
            logger.info(block_name)
            WebDriverWait(self.driver,
                          5).until(EC.element_to_be_clickable((By.LINK_TEXT,
                                                               block_name))).click()
            logger.info("after block click")
            self.print_current_window_handles(logger)

            self.vars["village_window"] = self.wait_for_window(5)
            self.driver.switch_to.window(self.vars["village_window"])
            self.vars["window_handles"] = self.driver.window_handles
            ### Here I should do all theprocessing of downloading the stats
            ### Also getting all the village codes.
            time.sleep(2)



            mandal_df = village_df[village_df['district_name_telugu'] == district_name]
            vill_df = mandal_df[mandal_df['block_name_telugu'] == block_name]
            #vill_df = vill_df.reset_index()
            vill_df_len = len(vill_df)
            logger.info(f"Lenth of village df is {vill_df_len}")
            for vill_index, vill_row in vill_df.iterrows():
                nvill_index = vill_df_len - vill_index
                village_code = str(vill_row['village_code'])
                district_code = vill_row['district_code']
                block_code = vill_row['block_code']
                village_name = vill_row['village_name']
                district_code = vill_row['village_name']
                block_code = vill_row['village_name']
                land_type = vill_row['land_type']
                land_sel_option = str(vill_row['land_sel_option'])
                logger.info(f"Processing {vill_index} {village_code}-{village_name}")
                error, dataframe = self.crawlPaymentvillReport(logger,
                                                               village_code,
                                                               village_name,
                                                              land_sel_option,
                                                              land_type)
                #error = "error"
                #dataframe = None
                df_rows = 0
                logger.info(f"Error is{vill_index}- {error}")
                if error is None:
                    error = "noError"
                    base_df.loc[vill_index, "status"] = "processed"
                if dataframe is not None:
                    df_rows = len(dataframe)
                    dataframe['village_name_tel']=village_name
                    dataframe['village_code']=village_code
                    dataframe['district_name_tel']=district_name
                    dataframe['mandal_name_tel']=block_name
                    dataframe['district_code']=district_code
                    dataframe['block_code']=block_code
                    dataframe.to_csv("~/thrash/vildata.csv")
                    rb_location = RBLocation(logger, village_code)
                    rb_location.save_report(logger, report_type, dataframe,
                                          sample_name=sample_name,
                                           land_type=land_type)
                    base_df.loc[vill_index, "records"] = df_rows

                status_row = [district_name, block_name, village_name, district_code,
                       block_code, village_code, df_rows, error]
                status_array.append(status_row)
                status_df = pd.DataFrame(status_array, columns=status_columns)
                base_df.loc[vill_index, "error"] = error
                base_df.to_csv("sample_village_df.csv")
                status_df.to_csv("~/thrash/crawlstatus.csv")
                logger.info(f"Finished {vill_index} {village_code}-{village_name}")
                self.print_current_window_handles(logger)
                self.driver.switch_to.window(self.vars["village_window"])
                time.sleep(3)
                #input()

            logger.info("closing village window")
            self.driver.close()#This will close the village window
            self.print_current_window_handles(logger)
            self.driver.switch_to.window(self.vars["mandal_window"])
            logger.info("closing mandal window")
            self.driver.close()#This will close the mandal window
            self.print_current_window_handles(logger)
            self.driver.switch_to.window(self.vars["district_window"])

        status_df = pd.DataFrame(status_array, columns=status_columns)
        status_df.to_csv("~/thrash/crawlstatus.csv")

    def crawlPaymentvillReport(self,logger, village_code, village_name,
                              land_sel_option, land_type):
        error = None
        villageDFs=[]
        villageName=village_name
        value=village_code
        village_extract_dict = {}
        village_extract_dict['pattern'] = "Katha Number"
        #WebDriverWait(self.driver, 10).until(EC.title_contains("వై ఎస్ ఆర్ రైతు భరోసా"))
        #logger.info("Page Title is : "+self.driver.title)
        url = self.payment_url
        logger.info('Fetching URL[%s]' % url)
        self.driver.get(url)
        time.sleep(3)
        dataframe = None
        villageXPath="//select[1]"
        landXpath="//select[2]"
        landXpath="//*/select[@ng-model='landtypemdl']"
        ##### Trying to locate village select Element
        try:
            villageSelect=Select(self.driver.find_element_by_xpath(villageXPath))
        except Exception as e:
            logger.error(f'Exception during villageSelect for {villageXPath} - EXCEPT[{type(e)}, {e}]')
            error = f'Exception during villageSelect for {villageXPath} - EXCEPT[{type(e)}, {e}]'
            return error, dataframe

        ### Using village select element to select a village
        try:
            villageSelect.select_by_value(value)
            time.sleep(3)
        except Exception as e:
            logger.error(f'Exception during select of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]')
            error=f'Exception during select of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]'
            return error, dataframe

        ### Locating land select
        landSelect=Select(self.driver.find_element_by_xpath(landXpath))
        #Now we will select the landtype and click on input button
        landValue=land_sel_option
        landType=land_type
        land_type = landType
        try:
            landSelect.select_by_value(landValue)
            self.driver.find_element_by_xpath('//input[@value="submit"]').click()
            logger.info(f"Submit clicked for vilageName[{villageName}], {slugify(villageName)}] landType[{landType}]")
        except Exception as e:
            logger.error(f'Exception during select for landType[{landType}] of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]')
            error = f'Exception during select landType[{landType}] of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]'
            return error, dataframe
        ### Trying to catch an alert for no data
        timeout=20
        try:
            logger.debug(f'Timeout value is {timeout}')
            WebDriverWait(self.driver, timeout).until(EC.element_to_be_clickable((By.XPATH, "//button[@class='swal2-confirm swal2-styled']"))).click()
            logger.info(f'Skipping for landType[{landType}] of Village[{villageName}]')
            error = None
            dataframe = None
            return error, dataframe
        except Exception as e:
            logger.info(f'Moving Ahead for landType[{landType}] of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]')
        #This loop will actually fetch the data
        while True:
            try:
                WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//input[@class='btn btn-primary']")))
                logger.info(f'Found Data')
                myhtml = self.driver.page_source
            except Exception as e:
                logger.error(f'When reading HTML source landType[{landType}] of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]')
                error=f'unable to read html landType[{landType}] of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]'
                #statusDF.loc[curIndex,'inProgress'] = 0
                #statusDF.to_csv(self.status_file)
                logger.warning(f'Skipping at HTML read level Village[{villageName}]')
                myhtml = None
                return error, dataframe
            if myhtml is not None:
                df = get_dataframe_from_html(logger, myhtml,
                                              mydict=village_extract_dict)
                df['land_type']=landType
                villageDFs.append(df)
                logger.info(f'Adding the table for village[{villageName}] and type[{landType}]')

            try:
                elem = WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable((By.LINK_TEXT, '›')))
                parent = elem.find_element_by_xpath('..')
                logger.info(f'parent[{parent.get_attribute("class")}] elem[{elem.get_attribute("class")}]')
                if 'disabled' in parent.get_attribute("class"):
                    logger.info(f'Disabled so end here!')
                    break
                else:
                    elem.click()
                    time.sleep(5)
                    continue
            except Exception as e:
                logger.info(f'No pagination here!')
                break

        if len(villageDFs) > 0:
            villageDF=pd.concat(villageDFs)
        else:
            villageDF=None

        return error, villageDF


    def download_payment_report1(self, logger, village_df, report_type=None, sample_name=None):
        """This function will download the payment report from the villages"""
        village_stat_df_array = []
        village_name_df_array = []
        extract_dict = {}
        extract_dict['pattern'] = "Village Name"
        village_extract_dict = {}
        village_extract_dict['pattern'] = "Katha Number"
        ##CSV to be built for village names

        district_list = village_df['district_name_telugu'].unique().tolist()
        logger.info(district_list)
        #district_list = [self.district]
         ##First we will Login in to the ryatu barosa website
        #self.login_portal(logger)
        ##Next we will fetch the distrit page and load it in one of the windoes
        logger.info('Fetching URL[%s]' % self.district_url)
        self.driver.get(self.district_url)
        ##Lets get the current window and save it as district_window
        self.vars["window_handles"] = self.driver.window_handles
        self.vars['district_window'] = self.driver.current_window_handle
        self.print_current_window_handles(logger,
                                          event_name="After district page fetch")
        #district_list = [self.district]
        logger.info(f"District list is {district_list}")
        time.sleep(5)
        for dist_no, district_name in enumerate(district_list):
            self.driver.switch_to.window(self.vars["district_window"])
            logger.info(f"Currently processling {dist_no}-{district_name}")
            mandal_df = village_df[village_df['district_name_telugu'] == district_name]
            mandal_list = mandal_df["block_name_telugu"].unique().tolist()
            logger.info(f"Mandal list for {district_name} is {mandal_list}")
            self.vars["window_handles"] = self.driver.window_handles
            WebDriverWait(self.driver,
                          5).until(EC.element_to_be_clickable((By.LINK_TEXT,
                                                               district_name))).click()

            logger.info("after district click")
            self.print_current_window_handles(logger)

            self.vars["mandal_window"] = self.wait_for_window(5)
            for mandal_no, mandal_name in enumerate(mandal_list):
                csv_array = []
                logger.info(f"processing {dist_no}-{district_name}-{mandal_no}-{mandal_name}")
                logger.info(self.vars)
                self.driver.switch_to.window(self.vars["mandal_window"])
                self.vars["window_handles"] = self.driver.window_handles
                logger.info("after waiting district click")
                self.print_current_window_handles(logger)

                logger.info("Will sleep for 5 seconds")
                time.sleep(5)
                WebDriverWait(self.driver,
                              5).until(EC.element_to_be_clickable((By.LINK_TEXT,
                                                                   mandal_name))).click()
                logger.info("after block click")
                self.print_current_window_handles(logger)

                self.vars["village_window"] = self.wait_for_window(5)
                self.driver.switch_to.window(self.vars["village_window"])
                self.vars["window_handles"] = self.driver.window_handles
                ### Here I should do all theprocessing of downloading the stats
                ### Also getting all the village codes.
                time.sleep(2)
                myhtml = self.driver.page_source
                village_stat_df = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
                village_stat_df = village_stat_df.merge(village_df,
                                                        how='left',
                                                        left_on='Village Name',
                                                        right_on='village_name'
                                                       )
                village_stat_df_array.append(village_stat_df)
                ### Now lets go to payment page
                vill_df = mandal_df[mandal_df['block_name_telugu'] == mandal_name]
                self.crawlPaymentvillReport(logger, district_name=district_name,
                                       mandal_name=mandal_name,
                                       village_df=vill_df,
                                        sample_name=sample_name)



                logger.info(f"Now I am going to close the village Window")
                self.print_current_window_handles(logger,
                                                  event_name="beforevllageclose")
                self.driver.close()#This will close the village window
                self.print_current_window_handles(logger,
                                                  event_name="aftervllageclose")
            self.driver.switch_to.window(self.vars["mandal_window"])
            self.print_current_window_handles(logger,
                                              event_name="beforemandalclose")
            self.driver.close()#This will close the Mandal Window
            self.print_current_window_handles(logger,
                                              event_name="aftermandalclose")
            self.print_current_window_handles(logger)
        dataframe = pd.concat(village_stat_df_array, ignore_index=True)
        dataframe.to_csv("village_stat.csv")


    def crawlPaymentvillReport1(self,logger, district_name=None,
                               mandal_name=None, village_df=None,
                               sample_name=None):
        village_extract_dict = {}
        village_extract_dict['pattern'] = "Katha Number"
        url = self.payment_url
        logger.info('Fetching URL[%s]' % url)
        self.driver.get(url)
        time.sleep(3)
        villageXPath="//select[1]"
        try:
            villageSelect=Select(self.driver.find_element_by_xpath(villageXPath))
        except Exception as e:
            logger.error(f'Exception during villageSelect for {villageXPath} - EXCEPT[{type(e)}, {e}]')
            return 'FAILURE'
        landXpath="//select[2]"
        landXpath="//*/select[@ng-model='landtypemdl']"
        landSelect=Select(self.driver.find_element_by_xpath(landXpath))
        landList=[]
        for o in landSelect.options[1:]:
            p={}
            p['name']=o.text
            p['value']=o.get_attribute('value')
            landList.append(p)
        #buttonXPath="//button[1]"
        logger.info(landList)

        #statusDF=pd.read_csv(self.status_file,index_col=0)
        #logger.info(statusDF)
        #filteredDF=statusDF[ (statusDF['status'] == 'pending') & (statusDF['inProgress'] == 0)]
        #if len(filteredDF) > 0:
        #    curIndex=filteredDF.index[0]
        #else:
        #    curIndex=None
        #    logger.info('No more requests to process')
        #    return 'SUCCESS'
        #statusDF.loc[curIndex,'inProgress'] = 1
        #statusDF.to_csv(self.status_file)
        #while curIndex is not None:
        #    row=filteredDF.loc[curIndex]
        for index, row in village_df.iterrows():
            village_code = str(row['village_code'])
            district_code = row['district_code']
            block_code = row['block_code']
            village_name = row['village_name']
            villageName=village_name
            value=village_code
            land_type = ''
            try:
                villageSelect.select_by_value(value)
                #time.sleep(3)
            except Exception as e:
                logger.error(f'Exception during select of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]')
               # statusDF.loc[curIndex,'inProgress'] = 0
                #statusDF.to_csv(self.status_file)
                #logger.warning(f'Skipping Village[{villageName}]')
                #return 'FAILURE'
            villageDFs=[]
            for p1 in landList:
                landValue=p1.get("value")
                landType=p1.get("name")
                land_type = landType
                logger.info(f"villageName[{villageName}, {slugify(villageName)}] landType[{landType}]")
                try:
                    landSelect.select_by_value(landValue)
                    self.driver.find_element_by_xpath('//input[@value="submit"]').click()
                    logger.info(f"Submit clicked for vilageName[{villageName}], {slugify(villageName)}] landType[{landType}]")
                except Exception as e:
                    logger.error(f'Exception during select for landType[{landType}] of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]')
                   # statusDF.loc[curIndex,'inProgress'] = 0
                   # statusDF.to_csv(self.status_file)
                    logger.warning(f'Skipping at landSelect level Village[{villageName}]')
                    return 'FAILURE'

                try:
                    if landValue == '4':
                        timeout = 25
                    else:
                        timeout = 2
                    logger.debug(f'Timeout value is {timeout}')
                    WebDriverWait(self.driver, timeout).until(EC.element_to_be_clickable((By.XPATH, "//button[@class='swal2-confirm swal2-styled']"))).click()
                    #statusDF.loc[curIndex, landType] = 'failed'
                    #statusDF.to_csv(self.status_file)
                    logger.info(f'Skipping for landType[{landType}] of Village[{villageName}]')
                    continue
                except Exception as e:
                    logger.info(f'Moving Ahead for landType[{landType}] of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]')

                while True:
                    try:
                        #WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.LINK_TEXT, 'Name Of Beneficiary')))
                        WebDriverWait(self.driver, timeout).until(EC.element_to_be_clickable((By.XPATH, "//input[@class='btn btn-primary']")))
                        logger.info(f'Found Data')
                        myhtml = self.driver.page_source
                    except Exception as e:
                        logger.error(f'When reading HTML source landType[{landType}] of Village[{villageName}, {slugify(villageName)}] - EXCEPT[{type(e)}, {e}]')
                        #statusDF.loc[curIndex,'inProgress'] = 0
                        #statusDF.to_csv(self.status_file)
                        logger.warning(f'Skipping at HTML read level Village[{villageName}]')
                        #continue
                        return 'FAILURE'

                    #dfs=pd.read_html(myhtml)
                    #df=dfs[0]
                    df = get_dataframe_from_html(logger, myhtml,
                                                  mydict=village_extract_dict)
                    #logger.info('Before')
                    #logger.info(f'{df}')
                    df['village_name_tel']=villageName
                    df['village_code']=value
                    df['district_name_tel']=district_name
                    df['mandal_name_tel']=mandal_name
                    df['land_type']=landType
                    villageDFs.append(df)
                    #statusDF.loc[curIndex, landType] = 'done'
                    #statusDF.to_csv(self.status_file)
                    logger.info(f'Adding the table for village[{villageName}] and type[{landType}]')
                    #logger.info(f'{df}')

                    try:
                        elem = WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable((By.LINK_TEXT, '›')))
                        parent = elem.find_element_by_xpath('..')
                        logger.info(f'parent[{parent.get_attribute("class")}] elem[{elem.get_attribute("class")}]')
                        if 'disabled' in parent.get_attribute("class"):
                            logger.info(f'Disabled so end here!')
                            break
                        else:
                            elem.click()
                            time.sleep(5)
                            continue
                    except Exception as e:
                        logger.info(f'No pagination here!')
                        break

            if len(villageDFs) > 0:
                villageDF=pd.concat(villageDFs)
                report_type = "rb_payment"
                rb_location = RBLocation(logger, village_code)
                rb_location.save_report(logger, report_type, villageDF,
                                       sample_name=sample_name)
            else:
                colHeaders = ['S No', 'Name Of Beneficiary', 'Father Name', 'PSS Name', 'Katha Number', 'Aadhaar', 'Bank Name', 'Bank Account Number(Last 4 Digits)', 'Status,Remarks', 'village_name_tel', 'village_code', 'district_name_tel', 'mandal_name_tel', 'land_type']
                villageDF=pd.DataFrame(columns = colHeaders)

            #csvFileName=f"{self.dir}/{district}_{mandal}_{villageName}.csv"
            #logger.info('Writing to [%s]' % csvFileName)
            #villageDF.to_csv(csvFileName, index=False)
            #statusDF.loc[curIndex,'status'] = 'done'
            #statusDF.loc[curIndex,'inProgress'] = 0
            #logger.info(f'Updating [{self.status_file}]')
            #statusDF.to_csv(self.status_file)

            #statusDF=pd.read_csv(self.status_file, index_col=0)
            #filteredDF=statusDF[ (statusDF['status'] == 'pending') & (statusDF['inProgress'] == 0)]
            #if len(filteredDF) > 0:
            #    curIndex=filteredDF.index[0]
            #    statusDF.loc[curIndex,'inProgress'] = 1
            #    statusDF.to_csv(self.status_file)
            #else:
            #    curIndex=None

        return 'SUCCESS'

    def crawl_status_update_report(self, logger, district=None, mandal=None):
        self.login(logger, auto_captcha=True)

        statusDF=pd.read_csv(self.status_file,index_col=0)
        #logger.info(statusDF)
        filteredDF=statusDF[ (statusDF['status'] == 'pending') & (statusDF['inProgress'] == 0)]
        if len(filteredDF) > 0:
            curIndex=filteredDF.index[0]
        else:
            curIndex=None
            logger.info('No more requests to process')
            return 'SUCCESS'
        statusDF.loc[curIndex,'inProgress'] = 1
        statusDF.to_csv(self.status_file)
        prev_village = ''
        villageDFs=[]

        while curIndex is not None:
            row = filteredDF.loc[curIndex]
            district = row['districtName']
            mandal = row['mandalName']
            villagename = row['villageName']
            villageName=villagename
            villageCode = str(row['villageCode'])
            district_code = str(row['district_code'])
            mandal_code = str(row['block_code'])
            kathaNo = str(int(row['kathaNo']))

            self.set_district(district_name=district,district_code=district_code)  # could give both name and code depending on input
            self.set_mandal(mandal_name=mandal,mandal_code=mandal_code)
            time.sleep(5)
            url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/Statusupdate'
            logger.info('Fetching URL[%s]' % url)
            self.driver.get(url)
            time.sleep(3)
            logger.info(villageCode)

            villageXPath="//select[1]"
            try:
                villageSelect=Select(self.driver.find_element_by_xpath(villageXPath))
            except Exception as e:
                logger.error(f'Exception during villageSelect for {villageXPath} - EXCEPT[{type(e)}, {e}]')
                return 'FAILURE'


            filename=f"{self.dir}/{district}_{mandal}_{villageName}_{kathaNo}.csv"
            if os.path.exists(filename):
                logger.info('File already downloaded. Reading [%s]...' % filename)
                df = pd.read_csv(filename)
            else:
                try:
                    if villageCode != prev_village:
                        villageSelect.select_by_value(villageCode)

                    elem = self.driver.find_element_by_xpath('//input[@type="text"]')
                    logger.info(f'Entering kathaNo[{kathaNo}]')
                    elem.clear()
                    elem.send_keys(kathaNo)
                    #time.sleep(1)

                    self.driver.find_element_by_xpath('//input[@value="submit"]').click()
                    logger.info(f'Submit clicked for vilageName[{villageName}], {slugify(villageName)}] kathaNo[{kathaNo}]')

                    time.sleep(1)
                    #WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.LINK_TEXT, kathaNo)))
                    myhtml = self.driver.page_source
                except Exception as e:
                    logger.error(f'Exception during select of Village[{villageName}, {slugify(villageName)}]  kathaNo[{kathaNo}] - EXCEPT[{type(e)}, {e}]')
                    statusDF.loc[curIndex,'status'] = 'failed'
                    statusDF.loc[curIndex,'inProgress'] = 0
                    statusDF.to_csv(self.status_file)
                    #logger.warning(f'Skipping Village[{villageName}]')
                    #exit(0)
                    #continue
                    return 'FAILURE'

                dfs=pd.read_html(myhtml)
                df=dfs[0]
                df['village_name_tel']=villageName
                df['village_code']=villageCode
                df['district_name_tel']=district
                df['mandal_name_tel']=mandal
                df['katha_no']=kathaNo
                logger.info('Writing to [%s]' % filename)
                df.to_csv(filename, index=False)

            logger.info(f'{df}')
            villageDFs.append(df)
            statusDF.loc[curIndex, 'status'] = 'done'
            statusDF.loc[curIndex,'inProgress'] = 0
            statusDF.to_csv(self.status_file)
            logger.info(f'Adding the table for village[{villageName}] and kathaNo[{kathaNo}]')

            statusDF=pd.read_csv(self.status_file, index_col=0)
            filteredDF=statusDF[ (statusDF['status'] == 'pending') & (statusDF['inProgress'] == 0)]
            if len(filteredDF) > 0:
                curIndex=filteredDF.index[0]
                statusDF.loc[curIndex,'inProgress'] = 1
                statusDF.to_csv(self.status_file)
            else:
                curIndex=None
            prev_village = villageCode

        if len(villageDFs) > 0:
            villageDF=pd.concat(villageDFs)
        else: # FIXME
            colHeaders = ['S No', 'Name Of Beneficiary', 'Father Name', 'PSS Name', 'Katha Number', 'Aadhaar', 'Bank Name', 'Bank Account Number(Last 4 Digits)', 'Status,Remarks', 'village_name_tel', 'village_code', 'district_name_tel', 'mandal_name_tel', 'land_type']
            villageDF=pd.DataFrame(columns = colHeaders)

        filename=f"{self.dir}/{district}.csv"
        logger.info('Writing to [%s]' % filename)
        villageDF.to_csv(filename, index=False)

        return 'SUCCESS'

    def set_district(self, district_name=None, district_code=None):
        if not district_name:
            district_name = 'విశాఖపట్నం'

        # district_code = lookup[district_name]  # Goli has the lookup?
        if not district_code:
            district_code = '520' # Vishakapatnam is hard coded

        self.driver.execute_script(f"sessionStorage.setItem('district', '{district_code}'); sessionStorage.setItem('districtname', '{district_name}');");

    def set_mandal(self, mandal_name=None, mandal_code=None):
        if not mandal_name:
            mandal_name = 'జి.మాడుగుల'  # put lookup here in case only code was given

        if not mandal_code:
            mandal_code = '4848'        # put lookup here in case only name was given

        self.driver.execute_script(f"sessionStorage.setItem('mandal', '{mandal_code}'); sessionStorage.setItem('mandalname', '{mandal_name}');");

    def crawl_death_abstract_report(self, logger, district=None, mandal=None, village=None):
        self.login(logger, auto_captcha=True)
        self.set_district(district)  # could give both name and code depending on input
        self.set_mandal(mandal)

        url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/RBDeathAbstractvillage'
        logger.info('Fetching URL[%s]' % url)
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, 25).until(EC.element_to_be_clickable((By.XPATH, "//button[@class='btn btn-primary']")))
            logger.info(f'Fetching Death Abstract Report for [{mandal}]...')
        except TimeoutException:
            logger.error(f'Timed out waiting for Death Abstract for [{mandal}]')
            #break
            #return 'FAILURE'
        except Exception as e:
            logger.error(f'Errored Waiting for Death Abstract for [{mandal}] - EXCEPT[{type(e)}, {e}]')
            #break
            #return 'FAILURE'

        filename=f'{self.dir}/{district}_{mandal}.csv'
        html_source = self.driver.page_source
        df = pd.read_html(html_source)[0]
        logger.debug(f'{df}')
        logger.info(f'Writing [{filename}]')
        df.to_csv(filename, index=False)

        villages = df['Village Name']

        table_id = 'tblreject'
        logger.info('Waiting for the table ID[{table_id}] to load')
        table = WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((By.ID, table_id))
        )

        if village:
            logger.info(f'The village list [{villages}]')
            specific_index = village.find(village)
            row = specific_index+1
            logger.info(f'Yippie! We have a hit for {village} {specific_index} [{row}]')
            df = self.fetch_death_abstract_report(logger, district, mandal, village, row)
            return df
        
        for row, village in enumerate(villages, 1):
            self.fetch_death_abstract_report(logger, district, mandal, village, row)

        return None

    def fetch_death_abstract_report(self, logger, district, mandal, village, row):
        village_path = f'/html/body/div[3]/div[3]/div/div[2]/div/div/table/tbody/tr[{row}]/td[2]'
        elem = self.driver.find_element_by_xpath(village_path)        
        village_value = elem.get_attribute('text') # FIXME why is this not working. Returns None always
        
        link_path = f'/html/body/div[3]/div[3]/div/div[2]/div/div/table/tbody/tr[{row}]/td[4]/a'
        elem = self.driver.find_element_by_xpath(link_path)        
        value = elem.get_attribute('text')

        logger.info("Handles : [%s]    Number : [%d]" % (self.driver.window_handles, len(self.driver.window_handles)))
        logger.info(f'Clicking for village[{village}] vs village_value[{village_value}] > value[{value}]')
        elem.click()
        time.sleep(5) #FIXME
        '''
        path = f'/html/body/div[3]/div[2]/h6/b[contains(text(),"{village}")]'
        logger.info(f'Waiting for page with village[{village}] to load on "{path}"')
        WebDriverWait(self.driver, 25).until(
            EC.presence_of_element_located((By.XPATH,  path))
        )
        '''
        parent_handle = self.driver.current_window_handle
        logger.info("Handles : [%s]    Number : [%d]" % (self.driver.window_handles, len(self.driver.window_handles)))
        
        if len(self.driver.window_handles) > 1:
            logger.info('Switching Window...')
            self.driver.switch_to.window(self.driver.window_handles[-1])
            logger.info('Switched!!!')
            html_source = self.driver.page_source
            #time.sleep(2)
        else:
            logger.error(f'Handlers gone wrong [{str(self.driver.window_handles)}]')
            self.driver.save_screenshot('./button_'+captcha_text+'.png')
            return 'FAILURE'
        df = pd.read_html(html_source)[0]
        logger.debug(f'{df}')
        filename=f'{self.dir}/{district}_{mandal}_{village}.csv'
        logger.info(f'Writing [{filename}]')
        df.to_csv(filename, index=False)
        logger.info('Closing Current Window')
        self.driver.close()
        logger.info('Switching back to Parent Window')
        self.driver.switch_to.window(parent_handle)

        return df

def get_unique_district_block(logger, dataframe):
    logger.info(dataframe.columns)
    df = dataframe.groupby(["district_name_telugu",
                            "block_name_telugu"]).size().reset_index(name='counts')
    logger.info(len(df))

class TestSuite(unittest.TestCase):
    def setUp(self):
        self.logger = logger_fetch('info')
        self.logger.info('BEGIN PROCESSING...')

    def tearDown(self):
        self.logger.info('...END PROCESSING')

    @unittest.skip('Skipping direct command approach')
    def test_fetch_gram_1b_report(self):
        count = 0
        url = base_url + 'ROR.aspx'

        while True:
            count += 1
            self.logger.info('Beginning the download for the nth time, where n=%d' % count)
            result = fetch_appi_reports(self.logger, dirname=directory, url=url)
            if result == 'SUCCESS' or count == 100:
                break
        self.assertEqual(result, 'SUCCESS')

    @unittest.skip('Skipping direct command approach')
    def test_fetch_gram_1b_report_for(self):
        url = base_url + 'ROR.aspx'
        result = fetch_reports_for(self.logger, dirname=directory, url=url, villages=village_list)
        self.assertEqual(result, 'SUCCESS')

    @unittest.skip('Skipping direct command approach')
    def test_dump_gram_1b_report(self):
        result = dump_gram_1b_reports(self.logger, dirname=directory)
        self.assertEqual(result, 'SUCCESS')

    def test_crawler(self):
        self.logger.info("Running Crawler Tests")
        # Start a RhythuBharosa Crawl
        report_type = "rb_payment"
        if report_type == "rb_payment":
           location_code = '4848'
           location_code = None
           sample_name = 'vizag_itda_10_sample'
           ###This is how your same report
           #rb_location = RBLocation(logger, village_code)
           #rb_location.save_report(logger, report_type, village_df)
           rb_crawler = RBCrawler(self.logger)
           if location_code is not None:
               sample_name = "on_demand"
               village_df = rb_crawler.get_crawl_df(self.logger, block_code=location_code)
           elif sample_name is not None:
              #village_df = rb_crawler.get_crawl_df(self.logger,
              #                                     tag_name=sample_name,
              #                                     enum_land_type=True
              #                                    )
              #village_df.to_csv("sample_village_df.csv")
              #exit(0)
               village_df = pd.read_csv("sample_village_df.csv", index_col=0)
               self.logger.info(f"lengt of village df is {len(village_df)}")
           else:
               logger.info("Either of location Code or Sample Name input is required")
               logger.info("Exiting!!!")
               exit(0)
           rb = Crawler()
           rb.download_payment_report(self.logger, village_df,
                                      report_type=report_type,
                                      sample_name=sample_name)
           del rb
        else:
           rb = Crawler()
           rb.run_crawl(self.logger, report_type='status',
                       auto_captcha=False)
           del rb

    def test_crawl_status_update_report(self):
        self.logger.info("Running test for Status Update Report")
        # Start a RhythuBharosa Crawl
        rb = Crawler()
        rb.crawl_status_update_report(self.logger, district='విశాఖపట్నం', mandal='జి.మాడుగుల')
        del rb

    def test_crawl_death_abstract_report(self):
        self.logger.info("Running test for Death Abstract Report")
        # Start a RhythuBharosa Crawl
        rb = Crawler()
        rb.crawl_death_abstract_report(self.logger, district='విశాఖపట్నం', mandal='జి.మాడుగుల')
        del rb

    def test_fetch_death_report(self):
        self.logger.info("Running test for Death Abstract Report")
        # Start a RhythuBharosa Crawl
        rb = Crawler()
        rb.crawl_death_abstract_report(self.logger, district='విశాఖపట్నం', mandal='జి.మాడుగుల', village='దేవరాపల్లి')
        rb.crawl_death_abstract_report(self.logger, district='విశాఖపట్నం', mandal='జి.మాడుగుల', village='క్రిష్ణాపురం')
        rb.crawl_death_abstract_report(self.logger, district='విశాఖపట్నం', mandal='జి.మాడుగుల', village='కె.బందవీధి')
        del rb

if __name__ == '__main__':
    unittest.main()
