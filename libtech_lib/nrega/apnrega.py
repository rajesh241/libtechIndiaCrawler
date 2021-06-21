"""This has all the functions for crawling NIC NREGA Website"""
#pylint: disable-msg = too-many-locals
#pylint: disable-msg = too-many-branches
#pylint: disable-msg = too-many-statements

import re
import urllib.parse as urlparse
from urllib.parse import parse_qs
import json
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from datetime import datetime, timedelta

NREGA_DATA_DIR = os.environ.get('NREGA_DATA_DIR', None)
print(NREGA_DATA_DIR)
LOCAL_DOWNLOAD = True

from libtech_lib.generic.commons import  (get_current_finyear,
                      get_full_finyear,
                      standardize_dates_in_dataframe,
                      get_default_start_fin_year,
                      insert_location_details,
                      get_finyear_from_muster_url,
                      insert_finyear_in_dataframe,
                      get_fto_finyear
                     )
from libtech_lib.generic.api_interface import api_get_report_url, api_get_report_dataframe
from libtech_lib.generic.html_functions import (get_dataframe_from_html,
                                                get_dataframe_from_url,
                                                request_with_retry_timeout,
                                                get_request_with_retry_timeout
                                                )
from libtech_lib.generic.libtech_queue import libtech_queue_manager

def get_ap_muster_transactions(lobj, logger):
    """Download Musters from AP/Telangana website"""
    dataframe_array  = []
    finyear = '18'
    start_fin_year = get_default_start_fin_year()
    end_fin_year = get_current_finyear()
    state_code='02'
    district_code=lobj.district_code[2:]
    block_code=lobj.block_code[5:]
    panchayat_code=lobj.panchayat_code[8:]
    logger.info(district_code+block_code+panchayat_code)
    url = 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=NewReportsRH&actionVal=R1Display&page=Newreportcenter_ajax_eng#'
    url='http://www.nrega.telangana.gov.in/Nregs/FrontServlet?requestType=Common_engRH&actionVal=musterinfo&page=MusterRolls_eng'
    url2='http://www.nrega.telangana.gov.in/Nregs/FrontServlet?'
    if lobj.state_code == "36":
        state_code = "02"
        base_url = 'http://www.nrega.telangana.gov.in/Nregs/FrontServlet?requestType=Common_engRH&actionVal=musterinfo&page=MusterRolls_eng'
        url2='http://www.nrega.telangana.gov.in/Nregs/FrontServlet?'
    else:
        state_code = "01"
        base_url = "http://www.mgnregs.ap.gov.in/Nregs/"
        url2 = 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=Common_engRH&actionVal=musterinfo&page=MusterRolls_eng'
    logger.info('Fetching URL[%s] for cookies' % url)
    for finyear in range(int(start_fin_year), int(end_fin_year) + 1):
        startYear=int(finyear)-1+2000
        endYear=int(finyear)+2000
        startDate="01/04/%s" % str(startYear)
        endDate="31/03/%s" % str(endYear)
        logger.info(startDate+endDate)
        with requests.Session() as session:
            response = session.get(url)

            cookies = session.cookies

            logger.info(cookies)
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Content-Type': 'application/x-www-form-urlencoded',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }

            params = (
                ('requestType', 'PaymentsWork_engRH'),
                ('actionVal', 'musterrolls'),
                ('page', 'SocialAuditPrint_eng'),
                ('District', district_code),
                ('Mandal', block_code),
                ('Panchayat', panchayat_code),
                ('FromDate', startDate),
                ('ToDate', endDate),
                ('exec', 'muster'),
            )

            data = {
                'State': state_code,
                'District': district_code,
                'Mandal': block_code,
                'Panchayat': panchayat_code,
                'FromDate': startDate,
                'ToDate': endDate,
                'Go': '',
                'spl': 'Select',
                'input2': '',
                'userCaptcha': ''
            }

            response = session.post(url2,
                                    headers=headers, params=params, cookies=cookies, data=data)
            if response.status_code == 200:
              dataframe = pd.read_excel(response.content,header=7)
              logger.info(dataframe.head())
              to_delete_rows = []
              for index, row in dataframe.iterrows():
                  tjobcard = ''
                  jobcard_worker = str(row['Jobcard ID'])
                  surname = row['SurName']
                  jobcard_worker_array = jobcard_worker.split("-")
                  if len(jobcard_worker_array) == 2:
                      tjobcard = jobcard_worker_array[0]
                  if (len(tjobcard)==18) and (surname != "Total"):
                      message="keep this row"
                  else:
                      to_delete_rows.append(index)
                  dataframe.loc[index, "tjobcard" ] = tjobcard
                  dataframe.loc[index, "tjobcard_str" ] = "~"+str(tjobcard)
                  dataframe.loc[index, "finyear" ] = finyear
              date_dict = {
                  'From Date' : '%d-%b-%Y',
                  'To Date' : '%d-%b-%Y',
                  'PayOrder Gen Date' : '%d-%b-%Y'
              }
              dataframe = dataframe.drop(to_delete_rows)
              dataframe = standardize_dates_in_dataframe(logger, dataframe,
                                                         date_dict)
              dataframe = insert_location_details(logger, lobj, dataframe)
              dataframe = dataframe.reset_index(drop=True)
              dataframe_array.append(dataframe)
    dataframe = pd.concat(dataframe_array, ignore_index=True)
    dataframe = dataframe.reset_index(drop=True)
    return dataframe

def fetch_ap_jobcard_register_for_village(logger, district_code, block_code, panchayat_code, village_code, village_name):

    response = requests.get('https://mgnregs.ap.gov.in/Nregs')
    cookies = response.cookies
    logger.info(district_code+block_code+panchayat_code+village_code)


    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        'sec-ch-ua-mobile': '?0',
        'Upgrade-Insecure-Requests': '1',
        'Origin': 'https://mgnregs.ap.gov.in',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Referer': 'https://mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=WageSeekersRH&actionVal=JobCardHolder&param=JCHI&type=-1&Atype=Display&Ajaxid=Village',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    params = (
        ('requestType', 'WageSeekersRH'),
        ('actionVal', 'JobCardHolder'),
        ('param', 'JCHI'),
        ('type', '-1'),
        ('Ajaxid', 'go'),
    )

    data = {
    'State': '-1',
    'District': district_code,
    'Mandal': block_code,
    'Panchayat': panchayat_code,
    'Village': village_code,
    'HouseHoldId': '',
    'Go': ''
    }

    response = requests.post('https://mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, data=data)

    dataframe = pd.read_html(response.content)[-1]
    if dataframe.iloc[0,0] != 'No records found for the selection made':
        dataframe.columns = ['sno','jobcard_num','nic_jobcard','head_of_the_family','registration_date','no_of_disabled','no_of_shg_members','no_of_males','no_of_females']
        dataframe['village_code'] = village_code
        dataframe['village_name'] = village_name
        dataframe['jobcard_num'] = '~0' + dataframe['jobcard_num'].astype(str)
        logger.info(f'shape of df is {dataframe.shape}')

    return dataframe

def fetch_hh_employment(logger,district_code,block_code,panchayat_code,village_code,finyear,cookies):
    
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'Origin': 'http://www.nrega.ap.gov.in',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': 'http://www.nrega.ap.gov.in/Nregs/FrontServlet?requestType=WageSeekersRH&actionVal=HHEDetails&type=-1&param=HHE&Atype=Display&Ajaxid=Financial',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }

    params = (
        ('requestType', 'WageSeekersRH'),
        ('actionVal', 'HHEDetails'),
        ('type', '-1'),
        ('param', 'HHE'),
        ('Ajaxid', 'go'),
        ('year', finyear),
    )

    data = {
      'State': '01',
      'District': district_code,
      'Mandal': block_code,
      'Panchayat': panchayat_code,
      'Village': village_code,
      'Financial': finyear,
      'Go': ''
    }

    response = request_with_retry_timeout(logger,'https://mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, data=data)

    dataframe = pd.read_html(response.content)[-1]

    dataframe.columns = ['sno','jobcard_no','nic_jobcard_no','head_of_family','no_of_works_worked','no_of_days_worked','total_wage','avg_wage_per_day','no_of_labour_worked_hh']
    dataframe['village_code'] = village_code
    dataframe['finyear'] = finyear
    return dataframe

def add_leading_zeros_village(string):
    if len(str(string)) == 2:
        string = '0' + str(string)
    if len(str(string)) == 1:
        string = '00' + str(string)
    return string

def add_leading_zeros_panchayat(string):
    if len(str(string)) != 2:
        string = '0' + str(string)
    return string

def add_leading_zeros_mandal(string):
    if len(str(string)) != 2:
        string = '0' + str(string)
    return string

def add_leading_zeros_district(string):
    if len(str(string)) != 2:
        string = '0' + str(string)
    return string


def get_ap_village_codes(lobj, logger):
    response = requests.get('https://mgnregs.ap.gov.in/Nregs/')
    cookies = response.cookies

    cookie = str(cookies).split('=')[-1].split(' ')[0]
    district_code=lobj.district_code[2:]
    block_code=lobj.block_code[5:]
    panchayat_code=lobj.panchayat_code[8:]
    logger.info(district_code+block_code+panchayat_code)

    headers = {
        'authority': 'mgnregs.ap.gov.in',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
        'accept': '*/*',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=NewReportsRH&actionVal=R1Display&page=Newreportcenter_ajax_eng',
        'accept-language': 'en-US,en;q=0.9,te;q=0.8',
        'cookie': f'JSESSIONID={cookie}',
    }

  
    params = (
        ('requestType', 'WageSeekersRH'),
        ('actionVal', 'JobCardHolder'),
        ('param', 'JCHI'),
        ('type', '-1'),
        ('Atype', 'Display'),
        ('Ajaxid', 'Panchayat'),
    )

    data = {
      'State': '-1',
      'District': district_code,
      'Mandal': block_code,
      'Panchayat': panchayat_code,
      'Village': '-1',
      'HouseHoldId': ''
    }

    response3 = requests.get('https://mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params,data=data)

    soup3 = BeautifulSoup(response3.content)
    village_tags = soup3.find('select',{'id':'Village'})
    village_tags = [i for i in village_tags if 'ALL' not in str(i)]
    village_tags = [i for i in village_tags if '\n' not in str(i)]

    allvillage_codes = []
    for village in village_tags:
            village_name = village.text
            village_code = village.get('value')
            village_dic = {}
            #village_dic['dist_code'] = dist_code
            village_dic['village_name'] = village_name
            village_dic['village_code'] = village_code
            try:
                allvillage_codes.append(village_dic)
            except:
                print('No villages found')
                pass

    dataframe = pd.DataFrame(allvillage_codes)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        logger.info("Dataframe is fetched")
    return dataframe


def get_ap_hh_employment(lobj, logger,finyear):

    url = 'https://mgnregs.ap.gov.in/Nregs/'
    session = requests.Session()
    response = session.get(url)

    cookies = session.cookies
    print(cookies)
    location_codes = get_ap_village_codes(lobj,logger)
    logger.info(f"Filtered df shape {location_codes.shape}")
    logger.info(location_codes)

    state_code='02'
    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]

    #filtered_codes = location_codes[location_codes.panchayat_code == int(panchayat_code)].reset_index(drop=True)

    dfs_list = []
    for index, row in location_codes.iterrows():
        village_code = str(row['village_code'])
        village_name = row['village_name']
        logger.info(panchayat_code + village_code + village_name + finyear)
        logger.debug("block_code : %s , panchayat_code: %s ,village_code: %s" % (block_code,panchayat_code,village_code))
        try:
            dataframe = fetch_hh_employment(logger,district_code,block_code,panchayat_code,village_code,finyear,cookies)
            dataframe['village_name'] = village_name
        except ValueError as v:
            dataframe = None
            pass
        if dataframe is not None:
            dfs_list.append(dataframe)
    if len(dfs_list):
        dataframe = pd.concat(dfs_list)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        logger.info("Dataframe is fetched")
    return dataframe

def get_ap_jobcard_register(lobj,logger):

    location_codes = get_ap_village_codes(lobj,logger)
    logger.info(f"Filtered df shape {location_codes.shape}")
    logger.info(location_codes)

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]

    #filtered_codes = location_codes[location_codes.panchayat_code == int(panchayat_code)].reset_index(drop=True)

    dfs_list = []
    for index, row in location_codes.iterrows():
        village_code = str(row['village_code'])
        village_name = row['village_name']
        logger.info(panchayat_code + village_code + village_name)
        dataframe = fetch_ap_jobcard_register_for_village(logger,district_code,block_code,panchayat_code,village_code,village_name)
        dfs_list.append(dataframe)
    dataframe = pd.concat(dfs_list).reset_index(drop=True)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        logger.info(dataframe.head())

    return dataframe

    
def get_ap_jobcard_register_old(lobj, logger):
    """Download Jobcard Register for a given panchayat
    return the pandas dataframe of jobcard Register"""
    dataframe = None
    response = requests.get('https://mgnregs.ap.gov.in/Nregs/')
    cookies = response.cookies

    cookie = str(cookies).split('=')[-1].split(' ')[0]
    logger.info(f"Fetching Jobcard Register for {lobj.code}")
    logger.info(f"state url = {lobj.home_url}")
    url = f"{lobj.home_url}?requestType=WageSeekersRH&actionVal=JobCardHolder&page=WageSeekersHome&param=JCHI"
    logger.info(url)
    district_code = lobj.district_code[-2:]

    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]
    column_headers = ['srno', 'tjobcard', 'jobcard', 'head_of_household',
                      'registraction_date', 'caste', 'no_of_disabled',
                      'no_of_shg_members', 'no_of_males', 'no_of_females']
    logger.info(district_code)
    logger.info(block_code)
    logger.info(panchayat_code  )

    if lobj.state_code == "36":
        state_code = "02"
        params = (
          ('requestType', 'Household_engRH'),
          ('actionVal', 'view'),
        )
        base_url = lobj.home_url
    else:
        state_code = "-1"
        column_headers = column_headers.remove("caste")
        base_url = "http://www.mgnregs.ap.gov.in/Nregs/"
        params = (
                ('requestType', 'WageSeekersRH'),
                ('actionVal', 'JobCardHolder'),
                ('param', 'JCHI'),
                ('type', '-1'),
                ('Ajaxid', 'go'),
        )

    logger.debug("DistrictCode: %s, block_code : %s , panchayat_code: %s " % (district_code,block_code,panchayat_code))
    headers = {
        'authority': 'mgnregs.ap.gov.in',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
        'accept': '*/*',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=NewReportsRH&actionVal=R1Display&page=Newreportcenter_ajax_eng',
        'accept-language': 'en-US,en;q=0.9,te;q=0.8',
        'cookie': f'JSESSIONID={cookie}',
    }

    data = [
    ('State', '-1'),
    ('District', district_code),
    ('Mandal', block_code),
    ('Panchayat', panchayat_code),
    ('Village', '-1'),
    ('HouseHoldId', ''),
    ('Go', '')
    ]
    logger.debug(lobj.home_url)
    response = requests.get(base_url)
    cookies = response.cookies
    logger.debug(response.cookies)
    response = requests.post(lobj.home_url, headers=headers, params=params, cookies=cookies, data=data)
    jobcard_prefix = f"{lobj.state_short_code}-"
    extract_dict = {}
    extract_dict['column_headers'] = column_headers
    extract_dict['table_id'] = 'sortable'
    extract_dict['data_start_row'] = 2
    if response.status_code == 200:
        myhtml = response.content
       #filename = f'../Data/panchayat_html/{district_code}_{block_code}_{panchayat_code}.html'
       #with open(filename, 'wb') as html_file:
       #    logger.info(f'Writing file[{filename}]')
       #    if LOCAL_DOWNLOAD:
       #        html_file.write(response.content)

        soup = BeautifulSoup(response.content, 'lxml')
        select = soup.find(id = 'Village')
        options = select.find_all('option')

        dfs = []
        for option in options:
            logger.info(f'{option}')
            village_code = option['value']
            if village_code == '-1':
                continue
            village_name = option.text
            logger.info(f'Fetching jobcard register for village_code[{village_code}]/village_name[{village_name}]')
            df = fetch_ap_jobcard_register_for_village(logger, cookie, district_code, block_code, panchayat_code, village_code, village_name, extract_dict)
            if len(df):
                #logger.info(df.head)
                dfs.append(df)
        dataframe = pd.concat(dfs)
       #if LOCAL_DOWNLOAD:
       #    dataframe.to_csv(filename.replace('.html', '.csv'), index=False)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        logger.info(dataframe.head())
    return dataframe

def ap_nrega_download_page(logger, url, session=None, cookies=None, params=None, headers=None):
    max_retry = 5
    retry = 0
    res = None
    timeout = 2
    while (retry < max_retry):
        try:
            if session:
                logger.info(f'Attempting using *session* to fetch the URL[{url}] for the {retry+1} time')
                res = session.get(url, cookies=cookies, params=params,
                                   headers=headers, verify=False)
            else:
                logger.info(f'Attempting using *requests* to fetch the URL[{url}] for the {retry+1} time')
                res = requests.get(url, cookies=cookies, params=params,
                                   headers=headers, verify=False)
            retry = max_retry
        except Exception as e:
            retry = retry + 1
            timeout += 5
            time.sleep(timeout)
            logger.warning(f'Need to retry. Failed {retry} time(s). Exception[{e}]')
            logger.warning(f'Waiting for {timeout} seconds...')
    return res

def get_ap_suspended_payments_r14_5(lobj, logger,finyear):
    """ Will download Suspended Payment information per panchayat"""
    dataframe = None
    logger.info(f"Fetching Suspended Payment Report {lobj.code}")
    logger.info(f"state url = {lobj.home_url}")
    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]
    location_id = district_code + block_code + panchayat_code
    finyear = str(finyear)
    full_finyear = get_full_finyear(finyear)
    lobj.home_url = "http://www.mgnregs.ap.gov.in/Nregs/"
    column_headers = ['sno', 'jobcard', 'worker_code', 'name',
                      'amount', 'from_date', 'to_date']
    logger.debug("DistrictCode: %s, block_code : %s , panchayat_code: %s location %s" % (district_code,block_code,panchayat_code, location_id))
    url = 'http://www.mgnregs.ap.gov.in/Nregs/'
    res = ap_nrega_download_page(logger, url)
    if (not res) or (res.status_code != 200):
        return None
    cookies = res.cookies
    logger.debug(f"cookies are {cookies}")
    headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        } 
    params = (
            ('requestType', 'SmartCardreport_engRH'),
            ('actionVal', 'DelayPay'),
            ('id', location_id),
            ('type', '-1'),
            ('Date', '-1'),
            ('File', ''),
            ('Agency', ''),
            ('listType', ''),
            ('yearMonth', '-1'),
            ('ReportType', 'Program : ALL'),
            ('flag', '-1'),
            ('Rtype', ''),
            ('Date1', '-1'),
            ('wtype', ''),
            ('ytype', ''),
            ('Date2', '-1'),
            ('ltype', ''),
            ('year', full_finyear),
            ('program', 'ALL'),
            ('fileName', location_id),
            ('stype', ''),
            ('ptype', ''),
            ('lltype', ''),
        )
    url1 = 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet'
    # response = requests.get('http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, verify=False)
    response = ap_nrega_download_page(logger, url1, headers=headers,
                                      params=params, cookies=cookies)
    if (not response) or (response.status_code != 200):
        return None

    if response.status_code != 200:
        return None
    myhtml = response.content
    extract_dict = {}
    extract_dict['column_headers'] = column_headers
    extract_dict['table_id'] = 'sortable'
    extract_dict['data_start_row'] = 3
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    if dataframe is None:
        return None
    logger.info(f"the shape of dataframe is {dataframe.shape}")
    dataframe['tjobcard'] = "~" + dataframe['jobcard']
    dataframe['finyear'] = finyear
    logger.info('Added finyear col to the df')
    dataframe = insert_location_details(logger, lobj, dataframe)
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    cols = location_cols + ["tjobcard",'finyear'] + column_headers
    dataframe = dataframe[cols]
    return dataframe

def get_ap_not_enrolled_r14_21A(lobj, logger):
    """Will download report not enrolled R 14_21A"""
    dataframe = None
    logger.info(f"Fetching Suspended Payment Report {lobj.code}")
    logger.info(f"state url = {lobj.home_url}")
    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]
    location_id = district_code + block_code
    lobj.home_url = "http://www.mgnregs.ap.gov.in/Nregs/"
    column_headers = [
        'S No',
        'Panchayat Name',
        'Wage Seekers Identified as NOT ENROLLED Opening Balance of 19-Jun-2020',
        'Wage Seekers Identified as NOT ENROLLED On 19-Jun-2020',
        'Wage Seekers Identified as NOT ENROLLED EOD of 19-Jun-2020',
        'Wage Seekers seeded on 19-Jun-2020 Seeded with UID',
        'Wage Seekers seeded on 19-Jun-2020 Seeded with existing EID *',
        'Wage Seekers seeded on 19-Jun-2020 Seeded with newly enrolled EID **',
        'Wage Seekers seeded on 19-Jun-2020 Total',
        'Wage Seekers Identified as NOT ENROLLED - Closing Balance of 19-Jun-2020'
    ]
    logger.debug("DistrictCode: %s, block_code : %s , panchayat_code: %s location %s" % (district_code,block_code,panchayat_code, location_id))
    url = 'http://www.mgnregs.ap.gov.in/Nregs/'
    res = ap_nrega_download_page(logger, url)
    if (not res) or (res.status_code != 200):
        return None
    cookies = res.cookies
    logger.debug(f"cookies are {cookies}")
    headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        } 
    params = (
            ('requestType', 'SmartCardreport_engRH'),
            ('actionVal', 'UIDSeedingLink'),
            ('id', location_id),
            ('type', ''),
            ('Date', '-1'),
            ('File', ''),
            ('Agency', ''),
            ('listType', ''),
            ('yearMonth', '-1'),
            ('ReportType', ''),
            ('flag', 'UIDLink'),
            ('Rtype', ''),
            ('Date1', '-1'),
            ('wtype', ''),
            ('ytype', ''),
            ('Date2', '-1'),
            ('ltype', ''),
            ('year', ''),
            ('program', ''),
            ('fileName', location_id),
            ('stype', ''),
            ('ptype', ''),
            ('lltype', ''),
        )

    url1 = 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet'
    # response = requests.get('http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, verify=False)
    response = ap_nrega_download_page(logger, url1, headers=headers,                                            
                                      params=params, cookies=cookies)
    if (not response) or (response.status_code != 200):
        return None

    if response.status_code != 200:
        return None
    myhtml = response.content
    extract_dict = {}
    extract_dict['column_headers'] = column_headers
    extract_dict['table_id'] = 'sortable'
    extract_dict['data_start_row'] = 3
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    logger.info(f"the shape of dataframe is {dataframe.shape}")
    if dataframe is None:
        return None
    # dataframe['tjobcard'] = "~" + dataframe['HouseHold Code']
    dataframe = insert_location_details(logger, lobj, dataframe)
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    #cols = location_cols + ["tjobcard"] + column_headers
    cols = location_cols + column_headers
    dataframe = dataframe[cols]
    return dataframe

def get_ap_nefms_report_r14_37(lobj, logger):
    """Will download NEFMS report"""
    dataframe = None
    logger.info(f"Fetching Suspended Payment Report {lobj.code}")
    logger.info(f"state url = {lobj.home_url}")
    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]
    location_id = f'{district_code}~{block_code}~{panchayat_code}'
    lobj.home_url = "http://www.mgnregs.ap.gov.in/Nregs/"
    column_headers = [
        'S.No.',
        'File_Name',
        'File_Sent_Date',
        'Social_Category',
        'Social_Category_URL',
        'No._of_Transactions',
        'Transaction_URL',
        'No._of_Wage_Seekers',
        'Total_Amount',
        'Success_Transactions',
        'Success_Amount',
        'Rejected_Transactions',
        'Rejected_Transaction_URL',
        'Rejected_Amount',
        'Response_Pending_Transactions',
        'Response_Pending_Amount',
        'Release_Pending_Transactions',
        'Release_Pending_Amount'
    ]
    logger.debug("DistrictCode: %s, block_code : %s , panchayat_code: %s location %s" % (district_code,block_code,panchayat_code, location_id))
    url = 'http://www.mgnregs.ap.gov.in/Nregs/'
    logger.info(f'To Get Cookies, fetching URL[{url}]')
    res = ap_nrega_download_page(logger, url)
    if (not res) or (res.status_code != 200):
        return None
    cookies = res.cookies
    logger.debug(f"cookies are {cookies}")
    headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        } 
    params = (
            ('requestType', 'SmartCardreport_engRH'),
            ('actionVal', 'NEFMS'),
            ('id', location_id),
            ('type', ''),
            ('Date', '-1'),
            ('File', ''),
            ('Agency', ''),
            ('listType', ''),
            ('yearMonth', '-1'),
            ('ReportType', ''),
            ('flag', '-1'),
            ('Rtype', '-1'),
            ('Date1', '-1'),
            ('wtype', '-1'),
            ('ytype', '-1'),
            ('Date2', '-1'),
            ('ltype', '-1'),
            ('year', ''),
            ('program', ''),
            ('fileName', location_id),
            ('stype', '-1'),
            ('ptype', '-1'),
            ('lltype', '-1'),
        )
    url1 = 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet'
    # response = requests.get('http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, verify=False)
    response = ap_nrega_download_page(logger, url1, headers=headers,
                                      params=params, cookies=cookies)
    if (not response) or (response.status_code != 200):
        return None

    if response.status_code != 200:
        return None
    myhtml = response.content
    extract_dict = {}
    extract_dict['column_headers'] = column_headers
    extract_dict['table_id'] = 'sortable'
    extract_dict['data_start_row'] = 3
    extract_dict['extract_url_array'] = [3, 5, 9]
    extract_dict['url_prefix'] = "http://www.mgnregs.ap.gov.in"
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    if dataframe is None:
        return None
    logger.info(f"the shape of dataframe is {dataframe.shape}")

    #dataframe['tjobcard'] = "~" + dataframe['HouseHold Code']
    dataframe = insert_location_details(logger, lobj, dataframe)
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    #cols = location_cols + ["tjobcard"] + column_headers
    cols = location_cols + column_headers
    dataframe = dataframe[cols]
    logger.info(f"Before: the shape of dataframe is {dataframe.shape}")
    dataframe = dataframe[dataframe['S.No.'] != 'Total']
    logger.info(f"After: the shape of dataframe is {dataframe.shape}")
    return dataframe

def get_ap_labour_report_r3_17(lobj, logger):
    """Will download Labour Report R 3_17"""
    dataframe = None
    logger.info(f"Fetching Suspended Payment Report {lobj.code}")
    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    location_id = district_code + block_code
    lobj.home_url = "http://www.mgnregs.ap.gov.in/Nregs/"
    date = datetime.strftime(datetime.now() - timedelta(1), '%d/%m/%Y')
    column_headers = [
        'S.No',
        'Panchayat Name',
        'No of Groups Registred',
        'No of groups Demanded',
        'Tagetted labour per day',
        'Targetted persondays for the month',
        'No of groups working',
        'No. of labour reported',
        'No of GPs where work not happened',
        'No of Persondays generated',
        '% of groups working over demanded',
        '% of labour reported over Target'
    ]
    logger.debug("DistrictCode: %s, block_code : %s , location %s" % (district_code, block_code, location_id))
    url = 'http://www.mgnregs.ap.gov.in/Nregs/'
    '''
    Using sesssion below
    res = ap_nrega_download_page(logger, url)
    if (not res) or (res.status_code != 200):
        return None
    cookies = res.cookies
    logger.debug(f"cookies are {cookies}")
    '''
    headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        } 
    params = (
            ('requestType', 'PRReportsRH'),
            ('actionVal', 'DailyLabour'),
            ('id', location_id),
            ('type', '-1'),
            ('type1', ''),
            ('dept', ''),
            ('fromDate', ''),
            ('toDate', ''),
            ('Rtype', ''),
            ('reportGroup', ''),
            ('fto', ''),
            ('LinkType', '-1'),
            ('rtype', ''),
            ('reptype', ''),
            ('date', date),
            ('program', ''),
            ('type2', ''),
            ('type3', ''),
        )
    url1 = 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet'
    logger.info('Fetching URL[%s] for cookies' % url)
    cookies = ''
    with requests.session() as session:
        #res = session.get(url)
        res = ap_nrega_download_page(logger, url, session=session)
        cookies = session.cookies
        if (not res) or (res.status_code != 200):
            return None
        logger.debug(f"cookies are {cookies}")

        response = ap_nrega_download_page(
            logger, url1, session=session,
            headers=headers, params=params,
            cookies=cookies)
        if (not response) or (response.status_code != 200):
            return None
    myhtml = response.content
    extract_dict = {}
    extract_dict['column_headers'] = column_headers
    extract_dict['table_id'] = 'sortable'
    extract_dict['data_start_row'] = 3
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    logger.info(f"the shape of dataframe is {dataframe.shape}")
    if dataframe is None:
        return None
    dataframe = insert_location_details(logger, lobj, dataframe)
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name"]
    cols = location_cols + column_headers
    dataframe = dataframe[cols]
    logger.info(f"Before: the shape of dataframe is {dataframe.shape}")
    dataframe = dataframe[dataframe['S.No'] != 'Total']
    logger.info(f"After: the shape of dataframe is {dataframe.shape}")

    return dataframe


def get_ap_rejected_transactions(lobj, logger, fto_report_df):
    """Get AP rejected transactions"""
    logger.debug(f"Labout report df columns {fto_report_df.columns}")
    column_headers = ['sr_no', 'jobcard_num', 'worker_code', 'name',
                      'epayorder_no', 'amount', 'nrega_account_no', 'file_sent_date',
                      'credit_status', 'credited_account_no', 'bank_name',
                      'bank_iin', 'utr_no', 'remarks']
    headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        } 
    cookies = None
    params = None
    url = 'http://www.mgnregs.ap.gov.in/Nregs/'
    with requests.session() as session:
        #res = session.get(url)
        res = ap_nrega_download_page(logger, url, session=session)
        cookies = session.cookies
        if (not res) or (res.status_code != 200):
            return None
        logger.debug(f"cookies are {cookies}")
    job_list = []
    extract_dict = {}
    extract_dict['column_headers'] = column_headers
    extract_dict['table_id'] = 'sortable'
    extract_dict['data_start_row'] = 2
    static_col_names = ["panchayat_code", "panchayat_name", "fto_date","fto_number"]
    static_col_values = ["", "", "",""]
    func_name = "ap_fetch_table_from_url"

    for index, row in fto_report_df.iterrows():
        rej_count = row.get("Rejected_Transactions", 0)
        fto_number = row.get("File_Name", None)
        fto_date = row.get("File_Sent_Date", None)
        rej_url = row.get("Social_Category_URL", None)
        panchayat_code = row.get("panchayat_code", None)
        panchayat_name = row.get("panchayat_name", None)
        static_col_values = [panchayat_code, panchayat_name, fto_date,fto_number]
        if rej_count > 0:
            func_args = [lobj, rej_url, session, headers, params, cookies, extract_dict,
                 static_col_names, static_col_values]
            job_dict = {
                'func_name' : func_name,
                'func_args' : func_args
            }
            job_list.append(job_dict)
    csv_array = []
    for item in job_list:
        func_args = item["func_args"]
        csv_array.append(func_args)
    dataframe = pd.DataFrame(csv_array)
    dataframe = libtech_queue_manager(logger, job_list)
    dataframe = insert_location_details(logger, lobj, dataframe)
    dataframe['jobcard_num'] = "~" + dataframe['jobcard_num']
    '''
    changed by Ranu
    '''
    dataframe['~epayorder_no'] = "~" + dataframe['epayorder_no'].astype(str)
    dataframe = dataframe[dataframe["jobcard_num"]!="~2"]
    dataframe = insert_finyear_in_dataframe(logger, dataframe,
                                           "fto_date",
                                           date_format="%d-%b-%Y")
    dataframe["jobcard_num"] = dataframe["jobcard_num"].astype(str)
    jr_df = lobj.fetch_report_dataframe(logger, "ap_jobcard_register")
    jr_df.rename(columns = {'Jobcard ID':'tjobcard', 'Govt of India JobCard ID':'jobcard'},
                       inplace = True)
    col_list = ["jobcard_num", "nic_jobcard", "village_code", "village_name", "head_of_the_family"]
    jr_df = jr_df[col_list]
    wr_df = lobj.fetch_report_dataframe(logger, "worker_register")
    if jr_df is not None:
        dataframe = dataframe.merge(jr_df, on=['jobcard_num'], how='left')
  # if wr_df is not None:
  #     col_list = ["jobcard", "village_name", "head_of_household", "caste"]
  #     wr_df = wr_df[col_list]
  #     wr_df = wr_df.drop_duplicates(subset=["jobcard"])
  #     dataframe = dataframe.merge(wr_df, on=['jobcard'], how='left')
    additional_cols = ["state_code", "state_name", "district_code",
                      "district_name", "block_code", "block_name",
                      "panchayat_code", "panchayat_name", "village_name", 
                       "village_code", "head_of_the_family", "jobcard_num", "nic_jobcard", 
                        "finyear", "fto_date", "~epayorder_no"]
    cols = additional_cols + column_headers
    dataframe = dataframe[cols]
    return dataframe

def get_ap_rejected_transactions1(lobj, logger):
    """Get AP rejected transactions"""
    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]
    headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        } 
    location_id = f'{district_code}~{block_code}~{panchayat_code}'
    logger.info(location_id)
    baseURL=f"http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=SmartCardreport_engRH&actionVal=NEFMS&id={location_id}&type=&Date=-1&File=&Agency=&listType=&yearMonth=-1&ReportType=&flag=-1&Rtype=-1&Date1=-1&wtype=-1&ytype=-1&Date2=-1&ltype=-1&year=&program=&fileName={location_id}&stype=-1&ptype=-1&lltype=ITDA"
    logger.info(f"AP URL is {baseURL}")
    url = 'http://www.mgnregs.ap.gov.in/Nregs/'
    cookies = ''
    with requests.session() as session:
        #res = session.get(url)
        res = ap_nrega_download_page(logger, url, session=session)
        cookies = session.cookies
        if (not res) or (res.status_code != 200):
            return None
        logger.debug(f"cookies are {cookies}")

        response = ap_nrega_download_page(
            logger, baseURL, session=session,
            headers=headers, params=None,
            cookies=cookies)
        if (not response) or (response.status_code != 200):
            return None
    logger.debug("Found HTML!!!")
    myhtml = response.content
    with open("/tmp/a.html", "wb") as f:
        f.write(myhtml)


def get_ap_employment_generation_r2_2(lobj, logger):

    logger.info(f"Downloading employment generation for {lobj.block_name}")

    url = 'http://www.mgnregs.ap.gov.in/Nregs/home.do'
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]

    ap_block_code = district_code + block_code

    logger.info(ap_block_code)
    dataframe = fetch_ap_r2_2(logger, ap_block_code,cookies=cookies)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        colnames = ['sno','state_code','state_name','district_code','district_name','block_code','block_name','panchayat_name',
                     'registered_households', 'num_applicants','employment_sc', 'employment_st', 'employment_other',
                     'employment_total', 'iay', 'women', 'others', 'fin_year']
        dataframe = dataframe[colnames]

    return dataframe

def fetch_ap_r2_2(logger,block_code,cookies=None):

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=NewReportsRH&actionVal=EmpGenRep&id=03&type=',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }

    params = (
        ('requestType', 'NewReportsRH'),
        ('actionVal', 'EmpGenRep'),
        ('id', block_code),
        ('type', ''),
    )

    response = get_request_with_retry_timeout(logger,'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies)

    if response is None:
        return None

    df = pd.read_html(response.content)[0]

    string = df.iloc[-2,0]

    fin_year = re.findall(r'[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]',string)[0]

    col_names = ["sno", "panchayat_name", "registered_households", "num_applicants", "employment_sc", "employment_st", "employment_other", "employment_total", "iay", "women", "others"]

    df.columns = col_names

    df['fin_year'] = fin_year
    
    df = df[:-4]

    return df


def get_ap_jobcard_updation_report_r24_43(lobj, logger):

    logger.info(f"Downloading jobcard updation report for {lobj.block_name}")

    url = 'http://www.mgnregs.ap.gov.in/Nregs/home.do'
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]

    ap_block_code = district_code + block_code

    logger.info(ap_block_code)
    dataframe = fetch_R24_43_report(logger, ap_block_code,cookies=cookies)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        colnames = ['sno','state_code','state_name','district_code','district_name','block_code','block_name','panchayat_name',
                    'jcs_printed', 'jcs_distributed','jc_not_distributed_nameInOtherJC', 'jc_not_distributed_double',
                    'jc_not_distributed_death', 'jc_not_distributed_photoNotMatching','jc_not_distributed_migrated']

        dataframe = dataframe[colnames]
        
    return dataframe

def fetch_R24_43_report(logger, block_code,cookies=None):

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=LandDevelopmentNewRH&actionVal=jobcardsUpdation&id=03&type=&type1=&type2=&year=&month=&Linktype=&selecteddate=&ctype=-1%20&subtype=&id1=&program=-1&design1=&design2=&reportCode=null&finYear=&category=&rep_type=&isItda=-1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }

    params = (
        ('requestType', 'LandDevelopmentNewRH'),
        ('actionVal', 'jobcardsUpdation'),
        ('id', block_code),
        ('type', ''),
        ('type1', ''),
        ('type2', ''),
        ('year', ''),
        ('month', ''),
        ('Linktype', ''),
        ('selecteddate', ''),
        ('ctype', '-1 '),
        ('subtype', ''),
        ('id1', ''),
        ('program', '-1'),
        ('design1', ''),
        ('design2', ''),
        ('reportCode', 'null'),
        ('finYear', ''),
        ('category', ''),
        ('rep_type', ''),
        ('isItda', '-1'),
    )

    response = get_request_with_retry_timeout(logger,'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies)

    dataframe = pd.read_html(response.content)[-1]
    dataframe.columns = ['sno', 'panchayat_name', 'jcs_printed', 'jcs_distributed', 'jc_not_distributed_nameInOtherJC',
                        'jc_not_distributed_double','jc_not_distributed_death',
                        'jc_not_distributed_photoNotMatching','jc_not_distributed_migrated']
    dataframe = dataframe[:-1]
    return dataframe


def get_ap_cm_dashboard_employment_r26_1(lobj, logger):

    logger.info(f"Downloading CM Dashboard employment report for {lobj.block_name}")

    url = 'http://www.mgnregs.ap.gov.in/Nregs/home.do'
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]

    ap_block_code = district_code + block_code

    logger.info(ap_block_code)
    dataframe = fetch_ap_26_1(logger, ap_block_code,cookies=cookies)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        colnames = ['sno','state_code','state_name','district_code','district_name','block_code','block_name','panchayat_name',
                    'approved_laborBudget','target_personDays_during_month', 'pd_acheived_during','percent_acheived_during',
                    'target_personDays_upto_month','pd_acheived_upto','percent_acheived_upto','overall_acheivement_%']

        dataframe = dataframe[colnames]

        return dataframe

def fetch_ap_26_1(logger,block_code,cookies=None):

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=CMDashBoardRH&actionVal=CMdashBoardTotalEmp&id=03&JOB_No=03&type=null&listType=&var=null',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    params = (
        ('requestType', 'CMDashBoardRH'),
        ('actionVal', 'CMdashBoardTotalEmp'),
        ('id', block_code),
        ('JOB_No', block_code),
        ('type', 'null'),
        ('listType', ''),
        ('var', 'null'),
    )

    response = get_request_with_retry_timeout(logger,'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies)
    
    dataframe = pd.read_html(response.content)[0]
    
    dataframe = dataframe[:-7]
    
    dataframe.columns = ['sno','panchayat_name','approved_laborBudget','target_personDays_during_month',
                        'pd_acheived_during','percent_acheived_during',
                        'target_personDays_upto_month','pd_acheived_upto','percent_acheived_upto','overall_acheivement_%']
    
    return dataframe

def get_ap_approved_labour_budget_r13_18(lobj, logger):

    logger.info(f"Downloading approved labour budget report for {lobj.block_name}")

    url = 'http://www.mgnregs.ap.gov.in/Nregs/home.do'
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]

    ap_block_code = district_code + block_code

    logger.info(ap_block_code)
    dataframe = fetch_ap_R13_18(logger, ap_block_code,cookies=cookies)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        colnames = ['sno','state_code','state_name','district_code','district_name','block_code','block_name','panchayat_name','month_name','cumulative_no_of_households','cumulative_persondays_lakhs','amount_at_248rs_lakhs']
        dataframe = dataframe[colnames]

    return dataframe

def fetch_ap_R13_18(logger,block_code,cookies=None):

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=LandDevelopmentRH&actionVal=LabourBudget&id=03&type=&type1=&type2=&year=&month=&Linktype=&selecteddate=&ctype=-1%20&subtype=&id1=&program=-1&design1=&design2=&reportCode=null&finYear=&category=&rep_type=&isItda=-1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    params = (
        ('requestType', 'LandDevelopmentRH'),
        ('actionVal', 'LabourBudget'),
        ('id', block_code),
        ('type', ''),
        ('type1', ''),
        ('type2', ''),
        ('year', ''),
        ('month', ''),
        ('Linktype', ''),
        ('selecteddate', ''),
        ('ctype', '-1 '),
        ('subtype', ''),
        ('id1', ''),
        ('program', '-1'),
        ('design1', ''),
        ('design2', ''),
        ('reportCode', 'null'),
        ('finYear', ''),
        ('category', ''),
        ('rep_type', ''),
        ('isItda', '-1'),
    )

    response = get_request_with_retry_timeout(logger,'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies)

    dataframe = pd.read_html(response.content)[-1]

    dataframe.columns = ['sno','panchayat_name','month_name','cumulative_no_of_households','cumulative_persondays_lakhs','amount_at_248rs_lakhs']
    
    return dataframe


def get_ap_cm_dashboard_total_expenditure_r26_2(lobj, logger):

    logger.info(f"Downloading CM Dashboard total expenditure report for {lobj.block_name}")

    url = 'http://www.mgnregs.ap.gov.in/Nregs/home.do'
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]

    ap_block_code = district_code + block_code

    logger.info(ap_block_code)
    dataframe = fetch_ap_R26_2(logger, ap_block_code,cookies=cookies)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        colnames = ['sno','state_code','state_name','district_code','district_name','block_code','block_name','panchayat_name',
                    'year_target_wage_rs_in_lakhs','year_target_material_rs_in_lakhs','year_target_total_rs_in_lakhs',
                    'target_upto_the_month_wage_rs_in_lakhs','target_upto_the_month_material_rs_in_lakhs',
                    'target_upto_the_month_total_rs_in_lakhs', 'expenditure_wage_rs_in_lakhs','expenditure_material_rs_in_lakhs',
                    'expenditure_contingent_rs_in_lakhs','expenditure_total_rs_in_lakhs','balance_material_entitlement_rs_in_lakhs',
                    'percent_achievement_year', 'percent_achievement_upto_the_month']

        dataframe = dataframe[colnames]

    return dataframe

def fetch_ap_R26_2(logger,block_code,cookies=None):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=CMDashBoardRH&actionVal=TotalExpenditure&id=03&JOB_No=03&type=&listType=&var=null',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    params = (
        ('requestType', 'CMDashBoardRH'),
        ('actionVal', 'TotalExpenditure'),
        ('id', block_code),
        ('JOB_No', block_code),
        ('type', ''),
        ('listType', ''),
        ('var', 'null'),
    )

    response = get_request_with_retry_timeout(logger,'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies)

    dataframe = pd.read_html(response.content)[0]

    dataframe = dataframe[:-7]

    dataframe.columns = ['sno', 'panchayat_name','year_target_wage_rs_in_lakhs','year_target_material_rs_in_lakhs',
                         'year_target_total_rs_in_lakhs','target_upto_the_month_wage_rs_in_lakhs','target_upto_the_month_material_rs_in_lakhs',
                          'target_upto_the_month_total_rs_in_lakhs', 'expenditure_wage_rs_in_lakhs','expenditure_material_rs_in_lakhs',
                         'expenditure_contingent_rs_in_lakhs','expenditure_total_rs_in_lakhs','balance_material_entitlement_rs_in_lakhs',
                          'percent_achievement_year', 'percent_achievement_upto_the_month']
    

    return dataframe


def get_ap_cm_dashboard_avg_days_worked_r26_3(lobj, logger):

    logger.info(f"Downloading CM Dashboard average days worked report for {lobj.block_name}")

    url = 'http://www.mgnregs.ap.gov.in/Nregs/home.do'
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]

    ap_block_code = district_code + block_code

    logger.info(ap_block_code)
    dataframe = fetch_ap_R26_3(logger, ap_block_code,cookies=cookies)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        colnames = ['sno','state_code','state_name','district_code','district_name','block_code','block_name','panchayat_name', 'hh_worked','person_days','avg_employment_per_hh']

        dataframe = dataframe[colnames]

    return dataframe

def fetch_ap_R26_3(logger,block_code,cookies=None):

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=CMDashBoardRH&actionVal=CMdashBoardAvgDaysWorked&id=03&JOB_No=03&type=null&listType=&var=null',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    params = (
        ('requestType', 'CMDashBoardRH'),
        ('actionVal', 'CMdashBoardAvgDaysWorked'),
        ('id', block_code),
        ('JOB_No', block_code),
        ('type', 'null'),
        ('listType', ''),
        ('var', 'null'),
    )

    response = get_request_with_retry_timeout(logger,'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies)

    dataframe = pd.read_html(response.content)[0]

    dataframe = dataframe[:-6]

    dataframe.columns = ['sno','panchayat_name','hh_worked','person_days','avg_employment_per_hh']
    
    return dataframe


def get_ap_cm_dashboard_avg_wage_report_r26_5(lobj, logger):

    logger.info(f"Downloading CM Dashboard average wage report for {lobj.block_name}")

    url = 'http://www.mgnregs.ap.gov.in/Nregs/home.do'
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]

    ap_block_code = district_code + block_code

    logger.info(ap_block_code)
    dataframe = fetch_ap_R26_5(logger, ap_block_code,cookies=cookies)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        colnames = ['sno','state_code','state_name','district_code','district_name','block_code','block_name', 
                    'panchayat_name', 'month_wise_wage_per_day','wage_expenditure_rs_in_lakhs', 'person_days_in_lakhs',
                    'avg_wage_per_day_rs']
        dataframe = dataframe[colnames]

    return dataframe

def fetch_ap_R26_5(logger,block_code,cookies=None):
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:81.0) Gecko/20100101 Firefox/81.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=CMDashBoardRH&actionVal=CMdashBoardAvgWageRate&id=03&JOB_No=03&type=&listType=&var=null',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }

    params = (
        ('requestType', 'CMDashBoardRH'),
        ('actionVal', 'CMdashBoardAvgWageRate'),
        ('id', block_code),
        ('JOB_No', block_code),
        ('type', ''),
        ('listType', ''),
        ('var', 'null'),
    )

    response = get_request_with_retry_timeout(logger,'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies)
    
    dataframe = pd.read_html(response.content)[0]
    
    colnames = ['sno', 'panchayat_name', 'month_wise_wage_per_day','wage_expenditure_rs_in_lakhs', 'person_days_in_lakhs',
               'avg_wage_per_day_rs']
    
    dataframe.columns = colnames
    
    dataframe = dataframe[:-6]

    return dataframe

def get_ap_grama_sachivalayam_report_r29_1(lobj, logger,fromDate='01/04/2020', toDate='10/10/2020'):

    logger.info(f"Downloading Grama sachivalayam report for {lobj.block_name}")

    url = 'http://www.mgnregs.ap.gov.in/Nregs/home.do'
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]

    ap_block_code = district_code + block_code

    logger.info(ap_block_code)
    dataframe = fetch_ap_r29_1(logger, ap_block_code,fromDate = fromDate, toDate=toDate,cookies=cookies)

    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        colnames = ['sno','state_code','state_name','district_code','district_name','block_code','block_name', 
                    'panchayat_name','jcs_created','wageseekers_created','jcs_issued','wageseekers_issued','fromDate','toDate']
        dataframe = dataframe[colnames]

        return dataframe


def fetch_ap_r29_1(logger,block_code,fromDate = None,toDate = None,cookies = None):



    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:81.0) Gecko/20100101 Firefox/81.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'http://www.mgnregs.ap.gov.in/Nregs/FrontServlet?requestType=HorticultureRH&actionVal=jobcardCreation&id=03&type=ALL&type1=&dept=&fromDate=01/04/2020&toDate=10/10/2020&Rtype=&reportGroup=&fto=Visakhapatnam&LinkType=-1&rtype=&reptype=&date=&program=&type2=&type3=',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }

    params = (
        ('requestType', 'HorticultureRH'),
        ('actionVal', 'jobcardCreation'),
        ('id', block_code),
        ('type', 'ALL'),
        ('type1', ''),
        ('dept', ''),
        ('fromDate', fromDate),
        ('toDate', toDate),
        ('Rtype', ''),
        ('reportGroup', ''),
        ('fto', 'Ananthagiri'),
        ('LinkType', '-1'),
        ('rtype', ''),
        ('reptype', ''),
        ('date', ''),
        ('program', ''),
        ('type2', ''),
        ('type3', ''),
    )

    response = requests.get('http://www.mgnregs.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies)

    dataframe = pd.read_html(response.content)[0]
    
    dataframe.columns = ['sno','panchayat_name','jcs_created','wageseekers_created','jcs_issued','wageseekers_issued']
    
    dataframe = dataframe[:-2]
    
    dataframe['fromDate'] = fromDate
    
    dataframe['toDate'] = toDate
        
    return dataframe