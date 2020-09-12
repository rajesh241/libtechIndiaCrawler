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
from datetime import datetime, timedelta

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
                                                request_with_retry_timeout
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
    url = 'http://www.nrega.ap.gov.in/Nregs/FrontServlet?requestType=NewReportsRH&actionVal=R1Display&page=Newreportcenter_ajax_eng#'
    url='http://www.nrega.telangana.gov.in/Nregs/FrontServlet?requestType=Common_engRH&actionVal=musterinfo&page=MusterRolls_eng'
    url2='http://www.nrega.telangana.gov.in/Nregs/FrontServlet?'
    if lobj.state_code == "36":
        state_code = "02"
        base_url = 'http://www.nrega.telangana.gov.in/Nregs/FrontServlet?requestType=Common_engRH&actionVal=musterinfo&page=MusterRolls_eng'
        url2='http://www.nrega.telangana.gov.in/Nregs/FrontServlet?'
    else:
        state_code = "01"
        base_url = "http://www.nrega.ap.gov.in/Nregs/"
        url2 = 'http://www.nrega.ap.gov.in/Nregs/FrontServlet?requestType=Common_engRH&actionVal=musterinfo&page=MusterRolls_eng'
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

def fetch_ap_jobcard_register_for_village(logger, cookies, district_code, block_code, panchayat_code, village_code, village_name, extract_dict):
    logger.info(f'fetch_ap_jobcard_register_for_village(cookies={cookies}, block_code={block_code}, panchayat_code={panchayat_code}, village_code={village_code}, village_name={village_name})')

    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Mobile Safari/537.36',
        'Origin': 'http://www.nrega.ap.gov.in',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': 'http://www.nrega.ap.gov.in/Nregs/FrontServlet?requestType=WageSeekersRH&actionVal=JobCardHolder&param=JCHI&type=-1&Atype=Display&Ajaxid=Village',
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
        'State': '01',
        'District': district_code,
        'Mandal': block_code,
        'Panchayat': panchayat_code,
        'Village': village_code,
        'HouseHoldId': '',
        'Go': ''
    }
    url = 'http://www.nrega.ap.gov.in/Nregs/FrontServlet'
    #response = requests.post('http://www.nrega.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, data=data, verify=False)
    response = request_with_retry_timeout(logger, url, data=data, headers=headers, params=params, cookies=cookies) 
    if response is None:
        return []
    content = response.content
    try:
        #df = pd.read_html(content, attrs = {'id': 'sortable'})[0]
        df = get_dataframe_from_html(logger, content, mydict=extract_dict)
    except Exception as e:
        logger.error(f'Errored in fetch. Exception[{e}]')
        df = pd.read_html(content, attrs = {'id': 'Table2'})[0]
        if df.iloc[0, 0] == 'No records found for the selection made':
            logger.error('No records found for the selection made')
            return []
        
    if df is None:
        return []

    df['village_code'] = village_code
    df['village_name'] = village_name
    return df

def get_ap_jobcard_register(lobj, logger):
    """Download Jobcard Register for a given panchayat
    return the pandas dataframe of jobcard Register"""
    dataframe = None
    logger.info(f"Fetching Jobcard Register for {lobj.code}")
    logger.info(f"state url = {lobj.home_url}")
    url = f"{lobj.home_url}?requestType=WageSeekersRH&actionVal=JobCardHolder&page=WageSeekersHome&param=JCHI"
    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]
    column_headers = ['srno', 'tjobcard', 'jobcard', 'head_of_household',
                      'registraction_date', 'caste', 'no_of_disabled',
                      'no_of_shg_members', 'no_of_males', 'no_of_females']
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
        base_url = "http://www.nrega.ap.gov.in/Nregs/"
        params = (
                ('requestType', 'WageSeekersRH'),
                ('actionVal', 'JobCardHolder'),
                ('param', 'JCHI'),
                ('type', '-1'),
                ('Ajaxid', 'go'),
        )

    logger.debug("DistrictCode: %s, block_code : %s , panchayat_code: %s " % (district_code,block_code,panchayat_code))
    headers = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:54.0) Gecko/20100101 Firefox/54.0',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.5',
      'Accept-Encoding': 'gzip, deflate',
      'Content-Type': 'application/x-www-form-urlencoded',
      'Referer': url,
      'Connection': 'keep-alive',
      'Upgrade-Insecure-Requests': '1',
     }

    data = [
    ('State', state_code),
    ('District', district_code),
    ('Mandal', block_code),
    ('Panchayat', panchayat_code),
    ('Village', '-1'),
    ('HouseHoldId', ''),
    ('Go', ''),
    ('spl', 'Select'),
    ('input2', ''),
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
            df = fetch_ap_jobcard_register_for_village(logger, cookies, district_code, block_code, panchayat_code, village_code, village_name, extract_dict)
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

def get_ap_suspended_payments_r14_5(lobj, logger):
    """ Will download Suspended Payment information per panchayat"""
    dataframe = None
    logger.info(f"Fetching Suspended Payment Report {lobj.code}")
    logger.info(f"state url = {lobj.home_url}")
    district_code = lobj.district_code[-2:]
    block_code = lobj.block_code[-2:]
    panchayat_code = lobj.panchayat_code[8:10]
    location_id = district_code + block_code + panchayat_code
    lobj.home_url = "http://www.nrega.ap.gov.in/Nregs/"
    column_headers = ['S.No.', 'HouseHold Code', 'Worker Code', 'Name',
                      'Amount', 'From Date', 'To Date']
    logger.debug("DistrictCode: %s, block_code : %s , panchayat_code: %s location %s" % (district_code,block_code,panchayat_code, location_id))
    url = 'http://www.nrega.ap.gov.in/Nregs/'
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
            ('year', '2019-2020'),
            ('program', 'ALL'),
            ('fileName', location_id),
            ('stype', ''),
            ('ptype', ''),
            ('lltype', ''),
        )
    url1 = 'http://www.nrega.ap.gov.in/Nregs/FrontServlet'
    # response = requests.get('http://www.nrega.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, verify=False)
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
    dataframe['tjobcard'] = "~" + dataframe['HouseHold Code']
    dataframe = insert_location_details(logger, lobj, dataframe)
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    cols = location_cols + ["tjobcard"] + column_headers
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
    lobj.home_url = "http://www.nrega.ap.gov.in/Nregs/"
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
    url = 'http://www.nrega.ap.gov.in/Nregs/'
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

    url1 = 'http://www.nrega.ap.gov.in/Nregs/FrontServlet'
    # response = requests.get('http://www.nrega.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, verify=False)
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
    lobj.home_url = "http://www.nrega.ap.gov.in/Nregs/"
    column_headers = [
        'S.No.',
        'File_Name',
        'File_Sent_Date',
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
    url = 'http://www.nrega.ap.gov.in/Nregs/'
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
            ('lltype', 'ITDA'),
        )
    url1 = 'http://www.nrega.ap.gov.in/Nregs/FrontServlet'
    # response = requests.get('http://www.nrega.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, verify=False)
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
    extract_dict['extract_url_array'] = [3, 8]
    extract_dict['url_prefix'] = "http://www.nrega.ap.gov.in"
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    logger.info(f"the shape of dataframe is {dataframe.shape}")
    if dataframe is None:
        return None
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
    lobj.home_url = "http://www.nrega.ap.gov.in/Nregs/"
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
    url = 'http://www.nrega.ap.gov.in/Nregs/'
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
    url1 = 'http://www.nrega.ap.gov.in/Nregs/FrontServlet'
    logger.info('Fetching URL[%s] for cookies' % url)
    cookies = ''
    with requests.session() as session:
        #res = session.get(url)
        res = ap_nrega_download_page(logger, url, session=session)
        cookies = session.cookies
        if (not res) or (res.status_code != 200):
            return none
        logger.debug(f"cookies are {cookies}")

        response = ap_nrega_download_page(
            logger, url1, session=session,
            headers=headers, params=params,
            cookies=cookies)
        if (not response) or (response.status_code != 200):
            return none
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
    column_headers = ['sr_no', 'tjobcard', 'worker_code', 'name',
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
    url = 'http://www.nrega.ap.gov.in/Nregs/'
    with requests.session() as session:
        #res = session.get(url)
        res = ap_nrega_download_page(logger, url, session=session)
        cookies = session.cookies
        if (not res) or (res.status_code != 200):
            return none
        logger.debug(f"cookies are {cookies}")
    job_list = []
    extract_dict = {}
    extract_dict['column_headers'] = column_headers
    extract_dict['table_id'] = 'sortable'
    extract_dict['data_start_row'] = 2
    static_col_names = ["panchayat_code", "panchayat_name", "fto_date"]
    static_col_values = ["", "", ""]
    func_name = "ap_fetch_table_from_url"

    for index, row in fto_report_df.iterrows():
        rej_count = row.get("Rejected_Transactions", 0)
        fto_date = row.get("File_Sent_Date", None)
        rej_url = row.get("Rejected_Transaction_URL", None)
        panchayat_code = row.get("panchayat_code", None)
        panchayat_name = row.get("panchayat_name", None)
        static_col_values = [panchayat_code, panchayat_name, fto_date]
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
    dataframe['~tjobcard'] = "~" + dataframe['tjobcard']
    dataframe['~epayorder_no'] = "~" + str(dataframe['epayorder_no'])
    dataframe = dataframe[dataframe["~tjobcard"]!="~2"]
    dataframe = insert_finyear_in_dataframe(logger, dataframe,
                                           "fto_date",
                                           date_format="%d-%b-%Y")
    dataframe["tjobcard"] = dataframe["tjobcard"].astype(str).astype(int)
    jr_df = lobj.fetch_report_dataframe(logger, "ap_jobcard_register")
    jr_df.rename(columns = {'Jobcard ID':'tjobcard', 'Govt of India JobCard ID':'jobcard'},
                       inplace = True)
    col_list = ["tjobcard", "jobcard", "village_code", "village_name", "Head of Family"]
    jr_df = jr_df[col_list]
    wr_df = lobj.fetch_report_dataframe(logger, "worker_register")
    if jr_df is not None:
        dataframe = dataframe.merge(jr_df, on=['tjobcard'], how='left')
  # if wr_df is not None:
  #     col_list = ["jobcard", "village_name", "head_of_household", "caste"]
  #     wr_df = wr_df[col_list]
  #     wr_df = wr_df.drop_duplicates(subset=["jobcard"])
  #     dataframe = dataframe.merge(wr_df, on=['jobcard'], how='left')
    additional_cols = ["state_code", "state_name", "district_code",
                      "district_name", "block_code", "block_name",
                      "panchayat_code", "panchayat_name", "village_name", 
                       "village_code", "Head of Family", "jobcard", "~tjobcard", 
                        "finyear", "fto_date"]
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
    baseURL=f"http://www.nrega.ap.gov.in/Nregs/FrontServlet?requestType=SmartCardreport_engRH&actionVal=NEFMS&id={location_id}&type=&Date=-1&File=&Agency=&listType=&yearMonth=-1&ReportType=&flag=-1&Rtype=-1&Date1=-1&wtype=-1&ytype=-1&Date2=-1&ltype=-1&year=&program=&fileName={location_id}&stype=-1&ptype=-1&lltype=ITDA"
    logger.info(f"AP URL is {baseURL}")
    url = 'http://www.nrega.ap.gov.in/Nregs/'
    cookies = ''
    with requests.session() as session:
        #res = session.get(url)
        res = ap_nrega_download_page(logger, url, session=session)
        cookies = session.cookies
        if (not res) or (res.status_code != 200):
            return none
        logger.debug(f"cookies are {cookies}")

        response = ap_nrega_download_page(
            logger, baseURL, session=session,
            headers=headers, params=None,
            cookies=cookies)
        if (not response) or (response.status_code != 200):
            return none
    logger.debug("Found HTML!!!")
    myhtml = response.content
    with open("/tmp/a.html", "wb") as f:
        f.write(myhtml)

