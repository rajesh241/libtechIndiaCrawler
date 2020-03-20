"""This has all the functions for crawling NIC NREGA Website"""
#pylint: disable-msg = too-many-locals
#pylint: disable-msg = too-many-branches
#pylint: disable-msg = too-many-statements

import re
import urllib.parse as urlparse
from urllib.parse import parse_qs
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from libtech_lib.generic.commons import  (get_current_finyear,
                      get_full_finyear,
                      standardize_dates_in_dataframe,
                      get_default_start_fin_year,
                      insert_location_details,
                      get_finyear_from_muster_url,
                      get_fto_finyear
                     )
from libtech_lib.generic.api_interface import api_get_report_url, api_get_report_dataframe
from libtech_lib.generic.html_functions import get_dataframe_from_html, get_dataframe_from_url
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
        dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        logger.info(dataframe.head())
    return dataframe
 
