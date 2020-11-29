"""This has all the functions for crawling NIC NREGA Website"""
#pylint: disable-msg = too-many-locals
#pylint: disable-msg = too-many-branches
#pylint: disable-msg = too-many-statements
#pylint: disable-msg = line-too-long
#pylint: disable-msg = bare-except
import os
import re
from pathlib import Path
import urllib.parse as urlparse
from urllib.parse import parse_qs
from slugify import slugify
import json
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from libtech_lib.generic.commons import  (get_current_finyear,
                                          get_current_finmonth,
                                          get_default_start_fin_year,
                                          get_percentage,
                                          get_previous_date,
                                          insert_location_details,
                                          insert_finyear_in_dataframe,
                                          is_english,
                                          get_full_finyear
                                          )
from libtech_lib.generic.api_interface import  (api_get_report_url,
                                                api_get_report_dataframe,
                                                api_location_update,
                                                get_location_dict
                                               )
from libtech_lib.generic.html_functions import ( get_dataframe_from_html,
                                                 get_dataframe_from_url,
                                                 nic_download_page, 
                                                 find_url_containing_text,
                                                 find_urls_containing_text,
                                                 get_options_list,
                                                 request_with_retry_timeout,
                                                 get_request_with_retry_timeout
                                               )
from libtech_lib.generic.libtech_queue import libtech_queue_manager
mis_url = "https://mnregaweb4.nic.in"
#HOMEDIR = str(Path.home())
#JSONCONFIGFILE = f"{HOMEDIR}/.libtech/crawlerConfig.json"
#with open(JSONCONFIGFILE) as CONFIG_file:
#    CONFIG = json.load(CONFIG_file)
#NREGA_DATA_DIR = CONFIG['nrega_data_dir']
#JSON_CONFIG_DIR = CONFIG['json_config_dir']
NREGA_DATA_DIR = os.environ.get('NREGA_DATA_DIR', None)
JSON_CONFIG_DIR = os.environ.get('JSON_CONFIG_DIR', None) 

def get_nic_block_urls(lobj, logger):
    """Will download NIC Block URLs"""
    csv_array = []
    finyear = get_current_finyear()
    full_finyear = get_full_finyear(finyear)
    url = lobj.mis_block_url.replace("fullFinYear", full_finyear)
    logger.debug(f"Block page is {url}")
    start_fin_year = get_default_start_fin_year()
    end_fin_year = get_current_finyear()
    column_headers = ['finyear', 'report_name', 'report_slug',
                       'mis_url', 'location_type', 'panchayat_code']
    mis_url_prefix = f"https://mnregaweb4.nic.in/netnrega/placeHolder1/"
    mis_url_prefix_panchayat = f"https://mnregaweb4.nic.in/netnrega/placeHolder1/placeHolder2/"
    for finyear in range(start_fin_year, end_fin_year+1):
        logger.debug(f"Currently Processing {finyear} for {lobj.code}")
        finyear = str(finyear)
        full_finyear = get_full_finyear(finyear)
        base_url = lobj.mis_block_url.replace("fullFinYear", full_finyear)
        res = request_with_retry_timeout(logger, base_url,  method="get")
        if res.status_code != 200:
            return None
        myhtml = res.content
        mysoup = BeautifulSoup(myhtml, "lxml")
        links = mysoup.findAll("a")
        for link in links:
            href = link.get("href", "")
            text = link.text
            mis_url = mis_url_prefix + href
            location_type = 'block'
            panchayat_code = ''
            row = [finyear, text, slugify(text), mis_url, location_type,
                   panchayat_code]
            csv_array.append(row)
            if (finyear == str(get_current_finyear())) and (slugify(text) == "registration-application-register"):
                response = request_with_retry_timeout(logger, mis_url,
                                                      method="get")
                if response is not None:
                    html = response.content
                    soup = BeautifulSoup(html, "lxml")
                    mylinks = soup.findAll("a")
                    for mylink in mylinks:
                        href = mylink.get("href", "")
                        if "panchregpeople.aspx" in href:
                            mis_url = mis_url_prefix_panchayat + href
                            location_type = 'panchayat'
                            parsed = urlparse.urlparse(mis_url)
                            params_dict = parse_qs(parsed.query)
                            panchayat_code = params_dict.get('panchayat_code', [''])[0]
                            row = [finyear, text, slugify(text), mis_url,
                                   location_type, panchayat_code]
                            csv_array.append(row)
            if (finyear == str(get_current_finyear())) and (slugify(text) == "job-card-employment-register"):
                response = request_with_retry_timeout(logger, mis_url,
                                                      method="get")
                if response is not None:
                    html = response.content
                    soup = BeautifulSoup(html, "lxml")
                    mylinks = soup.findAll("a")
                    for mylink in mylinks:
                        href = mylink.get("href", "")
                        if "JobCardReg.aspx" in href:
                            mis_url = mis_url_prefix_panchayat + href
                            location_type = 'panchayat'
                            parsed = urlparse.urlparse(mis_url)
                            params_dict = parse_qs(parsed.query)
                            panchayat_code = params_dict.get('Panchayat_code', [''])[0]
                            row = [finyear, text, slugify(text), mis_url,
                                   location_type, panchayat_code]
                            csv_array.append(row)
    dataframe = pd.DataFrame(csv_array, columns=column_headers)
    dataframe = insert_location_details(logger, lobj, dataframe)
    return dataframe


def nic_server_status(logger, location_code, scheme='nrega'):
    """Different States have different servers. This function will fetch the
    status of server for the corresponding location before the crawling
    starts"""
    if scheme != "nrega":
        return True
    ldict = get_location_dict(logger, location_code=location_code)
    state_name = ldict.get("state_name", None)
    state_code = ldict.get("state_code", None)
    crawl_ip = ldict.get("crawl_ip", None)
    url = f"http://{crawl_ip}/netnrega/homestciti.aspx?state_code={state_code}&state_name={state_name}"
    logger.info(url)
    res = requests.get(url)
    logger.info(res.status_code)
    return bool(res.status_code == 200)


def get_jobcard_register(lobj, logger):
    """Download Jobcard Register for a given panchayat
    return the pandas dataframe of jobcard Register"""
    logger.info(f"Fetching Jobcard Register for {lobj.code}")
    logger.info(lobj.panchayat_page_url)
    #First we need to fetch jobcard Register URL
    res = requests.get(lobj.panchayat_page_url)
    cookies = res.cookies
    jobcard_register_url = None
    url_prefix = f"http://{lobj.crawl_ip}/netnrega/"
    if res.status_code == 200:
        myhtml = res.content
        mysoup = BeautifulSoup(myhtml, "lxml")
        links = mysoup.findAll("a")
        for link in links:
            href = link.get("href", "")
            if "JobCardReg.aspx" in href:
                jobcard_register_url = url_prefix + href
                break
    logger.info(f"JR URL is {jobcard_register_url}")
    #Once wehave the jobcard Register URL, we can fetch it and parse it
    jobcard_prefix = f"{lobj.state_short_code}-"
    extract_dict = {}
    column_headers = ['srno', 'jobcard', 'jobcard_url', 'head_of_household']
    extract_dict['pattern'] = jobcard_prefix
    extract_dict['column_headers'] = column_headers
    extract_dict['extract_url_array'] = [1]
    extract_dict['base_url'] = jobcard_register_url
    extract_dict['url_prefix'] = f'http://{lobj.crawl_ip}/netnrega/placeHolder1/placeHolder2/'
    dataframe = None
    if jobcard_register_url is not None:
        res = requests.get(jobcard_register_url, cookies=cookies)
        if res.status_code == 200:
            myhtml = res.content
            dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
            dataframe = insert_location_details(logger, lobj, dataframe)
    return dataframe

def get_jobcard_register_mis(lobj, logger, nic_urls_df):
    """Download Jobcard Register for a given panchayat"""
    column_headers = ['srno', 'jobcard', 'jobcard_url', 'head_of_household']
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    all_cols = location_cols + column_headers
    empty_dataframe = pd.DataFrame(columns=all_cols)
    logger.debug(f"In download jobcard register for {lobj.panchayat_code}")
    logger.debug(f"Shape of df is {nic_urls_df.shape}")
    finyear = get_current_finyear()
    filtered_df = nic_urls_df[(nic_urls_df["report_slug"] == "job-card-employment-register") &
                              (nic_urls_df['finyear'] == int(finyear)) &
                              (nic_urls_df["panchayat_code"] == int(lobj.panchayat_code))]
    logger.debug(f"Filtered DF shape is {filtered_df.shape}")
    ##Establish session for request
    #session = requests.Session()
    #session.get(lobj.mis_state_url)
    #cookies = session.cookies
    response = request_with_retry_timeout(logger, lobj.mis_state_url)
    cookies = response.cookies
    logger.debug(lobj.mis_state_url)
    logger.debug(f"session cookies {cookies}")
    myhtml = None
    url = None
    
    for index, row in filtered_df.iterrows():
        url = row.get("mis_url")
        logger.debug(f"Url is {url}")
        response = request_with_retry_timeout(logger, url, cookies=cookies,
                                              method="get")
        break
    if url is None:
        return empty_dataframe
    if response is None:
        logger.debug("Returning from here")
        return empty_dataframe
    myhtml = response.content
    jobcard_prefix = f"{lobj.state_short_code}-"
    extract_dict = {}
    extract_dict['pattern'] = jobcard_prefix
    extract_dict['column_headers'] = column_headers
    extract_dict['extract_url_array'] = [1]
    extract_dict['base_url'] = url
    extract_dict['url_prefix'] = f'{mis_url}/netnrega/placeHolder1/placeHolder2/'
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        dataframe = dataframe[all_cols]
        return dataframe
    return empty_dataframe

def get_worker_register_mis(lobj, logger, nic_urls_df):
    """This will download the worker registe based on nic_urls and df"""
    logger.info(f"In download worker register for {lobj.panchayat_code}")
    logger.info(f"Shape of df is {nic_urls_df.shape}")
    finyear = get_current_finyear()
    filtered_df = nic_urls_df[(nic_urls_df["report_slug"] == "registration-application-register") &
                              (nic_urls_df['finyear'] == int(finyear)) &
                              (nic_urls_df["panchayat_code"] == int(lobj.panchayat_code))]
    logger.debug(f"Filtered DF shape is {filtered_df.shape}")
    ##Establish session for request
    resp = get_request_with_retry_timeout(logger, lobj.mis_state_url)
    if resp is None:
        return None
    cookies = resp.cookies
    myhtml = None
    for index, row in filtered_df.iterrows():
        url = row.get("mis_url")
        logger.debug(f"Url is {url}")
        response = get_request_with_retry_timeout(logger, url, cookies=cookies)
        if response is not None:
            myhtml = response.content
        break
    if myhtml is None:
        return None
    jobcard_prefix = f"{lobj.state_short_code}-"
    extract_dict = {}
    extract_dict['pattern'] = jobcard_prefix
    extract_dict['data_start_row'] = 2
    extract_dict['split_cell_array'] = [9]
    extract_dict['column_headers'] = ['sr_no', 'head_of_household', 'caste',
                                      'IAY_LR', 'name', 'father_husband_name',
                                      'gender', 'age', 'jobcard_request_date',
                                      'jobcard', 'jobcard_issue_date', 'jobcard_remarks',
                                      'disabled', 'minority',
                                      'jobcard_verification_date']
    dataframe = None
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    ##Here we need to do some post processing dataframe entries
    to_delete_rows = []
    village_name = ''
    prev_head_of_household = ''
    prev_caste = ''
    dataframe['village_name'] = ''
    for index, row in dataframe.iterrows():
        first_col = row.get('sr_no', None)
        if first_col is None:
            to_delete_rows.append(index)
            break
        name = row.get('name', None)
        if name is not None:
            if '*' in name:
                name = name.replace('*', '').lstrip().rstrip()
                dataframe.loc[index, 'name'] = name
        head_of_household = row.get('head_of_household', '')
        if head_of_household == '':
            head_of_household = prev_head_of_household
            dataframe.loc[index, 'head_of_household'] = head_of_household
        prev_head_of_household = head_of_household
        caste = row.get('caste', '')
        if caste == '':
            caste = prev_caste
            dataframe.loc[index, 'caste'] = caste
        prev_caste = caste
        if not first_col.isdigit():
            to_delete_rows.append(index)
        if "Villages" in first_col:
            village_name = re_extract_village_name(first_col)
            logger.debug(f" village name is {village_name}")
        dataframe.loc[index, 'village_name'] = village_name
    logger.debug(f"rows to be deleted are {to_delete_rows}")
    dataframe = dataframe.drop(to_delete_rows)
    dataframe = insert_location_details(logger, lobj, dataframe)
    dataframe = dataframe.reset_index(drop=True)
    return dataframe
    
def get_worker_register(lobj, logger, worker_reg_url=None, cookies=None):
    """This function will get the worker register from the nrega url"""
    dataframe = None
    if worker_reg_url is None:
        logger.info(f"panchayat page url is {lobj.panchayat_page_url}")
        res = requests.get(lobj.panchayat_page_url)
        cookies = res.cookies
        url = None
        url_prefix = f"http://{lobj.crawl_ip}/netnrega/"
        if res.status_code == 200:
            myhtml = res.content
            mysoup = BeautifulSoup(myhtml, "lxml")
            links = mysoup.findAll("a")
            for link in links:
                href = link.get("href", "")
                if "Panchregpeople.aspx" in href:
                    url = url_prefix + href
                    break
        logger.info(f"url is {url}")
    else:
        url = worker_reg_url
    jobcard_prefix = f"{lobj.state_short_code}-"
    extract_dict = {}
    extract_dict['pattern'] = jobcard_prefix
    extract_dict['data_start_row'] = 2
    extract_dict['split_cell_array'] = [9]
    extract_dict['column_headers'] = ['sr_no', 'head_of_household', 'caste',
                                      'IAY_LR', 'name', 'father_husband_name',
                                      'gender', 'age', 'jobcard_request_date',
                                      'jobcard', 'jobcard_issue_date', 'jobcard_remarks',
                                      'disabled', 'minority',
                                      'jobcard_verification_date']
    dataframe = None
    if url is not None:
        res = requests.get(url, cookies=cookies)
        if res.status_code == 200:
            myhtml = res.content
            dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    ##Here we need to do some post processing dataframe entries
    to_delete_rows = []
    village_name = ''
    prev_head_of_household = ''
    prev_caste = ''
    dataframe['village_name'] = ''
    for index, row in dataframe.iterrows():
        first_col = row.get('sr_no', None)
        if first_col is None:
            to_delete_rows.append(index)
            break
        name = row.get('name', None)
        if name is not None:
            if '*' in name:
                name = name.replace('*', '').lstrip().rstrip()
                dataframe.loc[index, 'name'] = name
        head_of_household = row.get('head_of_household', '')
        if head_of_household == '':
            head_of_household = prev_head_of_household
            dataframe.loc[index, 'head_of_household'] = village_name
        prev_head_of_household = head_of_household
        caste = row.get('caste', '')
        if caste == '':
            caste = prev_caste
            dataframe.loc[index, 'caste'] = caste
        prev_caste = caste
        if not first_col.isdigit():
            to_delete_rows.append(index)
        if "Villages" in first_col:
            village_name = re_extract_village_name(first_col)
            logger.info(f" village name is {village_name}")
        dataframe.loc[index, 'village_name'] = village_name
    logger.info(f"rows to be deleted are {to_delete_rows}")
    dataframe = dataframe.drop(to_delete_rows)
    dataframe = insert_location_details(logger, lobj, dataframe)
    dataframe = dataframe.reset_index(drop=True)
    return dataframe

def re_extract_village_name(in_s):
    """Function to extract village name from given string"""
    matched = None
    village_name_regex = re.compile(r'Villages\s+:\s+')
    remo = village_name_regex.search(in_s)
    if remo is not None:
        matched = remo.group()
        matched = in_s.replace(matched, '')
    return matched

def get_jobcard_transactions(lobj, logger, jobcards_dataframe):
    """ Given a list of jobciard urls this function will fetch all jobcard
    transactions"""
    logger.info(f"we are going to Jobcard Transactions")
    logger.info(f"state page url is {lobj.mis_state_url}")
    job_list = []
    #response = requests.get(lobj.panchayat_page_url)
    response = requests.get(lobj.mis_state_url)
    cookies = response.cookies
    ###Below is the worker function that needs to be called
    func_name = "fetch_jobcard_details"
    for index, row in jobcards_dataframe.iterrows():
        func_args = []
        url = row.get("jobcard_url", None)
        if url is not None:
            func_args = [lobj, url, cookies]
            job_dict = {
                'func_name' : func_name,
                'func_args' : func_args
            }
            job_list.append(job_dict)
    #dataframe = libtech_queue_manager(logger, job_list)
    dataframe = libtech_queue_manager(logger, job_list)
    if dataframe is None:
        return
    transactions_columns =  ['srno', 'name', 'work_date', 'noOfDays',
                             'work_name', 'work_name_url',
                                    'muster_no', 'muster_no_url', 'amount',
                             'payment_due', 'finyear']
    logger.debug(f"transactions columns {transactions_columns}")
    dataframe = insert_location_details(logger, lobj, dataframe)
    dataframe['muster_code'] = dataframe.apply(lambda row: str(row['block_code'])+"_"+str(row['finyear'])+"_"+str(row['muster_no']), axis=1)
    #dataframe['muster_code'] = ''
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    cols = location_cols + ["muster_code"] + transactions_columns
    dataframe = dataframe[cols]
    return dataframe

def get_muster_list(lobj, logger, jobcard_transactions_df,
                       current_muster_list_df):
    """Updating the muster list"""
    if current_muster_list_df is None:
        logger.info("Current Muster list is None")
     
def update_muster_list(lobj, logger, jobcard_transactions_df,
                       current_muster_list_df):
    """From the jobcard transactions df this function will first get unique
    work urls and from work urls it will get all unique muster urls"""
    ##We need to find out the musters which do not exists inthe current musteirlist
    logger.debug(f"Shape of jobcard transactiosn df is {jobcard_transactions_df.shape}")
    logger.debug(f"jobcard transactions columns{jobcard_transactions_df.columns}")
    if current_muster_list_df is None:
        filtered_df = jobcard_transactions_df
    else:
        merged_df = jobcard_transactions_df.merge(current_muster_list_df,
                                                 on=['muster_code'], how='left', indicator=True)
        filtered_df = merged_df[merged_df['_merge'] == 'left_only']
        
    logger.debug(f"the shape of filtered df is {filtered_df.shape}")
    col_list = ['muster_no', 'muster_url', 'finyear', 'date_from', 'date_to', 'work_name', 'work_code', 'block_code']
    work_url_array = filtered_df.work_name_url.unique()
    logger.info(f"Number of unique work urls is {len(work_url_array)}")
    response = request_with_retry_timeout(logger, lobj.mis_state_url, method="get")
    cookies = response.cookies
    job_list = []
    func_name = "fetch_muster_urls"
    for url in work_url_array:
        func_args = [lobj, url, cookies]
        job_dict = {
            'func_name' : func_name,
            'func_args' : func_args
        }
        job_list.append(job_dict)
    dataframe = libtech_queue_manager(logger, job_list)
    if dataframe is None:
        return current_muster_list_df
    #finyear_regex = re.compile(r'finyear=\d{4}-\d{4}')
    for index, row in dataframe.iterrows():
        url = row['muster_url']
        muster_no = row['muster_no']
        parsed = urlparse.urlparse(url)
        params_dict = parse_qs(parsed.query)
        work_name = params_dict.get('wn', [''])[0]
        work_code = params_dict.get('workcode', [''])[0]
        date_from = params_dict.get('dtfrm', [''])[0]
        date_to = params_dict.get('dtto', [''])[0]
        full_finyear = params_dict.get('finyear', ['00'])[0]
        finyear = full_finyear[-2:]
       # finyear = get_finyear_from_muster_url(logger, url, finyear_regex)
        if finyear.isdigit():
            finyear = int(finyear)
        else:
            finyear = 0
        muster_code = f"{lobj.block_code}_{finyear}_{muster_no}"
        dataframe.loc[index, 'finyear'] = finyear
        dataframe.loc[index, 'date_from'] = date_from
        dataframe.loc[index, 'date_to'] = date_to
        dataframe.loc[index, 'work_name'] = work_name
        dataframe.loc[index, 'work_code'] = work_code
        dataframe.loc[index, 'muster_code'] = muster_code
    #dataframe['block_code'] = lobj.block_code
    logger.info(f"shape of dataframe is {dataframe.shape}")
    start_fin_year = get_default_start_fin_year()
    dataframe = dataframe[dataframe['finyear'] >= start_fin_year]
    logger.info(f"shape of dataframe is {dataframe.shape}")
    dataframe = dataframe.drop_duplicates()
    logger.info(f"shape of dataframe is {dataframe.shape}")
    dataframe = dataframe.reset_index(drop=True)
    if current_muster_list_df is None:
        return dataframe
    concat_df = pd.concat([dataframe, current_muster_list_df])
    concat_df = concat_df.drop_duplicates()
    concat_df = concat_df.reset_index(drop=True)
    return concat_df

def create_work_payment_report(lobj, logger):
    """This function will create work payment report by merging different
    reports"""
    mt_df = lobj.fetch_report_dataframe(logger, "muster_transactions")
    ml_df = lobj.fetch_report_dataframe(logger, "muster_list")
    if mt_df is None:
        return None
    logger.debug(f"Shape of muster Transactions {mt_df.shape}")
    if ml_df is not None:
        wp_df = mt_df.merge(ml_df, on=['muster_code'], how='left')
     #   merged_df = jobcard_transactions_df.merge(current_muster_list_df,
     #                                            on=['muster_code'], how='left', indicator=True)
    logger.debug(f"Shape of report after muster merge  {wp_df.shape}")
    worker_df = lobj.fetch_report_dataframe(logger, "worker_register")
    if worker_df is not None:
        worker_df = worker_df.drop_duplicates(subset=['jobcard', 'name'],
                                              keep='last')
        wp_df = wp_df.merge(worker_df, how='left',
                         on=['jobcard', 'name'])
    logger.debug(f"Shape of report after worker merge  {wp_df.shape}")
    col_list = ['state_code', 'state_name', 'district_code', 'district_name',
                'block_code', 'block_name', 'panchayat_name', 'panchayat_code',
                'village_name', 'jobcard', 'name', 'relationship',
                'head_of_household', 'caste', 'IAY_LR', 'father_husband_name',
                'gender', 'age', 'jobcard_request_date', 'jobcard_issue_date', 'jobcard_remarks',
                'disabled', 'minority', 'jobcard_verification_date', 'work_code',
                'work_name', 'finyear', 'muster_code', 'muster_no', 'muster_index',
                'date_from', 'date_to', 'muster_url', 'm_caste',
                'days_worked', 'day_wage', 'm_labour_wage', 'm_travel_cost',
                'm_tools_cost', 'total_wage', 'm_postoffice_bank_name',
                'm_pocode_bankbranch', 'm_poadd_bankbranchcode',
                'm_wagelist_no', 'muster_status', 'credited_date',
                ]

    logger.info(wp_df.columns)
    wp_df = wp_df[col_list]
    return wp_df
def update_muster_transactions(lobj, logger):
    """This function will download muster transactions"""
    current_mt_df = lobj.fetch_report_dataframe(logger, "muster_transactions")
    if current_mt_df is None:
        completed_muster_list = []
    else:
        completed_ml_df = current_mt_df[current_mt_df["is_complete"] == 1]
        completed_muster_list = completed_ml_df.muster_code.unique()
    logger.debug(f"Completed muster list is {completed_muster_list}")
    ml_df = lobj.fetch_report_dataframe(logger, "muster_list")
    logger.info(f"length of musters that need to be downloaded is {len(ml_df)}")
    response = get_request_with_retry_timeout(logger, lobj.mis_state_url)
    if response is None:
        return
    cookies = response.cookies
    logger.info(f"cookies are {cookies}")
    ##Prepareing to run queue functions
    job_list = []
    func_name = "fetch_muster_details"
    muster_column_config_file = f"{JSON_CONFIG_DIR}/muster_column_name_dict.json"
    logger.info(muster_column_config_file)
    with open(muster_column_config_file) as config_file:
        muster_column_dict = json.load(config_file)
    logger.info(muster_column_dict)
    for index, row in ml_df.iterrows():
        muster_code = row['muster_code']
        if muster_code in completed_muster_list:
            continue
        url = row['muster_url']
        url = url.replace(lobj.crawl_ip, "mnregaweb4.nic.in")
        muster_no = row['muster_no']
        finyear = row['finyear']
        block_code = lobj.block_code
        func_args = [lobj, url, cookies, muster_no, finyear, block_code,
                     muster_column_dict, muster_code]
        job_dict = {
            'func_name' : func_name,
            'func_args' : func_args
        }
        job_list.append(job_dict)
    muster_col_list = ['muster_index', 'jobcard', 'caste', 'days_worked', 'day_wage',
                       'm_labour_wage',
                              'm_travel_cost', 'm_tools_cost', 'total_wage',
                       'm_postoffice_bank_name',
                              'm_pocode_bankbranch', 'm_poadd_bankbranchcode',
                       'm_wagelist_no',
                              'muster_status', 'credited_date',
                              'muster_no', 'finyear', 'block_code', 'name',
                       'relationship']
    dataframe = libtech_queue_manager(logger, job_list, num_threads=500)
    if dataframe is None:
        return current_mt_df
    if current_mt_df is None:
        return dataframe
    dataframe = pd.concat([dataframe, completed_ml_df])
    return dataframe
  
def get_muster_transactions(lobj, logger):
    """This function will download Muster Transactions"""
    """First it will take existing muster transactions, find out incomplete
    musters. Second it will take muster list and find out musters which have
    never been downloaded"""
    #First lets download worker df which we will later merge with muster
    #transactions to 
    worker_df = lobj.fetch_report_dataframe(logger, "worker_register")
    jt_df = lobj.fetch_report_dataframe(logger, "jobcard_transactions")
    mt_df = lobj.fetch_report_dataframe(logger, "muster_transactions")
    ml_df = lobj.fetch_report_dataframe(logger, "muster_list")
def get_musters_to_be_downloaded(lobj, logger, muster_list_df,
                            muster_transactions_df):
    """Download the pending musters from the muster list"""
    musters_to_download_df = None
    csv_array = []
    column_headers = ["block_code", "finyear", "muster_no", "muster_url"]
    logger.info(muster_transactions_df.shape)
    logger.info(muster_transactions_df.columns)
    logger.info(muster_list_df.columns)
    logger.info(f"Length of muster list is {len(muster_list_df)}")
    jt_df = lobj.fetch_report_dataframe(logger, "jobcard_transactions")
    grouped_jt = jt_df.groupby(['finyear','muster_no']).agg({'noOfDays':'sum'}).reset_index()
    ml_merged = muster_list_df.merge(grouped_jt, on=['finyear','muster_no'], how='left',
                         indicator=True)
    muster_list_df = ml_merged[ml_merged["_merge"]=="both"]
    drop_columns = ['_merge']
    muster_list_df = muster_list_df.drop(columns=drop_columns)
    logger.info(f"Length of muster list is {len(muster_list_df)}")

    grouped_muster_transactions = muster_transactions_df.groupby(["muster_url"]).agg(
                                             { 'days_worked':"sum"}
                                     ).reset_index()
    
    logger.info(f"unique musters is {len(grouped_muster_transactions)}")
    df_all = muster_list_df.merge(grouped_muster_transactions,
                                  on=["muster_url"], how='left', indicator=True)
    logger.info(f"length of df_all is {len(df_all)}")
    left_only = df_all[df_all["_merge"]=="left_only"]
    not_downloaded = left_only[column_headers]
    logger.info(f"length of not downloaded is {len(not_downloaded)}")
    ### We are only going to re download musters which are not complete and
    ### And have not been updated in last 7 days
    date_thresold = get_previous_date(logger, delta_days=7)
    logger.info(f"date threshold is {date_thresold}")
    muster_transactions_df['lastUpdateDate'] = pd.to_datetime(muster_transactions_df['lastUpdateDate'])
    mt_filtered = muster_transactions_df[muster_transactions_df['lastUpdateDate'] < date_thresold]
    logger.info(f"Length of mt_filtered is {len(mt_filtered)}")
    grouped_muster_transactions = mt_filtered.groupby(["muster_url"]).agg(
                                             { 'days_worked':"sum"}
                                     ).reset_index()

    ###Now we will iterate to see which musters are not complete
    for index, row in grouped_muster_transactions.iterrows():
        muster_url = row.get("muster_url")
        filtered_df = muster_transactions_df[(muster_transactions_df['muster_url']==muster_url)]
        filtered_df = filtered_df[filtered_df['muster_status'] != 'Credited']
        if len(filtered_df) > 0:
            row = [muster_url]
            csv_array.append(row)
    not_complete_df = pd.DataFrame(csv_array, columns=["muster_url"])
    not_complete = not_complete_df.merge(muster_list_df, on=["muster_url"],
                                         how='left')
    not_complete = not_complete[column_headers]
    logger.info(f"Lenght incomplete Musters is {len(not_complete)}")
    logger.info(f"Lenght not download Musters is {len(not_downloaded)}")
    musters_to_download_df = pd.concat([not_complete, not_downloaded])
    return musters_to_download_df

def get_muster_transactions1(lobj, logger, muster_list_df,
                            muster_transactions_df):
    """form the muster list dataframe this will get all the muster
    transactions"""
    if muster_transactions_df is not None:
        musters_to_download_df = get_musters_to_be_downloaded(lobj, logger,
                                                              muster_list_df,
                                                              muster_transactions_df)
    else:
        musters_to_download_df = muster_list_df
    logger.info(musters_to_download_df.head())
    logger.info(f"to be downloaded is {len(musters_to_download_df)}")
    logger.info(muster_list_df.columns)
    col_list = ['state_code', 'state_name', 'district_code', 'district_name',
                'block_code', 'block_name', 'panchayat_name', 'panchayat_code',
                'village_name', 'jobcard', 'name', 'relationship',
                'head_of_household', 'caste', 'IAY_LR', 'father_husband_name',
                'gender', 'age', 'jobcard_request_date', 'jobcard_issue_date', 'jobcard_remarks',
                'disabled', 'minority', 'jobcard_verification_date', 'work_code',
                'work_name', 'finyear', 'muster_code', 'muster_no', 'muster_index',
                'date_from', 'date_to', 'muster_url', 'm_caste',
                'days_worked', 'day_wage', 'm_labour_wage', 'm_travel_cost',
                'm_tools_cost', 'total_wage', 'm_postoffice_bank_name',
                'm_pocode_bankbranch', 'm_poadd_bankbranchcode',
                'm_wagelist_no', 'muster_status', 'credited_date',
                ]
    worker_df = lobj.fetch_report_dataframe(logger, "worker_register")
    logger.info(f"shape of worker df is {worker_df.shape}")
    try:
        response = requests.get(lobj.panchayat_page_url, timeout=10)
    except requests.exceptions.Timeout as exp:
        logger.error(exp)
    cookies = response.cookies
    job_list = []
    func_name = "fetch_muster_details"
    muster_column_config_file = f"{JSON_CONFIG_DIR}/muster_column_name_dict.json"
    logger.info(muster_column_config_file)
    with open(muster_column_config_file) as config_file:
        muster_column_dict = json.load(config_file)
    logger.info(muster_column_dict)
    for index, row in musters_to_download_df.iterrows():
        url = row['muster_url']
        logger.info(url)
        muster_no = row['muster_no']
        finyear = row['finyear']
        block_code = row['block_code']
        muster_transactions_df = muster_transactions_df[(muster_transactions_df['finyear']!= finyear) |
                                             (muster_transactions_df['block_code']!=block_code) |
                                             (muster_transactions_df['muster_no']!=muster_no)]
        func_args = [lobj, url, cookies, muster_no, finyear, block_code, muster_column_dict]
        job_dict = {
            'func_name' : func_name,
            'func_args' : func_args
        }
        job_list.append(job_dict)
    muster_col_list = ['muster_index', 'jobcard', 'caste', 'days_worked', 'day_wage',
                       'm_labour_wage',
                              'm_travel_cost', 'm_tools_cost', 'total_wage',
                       'm_postoffice_bank_name',
                              'm_pocode_bankbranch', 'm_poadd_bankbranchcode',
                       'm_wagelist_no',
                              'muster_status', 'credited_date',
                              'muster_no', 'finyear', 'block_code', 'name',
                       'relationship']
    muster_transactions_df = muster_transactions_df[muster_col_list]
    dataframe = libtech_queue_manager(logger, job_list, num_threads=500)
    if dataframe is not None:
        logger.info(dataframe.columns)
        ## In Muster HTML name and relationship appear in the same column.
        ## Separating them here below.
        rows_to_delete = []
        for index, row in dataframe.iterrows():
            sr_no = row.get("muster_index", None)
            if (sr_no is None) or (not sr_no.isdigit()):
                rows_to_delete.append(index)
            name_relationship = row['name_relationship']
            try:
                relationship = re.search(r'\((.*?)\)', name_relationship).group(1)
            except:
                relationship = ''
            name = name_relationship.replace(f"({relationship})", "")
            dataframe.loc[index, 'name'] = name
            dataframe.loc[index, 'relationship'] = relationship
        dataframe = dataframe.drop(rows_to_delete)
        logger.info(f"dataframe shape is {dataframe.shape}")
        logger.info(dataframe.columns)
        dataframe = pd.concat([dataframe, muster_transactions_df]) 
    else:
        logger.info("I am here since no musters to download")
        dataframe = muster_transactions_df
    logger.info(dataframe.shape)
    logger.info(muster_list_df.columns)
    dataframe = pd.merge(dataframe, muster_list_df, how='left',
                         on=['block_code', 'finyear', 'muster_no'])
    logger.info(f"dataframe shape is {dataframe.shape}")
    #logger.info(f"dataframe columns are {dataframe.columns}")
    drop_columns = ['caste', 'block_code']
    dataframe = dataframe.drop(columns=drop_columns)
    worker_df = worker_df.drop_duplicates(subset=['jobcard', 'name'],
                                          keep='last')
    dataframe = dataframe.merge(worker_df, how='left',
                         on=['jobcard', 'name'])
    logger.info(f"dataframe shape is {dataframe.shape}")
    dataframe = dataframe[col_list]
    logger.info(f"Length of data frame is {len(dataframe)}")
    dataframe1 = dataframe[dataframe['panchayat_code'] == int(lobj.code)]
    logger.info(f"Length of data frame is {len(dataframe1)}")
    return dataframe

def get_block_rejected_transactions_v2(lobj, logger, rej_stat_df):
    """This function will fetch all the block rejected Transactions"""
    worker_df= lobj.fetch_report_dataframe(logger, "worker_register")
    worker_df_cols = ["state_code", "state_name", "district_code", "district_name", "block_code", "block_name", "panchayat_code", "panchayat_name", "village_name", "caste", "head_of_household", "jobcard"]
    worker_df = worker_df[worker_df_cols]
    worker_df = worker_df.drop_duplicates()
    filtered_df = rej_stat_df[rej_stat_df['block_code'] == int(lobj.block_code)]
    start_fin_year = get_default_start_fin_year()
    logger.info(f"Shape of filtered_df is {filtered_df.shape}")
    filtered_df = filtered_df[filtered_df['finyear'] >= int(start_fin_year)]
    logger.info(f"Shape of filtered_df is {filtered_df.shape}")
    urls_to_download = []
    filtered_df = filtered_df.fillna('')
    for index, row in filtered_df.iterrows():
        finyear = row['finyear']
        fin_agency = row['fin_agency']
        url = row['rejected_url']
        nic_fin_year = row['finyear']
        if url != '':
            p = {}
            p['fin_agency'] = fin_agency
            p['url'] = url
            p['rejection_type'] = 'rejected'
            p['nic_fin_year'] = nic_fin_year
            urls_to_download.append(p)
        url = row['invalid_url']
        if url != '':
            p = {}
            p['fin_agency'] = fin_agency
            p['url'] = url
            p['rejection_type'] = 'invalid'
            p['nic_fin_year'] = nic_fin_year
            urls_to_download.append(p)
        logger.info(finyear)
    logger.info(len(urls_to_download))
    logger.info(urls_to_download)
    extract_dict = {}
    column_headers = ['srno', 'fto_no1', 'reference_no', 'reference_no_url',
                      'utr_no', 'transaction_date', 'name_eng',
                      'primary_account_holder', 'wagelist_no1', 'bank_code',
                      'ifsc_code', 'fto_amount', 'rejection_date',
                      'rejection_reason']
    extract_dict['pattern'] = 'UTR No'
    extract_dict['column_headers'] = column_headers
    extract_dict['extract_url_array'] = [2]
    extract_dict['url_prefix'] = f'http://mnregaweb4.nic.in/netnrega/FTO/'
    dataframe_array = []
    for p in urls_to_download:
        url = p['url']
        fin_agency = p['fin_agency']
        dataframe = get_dataframe_from_url(logger, url, mydict=extract_dict)
        if dataframe is not None:
            dataframe['fin_agency'] = fin_agency
            dataframe['nic_fin_year'] = p['nic_fin_year']
            dataframe['rejection_type'] = p['rejection_type']
            logger.info(f"size of dataframe for {fin_agency} {p['nic_fin_year']} is {len(dataframe)}")
            dataframe_array.append(dataframe)
    rejected_df = pd.concat(dataframe_array, ignore_index=True)
    column_headers = rejected_df.columns.to_list()
    logger.info(f"shape of dataframe after concat is {rejected_df.shape}")
    job_list = []
    func_name = "fetch_rejection_details_v2"
    for index, row in rejected_df.iterrows():
        url = row['reference_no_url']
        reference_no = row["reference_no"]
        func_args = [lobj, url, reference_no, row, column_headers]
        job_dict = {
            'func_name' : func_name,
            'func_args' : func_args
        }
        job_list.append(job_dict)
    dataframe = libtech_queue_manager(logger, job_list)
    all_cols = ['fto_fin_year', 'reference_no', 'reference_no_url', 'utr_no', 'transaction_date', 'name_eng', 'primary_account_holder', 'bank_code', 'ifsc_code', 'fto_amount', 'rejection_date', 'rejection_reason', 'fin_agency', 'nic_fin_year', 'rejection_type', 'wagelist_no',  'applicant_no', 'name', 'work_code', 'work_name', 'muster_no', 'reference_no', 'rejection_status', 'rejection_reason', 'process_date', 'fto_no', 'rejection_serial_no', 'final_reference_no', 'final_status', 'final_rejection_reason', 'final_process_date', 'final_fto_no', 'parent_reference_no', 'attempt_count', 'record_status']
    logger.info(f"dataframe shape is {dataframe.shape}")
    dataframe = pd.merge(dataframe, worker_df, how='left',
                         on=['jobcard'])
    all_cols = worker_df_cols + all_cols
    dataframe = dataframe[all_cols]
    return dataframe

def get_block_rejected_transactions(lobj, logger, rej_stat_df):
    """This function will fetch all theblock rejected transactions"""
    #As a first step we need to get all the list of jobcards for that block
    jobcard_df = lobj.fetch_report_dataframe(logger, "worker_register")
    logger.info(f"Shape of rej_df is {rej_stat_df.shape}")
    logger.info(f"Shape of rej_df is {rej_stat_df.columns}")
    filtered_df = rej_stat_df[rej_stat_df['block_code'] == int(lobj.block_code)]
    start_fin_year = get_default_start_fin_year()
    logger.info(f"Shape of filtered_df is {filtered_df.shape}")
    filtered_df = filtered_df[filtered_df['finyear'] > int(start_fin_year)]
    logger.info(f"Shape of filtered_df is {filtered_df.shape}")
    logger.info(start_fin_year)
    #As a second step we need to create array of urls that we need to download
    urls_to_download = []
    filtered_df = filtered_df.fillna('')
    for index, row in filtered_df.iterrows():
        finyear = row['finyear']
        fin_agency = row['fin_agency']
        url = row['rejected_url']
        if url != '':
            p = {}
            p['fin_agency'] = fin_agency
            p['url'] = url
            urls_to_download.append(p)
        url = row['invalid_url']
        if url != '':
            p = {}
            p['fin_agency'] = fin_agency
            p['url'] = url
            urls_to_download.append(p)
        logger.info(finyear)
    logger.info(len(urls_to_download))
    logger.info(urls_to_download)
   #url_report_types = ["NICRejectedTransactionsURL",
   #                    "NICRejectedTransactionsPostURL",
   #                    "NICRejectedTransactionsCoBankURL"
   #                   ]
   #start_fin_year = get_default_start_fin_year()
   #current_fin_year = get_current_finyear()
   #finyear_array = []
   #for finyear in range(start_fin_year, int(current_fin_year) +1):
   #    finyear_array.append(finyear)
   ###Now we will loop thorugh finyear and urls to create an array of urls that
   ###needs to be fetched
   #for url_report_type in url_report_types:
   #    for finyear in finyear_array:
   #        report_url = api_get_report_url(logger, lobj.id,
   #                                        url_report_type, finyear=finyear)
   #        if report_url is not None:
   #            urls_to_download.append(report_url)
   #logger.info(urls_to_download)

    extract_dict = {}
    column_headers = ['srno', 'fto_no', 'reference_no', 'reference_no_url',
                      'utr_no', 'transaction_date', 'name',
                      'primary_account_holder', 'wagelist_no', 'bank_code',
                      'ifsc_code', 'fto_amount', 'rejection_date',
                      'rejection_reason']
    extract_dict['pattern'] = 'UTR No'
    extract_dict['column_headers'] = column_headers
    extract_dict['extract_url_array'] = [2]
    extract_dict['url_prefix'] = f'http://mnregaweb4.nic.in/netnrega/FTO/'
    dataframe_array = []
    for p in urls_to_download:
        url = p['url']
        fin_agency = p['fin_agency']
        dataframe = get_dataframe_from_url(logger, url, mydict=extract_dict)
        if dataframe is not None:
            dataframe['fin_agency'] = fin_agency
            dataframe_array.append(dataframe)
    rejected_df = pd.concat(dataframe_array, ignore_index=True)
    logger.info(f"shape of dataframe is {dataframe.shape}")
    job_list = []
    func_name = "fetch_rejection_details"
    for index, row in rejected_df.iterrows():
        url = row['reference_no_url']
        func_args = [lobj, url]
        job_dict = {
            'func_name' : func_name,
            'func_args' : func_args
        }
        job_list.append(job_dict)
    dataframe = libtech_queue_manager(logger, job_list)
    logger.info(f"dataframe shape is {dataframe.shape}")
    dataframe = dataframe.drop_duplicates()
    logger.info(f"dataframe shape is {dataframe.shape}")
    column_headers = ["wagelist_no", "jobcard", "applicant_no", "name",
                      "work_code", "work_name", "muster_no", "reference_no",
                      "rejection_status", "rejection_reason", "process_date", "fto_no",
                      "rejection_serial_no", 'attempt_count', 'record_status',
                      'fto_finyear']
    dataframe = dataframe[column_headers]
    rejected_df_columns = ['reference_no', 'reference_no_url', 'utr_no',
                           'transaction_date', 'primary_account_holder',
                           'bank_code', 'ifsc_code', 'fto_amount',
                           'rejection_date', 'fin_agency']
    rejected_df = rejected_df[rejected_df_columns]
    dataframe = pd.merge(dataframe, rejected_df, how='left',
                         on=['reference_no'])
    logger.info(f"dataframe shape is {dataframe.shape}")
    dataframe = pd.merge(dataframe, jobcard_df, how='left',
                         on=['jobcard', 'name'])
    col_list = ['state_code', 'state_name', 'district_code', 'district_name',
                'block_code', 'block_name', 'panchayat_name', 'panchayat_code',
                'village_name', 'jobcard', 'applicant_no', 'name',
                'head_of_household', 'caste', 'IAY_LR', 'father_husband_name',
                'gender', 'age', 'jobcard_request_date', 'jobcard_issue_date', 'jobcard_remarks',
                'disabled', 'minority', 'jobcard_verification_date',
                'fin_agency', 'work_code',
                'work_name', 'muster_no', 'attempt_count', 'record_status',
                'wagelist_no', 'fto_no', 'fto_finyear', 'fto_amount',
                'transaction_date', 'process_date', 'reference_no',
                'rejection_status', 'rejection_reason', 'rejection_serial_no',
                'utr_no', 'primary_account_holder', 'bank_code', 'ifsc_code']
    dataframe = dataframe[col_list]
    logger.info(f"dataframe shape is {dataframe.shape}")
    return dataframe
def get_block_rejected_stats(lobj, logger, fto_status_df):
    """This function will get block Rejected Stats"""
    logger.info(f"Block Level Rejection Stats for {lobj.code}")
    current_df = lobj.fetch_report_dataframe(logger, "block_rejected_stats")
    logger.info(f"Shape of current df is {current_df.shape}")
    current_finyear = get_current_finyear()
    filtered_df = current_df[current_df['finyear'] != current_finyear]
    logger.info(f"Shape of filtered df is {filtered_df.shape}")
    ##Calculate the current finyear urls to download
    filtered_fto_status_df = fto_status_df[fto_status_df['finyear'] ==
                                           current_finyear]
    logger.info(f"shape of fto status is {fto_status_df.shape}")
    logger.info(f"shape of filtered status is {filtered_fto_status_df.shape}")
    job_list = []
    column_headers = ["srno", "block", "total_fto_generated", "fs_fto_signed",
                      "fs_fto_pending", "ss_fto_signed",
                      "second_singnatory_fto_url", "ss_fto_pending",
                      "sent_to_bank", "sent_to_bank_transactions",
                      "processed_by_bank", "process_by_bank_transactions",
                      "partial_processed", "partial_processed_transactions",
                      "partial_processed_pending", "pending_for_processing",
                      "pending_from_processing_transactions",
                      "processed", "invalid", "invalid_url",
                      "rejected", "rejected_url",
                      "total"]
    url_prefix = "http://mnregaweb4.nic.in/netnrega/FTO/"
    extract_dict = {}
    extract_dict['pattern'] = f"Second Signatory"
    extract_dict['column_headers'] = column_headers
    extract_dict['data_start_row'] = 4
    extract_dict['extract_url_array'] = [5, 17, 18]
    extract_dict['url_prefix'] = url_prefix
    func_name = "fetch_rejected_stats"
    for index, row in filtered_fto_status_df.iterrows():
        func_args = []
        url = row.get("dist_url", None)
        finyear = row.get("finyear", None)
        logger.info(f"finyear {finyear} url {url}")
        if url is not None:
            func_args = [lobj, url, finyear, extract_dict]
            job_dict = {
                'func_name' : func_name,
                'func_args' : func_args
            }
            job_list.append(job_dict)
    dataframe = libtech_queue_manager(logger, job_list, num_threads=25)
    concat_df = pd.concat([filtered_df, dataframe])
    return concat_df

def get_nic_r4_1_urls(lobj, logger, report_type=None, url_text=None,
                      url_prefix=None):
    """This function will get the Urls at the block level"""
    state_pattern = f"state_code={lobj.state_code}"
    logger.info(f"Getting URLs from MIS Reports for {report_type} and pattern{url_text}")
    current_df = lobj.fetch_report_dataframe(logger, report_type)
    filtered_df = None
   #if current_df is not None:
   #  logger.info(f"Shape of current df is {current_df.shape}")
   #  current_finyear = get_current_finyear()
   #  filtered_df = current_df[current_df['finyear'] != current_finyear]
   #  logger.info(f"Shape of filtered df is {filtered_df.shape}")

    csv_array = []
    column_headers = ["state_code", "district_code", "block_code", "state_name",
                      "district_name", "block_name", "finyear", "url"]
    start_finyear = get_default_start_fin_year()
    end_finyear = get_current_finyear()
    for finyear in range(int(start_finyear), int(end_finyear)+1):
        logger.info(f"Downloading for FinYear {finyear}")
        filename = f"{NREGA_DATA_DIR}/misReport_{finyear}.html"
        logger.info(filename)
        with open(filename, "rb") as infile:
            myhtml = infile.read()
        mysoup = BeautifulSoup(myhtml, "lxml")
        elem = mysoup.find("a", href=re.compile(url_text))
        if elem is not None:
            base_href = elem["href"]
        logger.info(base_href)
        res = requests.get(base_href)
        myhtml = None
        if res.status_code == 200:
            myhtml = res.content
        if myhtml is not None:
            mysoup = BeautifulSoup(myhtml, "lxml")
            elems = mysoup.find_all("a", href=re.compile(url_text))
            for elem in elems:
                logger.info(elem)
                state_href = elem["href"]
                logger.info(f"State URL is {state_href}")
                if state_pattern not in state_href:
                    continue
                url = url_prefix + state_href
                response = requests.get(url)
                logger.info(url)
                dist_html = None
                if response.status_code == 200:
                    dist_html = response.content
                if dist_html is not None:
                    dist_soup = BeautifulSoup(dist_html, "lxml")
                    elems = dist_soup.find_all("a", href=re.compile(url_text))
                    for elem1 in elems:
                        dist_url = url_prefix + elem1["href"]
                        block_res = requests.get(dist_url)
                        if block_res.status_code == 200:
                            block_html = block_res.content
                            block_soup = BeautifulSoup(block_html, "lxml")
                            belems = block_soup.find_all("a", href=re.compile(url_text))
                            for belem in belems:
                                block_url = url_prefix + belem["href"]
                                #logger.info(dist_url)
                                parsed = urlparse.urlparse(block_url)
                                params_dict = parse_qs(parsed.query)
                                #logger.info(params_dict)
                                state_name = params_dict.get("state_name", [''])[0]
                                state_code = params_dict.get("state_code", [""])[0]
                                district_name = params_dict.get("district_name",
                                                                [""])[0]
                                district_code = params_dict.get("district_code",
                                                                [""])[0]
                                block_name = params_dict.get("block_name",
                                                                [""])[0]
                                block_code = params_dict.get("block_code",
                                                                [""])[0]
                                row = [state_code, district_code, block_code, state_name,
                                       district_name, block_name, finyear, block_url]
                                logger.info(row)
                                csv_array.append(row)

    dataframe = pd.DataFrame(csv_array, columns=column_headers)
    if filtered_df is not None:
        dataframe = pd.concat([filtered_df, dataframe])
    return dataframe


def get_fto_status_urls(lobj, logger):
    """This function will download the block level Rejection Stats"""
    logger.info(f"Block Level Rejection Stats for {lobj.code}")
    current_df = lobj.fetch_report_dataframe(logger, "fto_status_urls")
    logger.info(f"Shape of current df is {current_df.shape}")
    current_finyear = get_current_finyear()
    filtered_df = current_df[current_df['finyear'] != current_finyear]
    logger.info(f"Shape of filtered df is {filtered_df.shape}")

    csv_array = []
    column_headers = ["state_code", "district_code", "state_name",
                      "district_name", "finyear", "dist_url"]
    start_finyear = get_current_finyear()
    end_finyear = get_current_finyear()
    url_prefix = "http://mnregaweb4.nic.in/netnrega/FTO/"
    for finyear in range(int(start_finyear), int(end_finyear)+1):
        logger.info(f"Downloading for FinYear {finyear}")
        filename = f"{NREGA_DATA_DIR}/misReport_{finyear}.html"
        logger.info(filename)
        with open(filename, "rb") as infile:
            myhtml = infile.read()
        mysoup = BeautifulSoup(myhtml, "lxml")
        elem = mysoup.find("a", href=re.compile("FTOReport.aspx"))
        if elem is not None:
            base_href = elem["href"]
        logger.info(base_href)
        res = requests.get(base_href)
        myhtml = None
        if res.status_code == 200:
            myhtml = res.content
        if myhtml is not None:
            mysoup = BeautifulSoup(myhtml, "lxml")
            elems = mysoup.find_all("a", href=re.compile("FTOReport.aspx"))
            for elem in elems:
                logger.info(elem)
                state_href = elem["href"]
                logger.info(state_href)
                url = url_prefix + state_href
                response = requests.get(url)
                logger.info(url)
                dist_html = None
                if response.status_code == 200:
                    dist_html = response.content
                if dist_html is not None:
                    dist_soup = BeautifulSoup(dist_html, "lxml")
                    elems = dist_soup.find_all("a", href=re.compile("FTOReport.aspx"))
                    for elem1 in elems:
                        dist_url = url_prefix + elem1["href"]
                        #logger.info(dist_url)
                        parsed = urlparse.urlparse(dist_url)
                        params_dict = parse_qs(parsed.query)
                        #logger.info(params_dict)
                        state_name = params_dict.get("state_name", [''])[0]
                        state_code = params_dict.get("state_code", [""])[0]
                        district_name = params_dict.get("district_name",
                                                        [""])[0]
                        district_code = params_dict.get("district_code",
                                                        [""])[0]
                        row = [state_code, district_code, state_name,
                               district_name, finyear, dist_url]
                        csv_array.append(row)

    dataframe = pd.DataFrame(csv_array, columns=column_headers)
    concat_df = pd.concat([filtered_df, dataframe])
    return concat_df


def get_nic_stat_urls(lobj, logger, panchayat_code_array):
    dataframe = None
    csv_array = []
    column_headers = ["state_code", "district_code", "block_code",
                      "panchayat_code", "location_code", "location_type", "stats_url"]
    urlPrefix = "http://mnregaweb4.nic.in/netnrega/"
    url = "http://mnregaweb4.nic.in/netnrega/all_lvl_details_dashboard_new.aspx"
    headers  =  {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q = 0.9,*/*;q = 0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-GB,en;q = 0.5',
    'Connection': 'keep-alive',
    'Host': 'mnregaweb4.nic.in',
    'Referer': url,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Content-Type': 'application/x-www-form-urlencoded',
    }
    r = requests.get(url)
    if r.status_code  ==  200:
        myhtml = r.content
        htmlsoup = BeautifulSoup(myhtml,"lxml")
        validation  =  htmlsoup.find(id = '__EVENTVALIDATION').get('value')
        view_state  =  htmlsoup.find(id = '__VIEWSTATE').get('value')
        data  =  {
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': view_state,
            '__VIEWSTATEENCRYPTED': '',
            '__EVENTVALIDATION': validation,
            'ddl_state': lobj.state_code,
            '__EVENTTARGET': 'ddl_state',
        }
    
        response  =  request_with_retry_timeout(logger,url, headers=headers, data=data)
        if response.status_code == 200:
            myhtml = response.content
        else:
            myhtml = None
        if myhtml is not None:
            htmlsoup = BeautifulSoup(myhtml,"lxml")
            validation  =  htmlsoup.find(id = '__EVENTVALIDATION').get('value')
            view_state  =  htmlsoup.find(id = '__VIEWSTATE').get('value')
            data['ddl_dist'] = lobj.district_code
            data['__EVENTTARGET'] = 'ddl_dist'
            data['__VIEWSTATE'] =  view_state
            data['__EVENTVALIDATION'] =  validation

            response  =  request_with_retry_timeout(logger ,url, headers=headers, data=data)
            if response.status_code == 200:
                myhtml = response.content
            else:
                myhtml = None
            if myhtml is not None:
                htmlsoup = BeautifulSoup(myhtml,"lxml")
                validation = htmlsoup.find(id = '__EVENTVALIDATION').get('value')
                view_state = htmlsoup.find(id = '__VIEWSTATE').get('value')
                data['ddl_blk'] = lobj.block_code
                data['__EVENTTARGET'] = 'ddl_blk'
                data['__VIEWSTATE'] =  view_state
                data['__EVENTVALIDATION'] =  validation
                response  =  request_with_retry_timeout(logger, url,headers=headers, data=data)
                if response.status_code == 200:
                    myhtml = response.content
                else:
                    myhtml = None
                if myhtml is not None:
                    htmlsoup = BeautifulSoup(myhtml,"lxml")
                    validation = htmlsoup.find(id = '__EVENTVALIDATION').get('value')
                    view_state = htmlsoup.find(id = '__VIEWSTATE').get('value')
                    panchayat_code_array.append(lobj.block_code)
                    for location_code in panchayat_code_array:
                        stats_url = None
                        if location_code  ==  lobj.block_code:
                            location_code_string = 'ALL'
                            panchayat_code = ''
                            location_type = 'block'
                        else:
                            location_code_string = location_code
                            panchayat_code = location_code
                            location_type = 'panchayat'
                        data  =  {
                          '__EVENTTARGET': '',
                          '__EVENTARGUMENT': '',
                          '__LASTFOCUS': '',
                          '__VIEWSTATE': view_state,
                          '__VIEWSTATEENCRYPTED': '',
                          '__EVENTVALIDATION': validation,
                          'ddl_state': lobj.state_code,
                          'ddl_dist' : lobj.district_code,
                          'ddl_blk' : lobj.block_code,
                          'ddl_pan' : location_code_string,
                          'btproceed' : 'View Detail'
                        }
                        response  =  request_with_retry_timeout(logger, url, headers=headers, data=data)
                        if response.status_code == 200:
                            myhtml = response.content
                        else:
                            myhtml = None
                        if myhtml is not None:
                            htmlsoup = BeautifulSoup(myhtml,"lxml")
                            myi_frame = htmlsoup.find("iframe")
                            if myi_frame is not None:
                                stats_url = urlPrefix+myi_frame['src']
                                #logger.info(stats_url)
                                row = [lobj.state_code, lobj.district_code,
                                       lobj.block_code, panchayat_code,
                                       location_code, location_type, stats_url] 
                                csv_array.append(row)
                        if stats_url is None:
                            exception_message = f"Unable to get stats for {location_code_string} and block_code {lobj.block_code}"
                            raise Exception('x should not exceed 5. The value of xwas:')
    dataframe = pd.DataFrame(csv_array, columns=column_headers)
    return dataframe


def get_nic_stats(lobj, logger, nic_stat_urls_df):
    """This function will fetch nic Stats"""
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    transpose_columns = ["stat", "finyear", "value"]
    all_cols = location_cols + transpose_columns
    empty_dataframe = pd.DataFrame(columns=all_cols)
    logger.info(f"Going to fetch nic Stats for {lobj.code}-{lobj.name}")
    logger.debug(nic_stat_urls_df.columns)
    filtered_df = nic_stat_urls_df[nic_stat_urls_df['location_code'] ==
                                   int(lobj.code)].reset_index()
    logger.debug(f"length of filtered_df is {len(filtered_df)}")
    if len(filtered_df) != 1:
        return None
    stats_url = filtered_df.loc[0, "stats_url"]
    logger.debug(stats_url)
    #res = requests.get(stats_url)
    res = request_with_retry_timeout(logger, stats_url, method="get")
    #res = requests.get(stats_url)
    if res is None:
        return empty_dataframe
    myhtml = res.content
    extract_dict = {}
    extract_dict['table_id'] = 'GridView1'
    finyear = int(get_current_finyear())
    fin_array = [finyear, finyear-1, finyear-2, finyear-3, finyear-4]
    column_headers = ['name'] + fin_array + ['graphs']
    extract_dict['column_headers'] = column_headers
    dataframe = get_dataframe_from_html(logger, myhtml,
                                        mydict=extract_dict)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        if lobj.location_type == "block":
            dataframe["panchayat_code"] = ''
            dataframe["panchayat_name"] = ''
        return dataframe
    return empty_dataframe
    ##Now we will convert this dataframe to JSON
    ignored_rows = ["I             Job Card", "II             Progress",
                    "III             Works",
                    "IV             Financial Progress"
                   ]
    stat_dict = {}
    finyear_wise = False
    csv_array = []
    for index, row in dataframe.iterrows():
        name = row.get("name", "")
        if name == "II             Progress":
            finyear_wise = True
        if name in ignored_rows:
            continue
        if not finyear_wise:
            stat_dict[slugify(name)] = row.get(finyear, 0)
            row1 = [slugify(name), finyear, row.get(finyear, 0)]
            csv_array.append(row1)
        else:
            fin_stat = {}
            for each_year in fin_array:
                fin_stat[each_year] = row.get(each_year, 0)
                row1 = [slugify(name), each_year, row.get(each_year, 0)]
                csv_array.append(row1)
            stat_dict[slugify(name)] = fin_stat
    #data_json = lobj.data_json
    #data_json["at_a_glance"] = stat_dict
    #api_location_update(logger, lobj.id, data_json)
    if len(csv_array) > 0:
        dataframe = pd.DataFrame(csv_array, columns=transpose_columns)
        dataframe = insert_location_details(logger, lobj, dataframe)
        if lobj.location_type == "block":
            dataframe["panchayat_code"] = ''
            dataframe["panchayat_name"] = ''
        dataframe = dataframe[all_cols]
        return dataframe
    else:
        return empty_dataframe
        
def get_nic_urls(lobj, logger):
    """This will get important nic URLs from the panchayat page"""
    csv_array = []
    logger.info(f"{lobj.state_name}-{lobj.district_name}-{lobj.block_name}-{lobj.panchayat_name}")
    panchayat_page_url = (f"https://{lobj.crawl_ip}/netnrega/IndexFrame.aspx?"
                              f"lflag=eng&District_Code={lobj.state_code}&"
                              f"district_name={lobj.district_name}"
                              f"&state_name={lobj.state_name}"
                              f"&state_Code={lobj.state_code}&block_name={lobj.block_name}"
                              f"&block_code={lobj.block_code}&fin_year=fullFinYear"
                              f"&check=1&Panchayat_name={lobj.panchayat_name}"
                              f"&Panchayat_Code={lobj.panchayat_code}")
    start_fin_year = get_default_start_fin_year()
    end_fin_year = get_current_finyear()
    column_headers = ['finyear', 'report_name', 'report_slug',
                      'state_url', 'mis_url']
    for finyear in range(start_fin_year, end_fin_year+1):
        logger.debug(f"Currently Processing {finyear} for {lobj.code}")
        finyear = str(finyear)
        full_finyear = get_full_finyear(finyear)
        base_url = panchayat_page_url.replace("fullFinYear", full_finyear)
        res = get_request_with_retry_timeout(logger, base_url)
        if res is None:
            return None
        myhtml = res.content
        mysoup = BeautifulSoup(myhtml, "lxml")
        links = mysoup.findAll("a")
        url_prefix = f"https://{lobj.crawl_ip}/netnrega/"
        mis_url_prefix = f"https://mnregaweb4.nic.in/netnrega/"
        for link in links:
            href = link.get("href", "")
            text = link.text
            state_url = url_prefix + href
            mis_url = mis_url_prefix + href
            row = [finyear, text, slugify(text), state_url, mis_url]
            csv_array.append(row)
    dataframe = pd.DataFrame(csv_array, columns=column_headers)
    dataframe = insert_location_details(logger, lobj, dataframe)
    return dataframe

def scrape_muster_list(logger, lobj, finyear, url, cookies=None):
    column_headers = ["finyear", "work_code",
                      "muster_no", "from_date", "to_date",
                      "muster_value", "work_name"]
    csv_array = []
    full_finyear = get_full_finyear(finyear)
    r = request_with_retry_timeout(logger, url, method="get")
    if r is None:
        return None
    cookies = r.cookies
    logger.debug(cookies)
    myhtml = r.content
    mysoup = BeautifulSoup(myhtml, "lxml")
    validation  =  mysoup.find(id = '__EVENTVALIDATION').get('value')
    view_state  =  mysoup.find(id = '__VIEWSTATE').get('value')
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'X-MicrosoftAjax': 'Delta=true',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': '*/*',
        'Origin': 'https://mnregaweb4.nic.in',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer' : url,
      #  'Referer': 'https://mnregaweb4.nic.in/netnrega/Citizen_html/Musternew.aspx?id=2&lflag=eng&ExeL=GP&fin_year=2019-2020&state_code=27&district_code=27&block_code=2724007&panchayat_code=2724007283&State_name=RAJASTHAN&District_name=BHILWARA&Block_name=SAHADA&panchayat_name=%e0%a4%85%e0%a4%b0%e0%a4%a8%e0%a4%bf%e0%a4%af%e0%a4%be+%e0%a4%96%e0%a4%be%e0%a4%b2%e0%a4%b8%e0%a4%be&Digest=NV/nIrrL5cMS/YBl64Zfhg',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    params = (
        ('id', '2'),
        ('lflag', 'eng'),
        ('ExeL', 'GP'),
        ('fin_year', '2019-2020'),
        ('state_code', '27'),
        ('district_code', '27'),
        ('block_code', '2724007'),
        ('panchayat_code', '2724007283'),
        ('State_name', 'RAJASTHAN'),
        ('District_name', 'BHILWARA'),
        ('Block_name', 'SAHADA'),
        ('panchayat_name', '%u0905%u0930%u0928%u093f%u092f%u093e %u0916%u093e%u0932%u0938%u093e'),
        ('Digest', 'NV/nIrrL5cMS/YBl64Zfhg'),
    )
    
    data = {
      'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$ScriptManager1|ctl00$ContentPlaceHolder1$ddlwork',
      '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlwork',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': view_state,
      '__VIEWSTATEGENERATOR': '75DEE431',
      '__VIEWSTATEENCRYPTED': '',
      '__EVENTVALIDATION': validation,
      'ctl00$ContentPlaceHolder1$ddlFinYear': full_finyear,
      'ctl00$ContentPlaceHolder1$btnfill': 'btnfill',
      'ctl00$ContentPlaceHolder1$txtSearch': '',
      'ctl00$ContentPlaceHolder1$ddlwork': '2724007/RC/112908174689',
      'ctl00$ContentPlaceHolder1$ddlMsrno': '---select---',
      '__ASYNCPOST': 'true',
      '': ''
    }
    

    work_select_id = 'ctl00_ContentPlaceHolder1_ddlwork'
    muster_select_id = 'ctl00_ContentPlaceHolder1_ddlMsrno'
    options_list = get_options_list(logger, mysoup, select_id=work_select_id) 
    for option in options_list:
        work_code = option["value"]
        work_name = option["name"]
        if('---select---' in work_code):
            logger.debug(f'Skipping muster_no[{work_code}]')
            continue
        data["ctl00$ContentPlaceHolder1$ddlwork"] = work_code
        logger.debug(f"processing work_code {work_code}")
        response = request_with_retry_timeout(logger, url, headers=headers,
                                              cookies=cookies, data=data)
        if response is None:
            continue
        if response.status_code == 200:
            htmlsoup = BeautifulSoup(response.content, "lxml")
            muster_options_list = get_options_list(logger, htmlsoup,
                                                   select_id=muster_select_id) 
            for muster_option in muster_options_list:
                value = muster_option["value"]
                if('---Select---' in value):
                    logger.debug(f'Skipping muster_no[{value}]')
                    continue
                value_array = value.split("~~")
                if len(value_array) == 3:
                    muster_no = value_array[0]
                    from_date = value_array[1]
                    to_date = value_array[2]
                else:
                    muster_no = ''
                    from_date = ''
                    to_date = ''
                logger.debug(f"found muster {value}")
                row = [finyear, work_code, muster_no,
                       from_date, to_date, value, work_name]
                csv_array.append(row)
    if len(csv_array) > 0:
        dataframe = pd.DataFrame(csv_array, columns=column_headers)
    else:
        dataframe = None
    return dataframe
    

def fetch_muster_list(lobj, logger, nic_urls_df):
    """This will fetch the muster list according to the new URL available"""
    logger.info(f"In Fetch muster list for {lobj.panchayat_code}")
    logger.info(f"Shape of df is {nic_urls_df.shape}")
    filtered_df = nic_urls_df[(nic_urls_df["report_slug"] == "muster-roll") &
                              (nic_urls_df["panchayat_code"] == int(lobj.panchayat_code))]
    logger.debug(f"Filtered DF shape is {filtered_df.shape}")
    ##Establish session for request
    session = requests.Session()
    session.get(lobj.mis_state_url)
    response = request_with_retry_timeout(logger, lobj.mis_state_url,
                                          method="get")
    if response is None:
        return None
    cookies = response.cookies
    logger.debug(f"session cookies {cookies}")
    df_array = []
    for index, row in filtered_df.iterrows():
        nic_url = row.get("mis_url")
        finyear = row.get("finyear")
        logger.debug(f"Processing finyear {finyear} url {nic_url}")
        dataframe = scrape_muster_list(logger, lobj, finyear, nic_url,
                                       cookies=cookies)
        if dataframe is not None:
            df_array.append(dataframe)
    if len(df_array) == 0:
        return None
    dataframe = pd.concat(df_array)
    dataframe = insert_location_details(logger, lobj, dataframe)
    return dataframe

def get_data_accuracy(lobj, logger, muster_transactions_df, nic_stats_df):
    """This function will calculate data accuracy for last 3 financial years"""
    accuracy = None
    start_finyear = get_default_start_fin_year()
    end_finyear = get_current_finyear()
    work_days_df = nic_stats_df[nic_stats_df['name'] == 'Persondays Generated so far'].reset_index()
    if len(work_days_df) != 1:
        return accuracy 
    nic_stat_row = work_days_df.loc[0]
    logger.info(muster_transactions_df.columns)
    libtech_total_workdays = 0
    nic_total_workdays = 0
    for finyear in range(int(start_finyear), int(end_finyear)+1):
        filtered_transactions_df = muster_transactions_df[muster_transactions_df['finyear'] == finyear]
        #filtered_transactions_df = filtered_transactions_df[filtered_transactions_df['panchayat_code'] == int(lobj.code)]
        libtech_workdays = filtered_transactions_df.days_worked.sum()
        nic_workdays = int(nic_stat_row.get(str(finyear)).replace(",",""))
        logger.info(f"Libtech {libtech_workdays} - NIC {nic_workdays}")
        libtech_total_workdays = libtech_total_workdays + libtech_workdays
        nic_total_workdays = nic_total_workdays + nic_workdays
    accuracy = get_percentage(nic_total_workdays, libtech_total_workdays,
                              round_digits=2)
    logger.info(f"Accuracy is {accuracy}")
    return accuracy 

def get_ap_worker_register(lobj, logger):
    finyear = get_current_finyear()
    fullfinyear = get_full_finyear(finyear)
    block_url = f"http://mnregaweb4.nic.in/netnrega/Progofficer/PoIndexFrame.aspx?flag_debited=N&lflag=eng&District_Code={lobj.district_code}&district_name={lobj.district_name}&state_name={lobj.state_name}&state_Code={lobj.state_code}&finyear={fullfinyear}&check=1&block_name={lobj.block_name}&Block_Code={lobj.block_code}"
    logger.info(f"block url is {block_url}")
    res = requests.get(block_url)
    if res.status_code != 200:
        return None
    myhtml = res.content
    cookies = res.cookies
    logger.info(f"cookies is {cookies}")
    worker_register_identifier = "blockregpeople.aspx"
    url_prefix = "http://mnregaweb4.nic.in/netnrega/placeholder/"
    url = find_url_containing_text(myhtml, worker_register_identifier,
                                   url_prefix=url_prefix)
    logger.info(f"url is {url}")
    res = requests.get(url, cookies=cookies)
    if res.status_code != 200:
        return None
    myhtml = res.content
    worker_register_identifier = lobj.panchayat_code
    url_prefix = "http://mnregaweb4.nic.in/netnrega/placeholder1/placeholder2/"
    url = find_url_containing_text(myhtml, worker_register_identifier,
                                   url_prefix=url_prefix)
    logger.info(f"worker url is {url}")
    dataframe = get_worker_register(lobj, logger, worker_reg_url=url,
                                    cookies=cookies)
    return dataframe

def fetch_jobcard_stats(this, logger, lobj, url):
    logger.info(f'fetch_jobcard_stats(..., lobj.panchayat_code={lobj.panchayat_code}, url={url}])')

    # Block Drop Down
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'X-MicrosoftAjax': 'Delta=true',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.142 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': '*/*',
        'Origin': 'http://nregasp2.nic.in',
        'Referer': url,
        'Accept-Language': 'en-US,en;q=0.9',
    }

    params = (
        ('lflag', 'local'),
        ('page', 'B'),
        ('state_name', lobj.state_name),
        ('state_code', lobj.state_code),
        ('block_name', lobj.block_name),
        ('block_code', lobj.block_code),
        ('district_name', lobj.district_name),
        ('panchayat_code', lobj.panchayat_code),
        ('panchayat_name', lobj.panchayat_name),
        ('district_code', lobj.district_code),
        ('fin_year', this['finyear']),
        ('source', ''),
        ('Digest', url.split('&')[-1].strip('Digest=')),
    )

    data = {
      'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddr_blk',
      '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddr_blk',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': this['view_state'],
      '__EVENTVALIDATION': this['event_validation'],
      'ctl00$ContentPlaceHolder1$ddr_blk': lobj.block_code,
      'ctl00$ContentPlaceHolder1$ddr_panch': '--Select--',
      'ctl00$ContentPlaceHolder1$ddr_cond': '',
      'ctl00$ContentPlaceHolder1$lbl_days': '100',
      'ctl00$ContentPlaceHolder1$rblRegWorker': 'Y',
      '__ASYNCPOST': 'true',
      '': ''
    }

    # FIXME - Mynk this should work - Confirmed multiple times it does not
    # response = this['session'].post(url, data=data, verify=False)
    response = this['session'].post('http://nregasp2.nic.in/netnrega/state_html/empspecifydays.aspx', headers=headers, params=params, cookies=this['cookies'], data=data, verify=False)

    soup = BeautifulSoup(response.content, 'lxml')
    # logger.debug(soup)

    # Update the context
    body = soup.find('body')
    #logger.warning(body.text)
    array = body.text.split('|')
    view_state = array[array.index('__VIEWSTATE')+1]
    #logger.debug(self.view_state)
    event_validation = array[array.index('__EVENTVALIDATION')+1]
    #logger.debug(self.event_validation)

    # Panchayat Drop Down
    data['ctl00$ContentPlaceHolder1$ScriptManager1'] = 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddr_panch'
    data['ctl00$ContentPlaceHolder1$ddr_panch'] = lobj.panchayat_code
    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ddr_panch'
    data['__VIEWSTATE'] = view_state
    data['__EVENTVALIDATION'] = event_validation,

    response = this['session'].post('http://nregasp2.nic.in/netnrega/state_html/empspecifydays.aspx', headers=headers, params=params, cookies=this['cookies'], data=data, verify=False)
    response
    soup = BeautifulSoup(response.text, 'lxml')
    #print(soup.prettify())

    # Update the context
    body = soup.find('body')
    #logger.warning(body.text)
    array = body.text.split('|')
    view_state = array[array.index('__VIEWSTATE')+1]
    #logger.debug(self.view_state)
    event_validation = array[array.index('__EVENTVALIDATION')+1]
    #logger.debug(self.event_validation)

    # With gt 0 and worker wise
    data['ctl00$ContentPlaceHolder1$ScriptManager1'] = 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddr_cond'
    data['ctl00$ContentPlaceHolder1$ddr_cond'] = 'gt'
    data['ctl00$ContentPlaceHolder1$lbl_days'] = '0'
    data['ctl00$ContentPlaceHolder1$rblRegWorker'] = 'N'
    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ddr_cond'
    data['__VIEWSTATE'] = view_state
    data['__EVENTVALIDATION'] = event_validation
    response = this['session'].post('http://nregasp2.nic.in/netnrega/state_html/empspecifydays.aspx', headers=headers, params=params, cookies=this['cookies'], data=data, verify=False)
    soup = BeautifulSoup(response.text, 'lxml')
    #print(soup.prettify())

    # Update the context
    body = soup.find('body')
    #logger.warning(body.text)
    array = body.text.split('|')
    view_state = array[array.index('__VIEWSTATE')+1]
    #logger.debug(self.view_state)
    event_validation = array[array.index('__EVENTVALIDATION')+1]
    #logger.debug(self.event_validation)

    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$btn_pro'
    data['ctl00$ContentPlaceHolder1$ScriptManager1'] = 'ctl00$ContentPlaceHolder1$UpdatePanel1|' + data['__EVENTTARGET']
    data['__VIEWSTATE'] = view_state
    data['__EVENTVALIDATION'] = event_validation

    response = this['session'].post('http://nregasp2.nic.in/netnrega/state_html/empspecifydays.aspx', headers=headers, params=params, cookies=this['cookies'], data=data, verify=False)

    # Fetch the redirect URL

    soup = BeautifulSoup(response.text, 'lxml')
    # print(soup.prettify())

    # Fetch the redirect URL
    body = soup.find('body')
    p = body.find('p')
    array = p.text.split('|')
    redirect_url = array[array.index('pageRedirect')+2]
    url = 'http://nregasp2.nic.in' + redirect_url
    logger.info(f'Final Redirect URL[{url}]')

    # Fetch the report :)

    response = this['session'].get(url, verify=False)

    # soup = BeautifulSoup(response.text, 'lxml')
    print(soup.prettify())

    return response

def get_jobcard_stats(lobj, logger):
    logger.info(f'Fetching Jobcard Stats for lobj[{lobj.code}]')
    start_fin_year = get_default_start_fin_year()
    end_fin_year = get_current_finyear()
    extract_dict = {}
    column_headers = ['srno', 'jobcard', 'head_of_household', 'worker_name', 'no_of_person_days']
    extract_dict['column_headers'] = column_headers
    extract_dict['table_id'] = 'ctl00_ContentPlaceHolder1_GridView1'
    extract_dict['data_start_row'] = 2

    dataframe = None
    dfs = []

    # Set up the Session
    this = {}    # Equivalent of self/context. Pending class use.
    url = 'https://nrega.nic.in/netnrega/home.aspx'  # put under lobj.home_url or base_url?
    with requests.Session() as session:
        this['session'] = session
        response = session.get(url)
        this['cookies'] = session.cookies # May not be needed
        logger.info(this['cookies'])

        for finyear in range(20, 22): # FIXME - does not work for 17 & 18 range(start_fin_year, end_fin_year+1):
            full_finyear = get_full_finyear(finyear)
            this['finyear'] = full_finyear
            logger.info(f'Inside Financial Year[{full_finyear}]')
            #url = http://nregasp2.nic.in/netnrega/Progofficer/PoIndexFrame.aspx?flag_debited=D&lflag=local&District_Code=3403&district_name=GUMLA&state_name=JHARKHAND&state_Code=34&finyear=2020-2021&check=1&block_name=BASIA&Block_Code=3403009
# Fetch the block level URL with desired finyear
            '''
            this.district_code = '3403'
            this.district_name = 'GUMLA'
            this.state_name = 'JHARKHAND'
            this.state_code = '34'
            this.finyear = '2020-2021'
            this.block_name = 'BASIA'
            this.block_code = '3403009'
            this.panchayat_code = '3403009003'
            this.panchayat_name = 'EITAM'
            '''
            url = f'http://nregasp2.nic.in/netnrega/Progofficer/PoIndexFrame.aspx?flag_debited=D&lflag=local&\
District_Code={lobj.district_code}&district_name={lobj.district_name}&\
state_name={lobj.state_name}&state_Code={lobj.state_code}&\
finyear={full_finyear}&check=1&\
block_name={lobj.block_name}&Block_Code={lobj.block_code}'
            logger.info(f'Fetch URL[{url}] for all panchayats')
            this['url'] = url

            response = session.get(url)
            soup = BeautifulSoup(response.text, 'lxml')

            # Fetch Employment Offered Report URL

            try:
                html = soup.find(text='Employment Provided Period wise ')
                url = 'http://nregasp2.nic.in/netnrega/state_html/pmsr.aspx' + html.parent.parent['href'].strip('../state_html/pmsr.aspx')
                logger.info(f'Employment Offered URL[{url}]')

                response = session.get(url, verify=False)
            except Exception as e:
                logger.error(f'Error occured Exception[{e}]')
                return None
            '''
            soup = BeautifulSoup(response.text, 'lxml')

            anchors = soup.find_all('a')

            for anchor in anchors:
                url = anchor['href']
                if 'empspecifydays' not in url:
                    continue
                print(f'URL[{url}]')
                url = 'http://nregasp2.nic.in/netnrega/state_html/' + url

                response = session.get(url)
                soup = BeautifulSoup(response.content, 'lxml')
                this['view_state'] =soup.find(id="__VIEWSTATE")['value']
                this['event_validation']=soup.find(id="__EVENTVALIDATION")['value']

                fetch_jobcard_stats(this, logger, lobj, response.url)
                exit(0)
            '''
            url_prefix = 'http://nregasp2.nic.in/netnrega/state_html/'
            for pobj in lobj.get_all_panchayat_objs(logger):
                logger.info(f'Fetch Report for pobj[{pobj}]')
                url = find_url_containing_text(response.content, pobj.panchayat_code,
                                       url_prefix=url_prefix)
                logger.debug(url)
                response = session.get(url)
                soup = BeautifulSoup(response.content, 'lxml')
                this['view_state'] =soup.find(id="__VIEWSTATE")['value']
                this['event_validation']=soup.find(id="__EVENTVALIDATION")['value']

                response = fetch_jobcard_stats(this, logger, pobj, url)
                df = None
                if response.status_code == 200:
                    df = get_dataframe_from_html(logger, response.content, mydict=extract_dict)
                    if df is not None:
                        df['finyear'] = finyear
                        df = insert_location_details(logger, lobj, dataframe)
                        df.head(10)
                        dfs.append(df)
    if len(dfs) > 0:
        dataframe = pd.concat(dfs)
    return dataframe

def get_nic_r4_1_columns(logger, finyear):
    if str(finyear) == str(get_current_finyear()):
        total_months = get_current_finmonth()
    else:
        total_months = 12
    total_columns = 2 + 7 + (total_months*2*7)
    columns = []
    ff_column_header = [
        'Muster Roll Issued',
        'Printed E Muster Roll(excluding zero muster)',
        'Muster Roll Filled',
        'Filled E-Muster Roll(excluding zero muster)',
        'Attendance Filled For Persons on Muster Roll',
        'Attendance Not Yet Filled On E Muster Roll',
        'Muster Roll with Date of Payment'
    ]
    columns.append("sr_no")
    columns.append("panchayat")
    fort_array = ["first_fn", "second_fn"]
    for i in range(0, total_months):
        for fort in fort_array:
            cur_month = ((i + 1 + 2) % 12)+1
            for each_col in ff_column_header:
                col = f"{cur_month}-{fort}-{each_col.replace(' ','_')}"
                columns.append(col)
    for each_col in ff_column_header:
        col = f"total-{each_col.replace(' ','_')}"
        columns.append(col)
    logger.info(f"Length of columns is {len(columns)}")
    return columns

def get_nic_r4_1(lobj, logger, url_df, finyear):
    '''Will Download NIC4_1 MIS report'''
    if url_df is None:
        return None
    logger.info(f"Shape of url_df is {url_df.shape}")
    filtered_df = url_df[url_df['block_code']==int(lobj.block_code)]
    filtered_df = filtered_df[filtered_df['finyear']==int(finyear)]
    logger.info(f"Shape of filtered_df is {filtered_df.shape}")
    df_array = []
    for index, row in filtered_df.iterrows():
        url = row.get('url')
        finyear = row.get('finyear')
        extract_dict = {}
        column_headers = get_nic_r4_1_columns(logger, finyear)
        extract_dict['pattern'] = "Fortnight"
        extract_dict['column_headers'] = column_headers
        extract_dict['data_start_row'] = 4
        logger.info(url)
        response = requests.get(url)
        if response.status_code == 200:
            myhtml = response.content
            dataframe = get_dataframe_from_html(logger, myhtml,
                                                mydict=extract_dict)
            dataframe['finyear'] = finyear
            df_array.append(dataframe)
    dataframe = pd.concat(df_array)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
    return dataframe

def get_location_list_from_dict(mydict):
    """"Getting location list from dict"""
    columns = ["state_code", "state_name", "district_code", "district_name", "block_code", "block_name", "panchayat_code", "panchayat_name"]
    mylist = []
    for column in columns:
        value = mydict.get(column, [""])
        mylist.append(value[0])
    return mylist

def get_nrega_locations(lobj, logger):
    """This loop will crawl all the nrega Locations"""
    csv_array = []
    logger.info(f"Crawling locations for state {lobj.state_name}")
    url_prefix = "https://mnregaweb4.nic.in/netnrega/"
    res = requests.get(lobj.mis_state_url)
    logger.info(lobj.mis_state_url)
    column_headers = ["parent_location_code", "crawl_ip", "state_short_code", "is_nic", "location_type", "name", "code", "name_not_english", "slug", "state_code", "state_name", "district_code", "district_name", "block_code", "block_name", "panchayat_code", "panchayat_name"]
    row = ["0", lobj.crawl_ip, lobj.state_short_code, lobj.is_nic, lobj.location_type, lobj.name, lobj.code, False, lobj.slug, lobj.state_code, lobj.state_name, "", "", "", "", "", ""]
    csv_array.append(row)
    if res.status_code != 200:
        return None
    cookies = res.cookies
    logger.debug(f"cookies are {cookies}")
    myhtml = res.content
    dist_identifier = "Homedist.aspx"
    dist_urls = find_urls_containing_text(myhtml, dist_identifier,
                                   url_prefix=url_prefix)
    for dist_url in dist_urls:
        parent_location_code = lobj.code
        logger.debug(f"Currently processing {dist_url}")
        parsed = urlparse.urlparse(dist_url.lower())
        dist_dict = parse_qs(parsed.query)
        name = dist_dict.get("district_name")[0]
        code = dist_dict.get("district_code")[0]
        district_code = code
        slug = slugify(name)
        row1 = [parent_location_code, lobj.crawl_ip, lobj.state_short_code, lobj.is_nic, "district", name, code, is_english(name), slugify(name)]
        row2 = get_location_list_from_dict(dist_dict)
        row = row1 + row2
        csv_array.append(row)
        block_identifier = 'PoIndexFrame.aspx'
        res1 = requests.get(dist_url)
        if res1.status_code != 200:
            return None
        disthtml = res1.content
        block_urls = find_urls_containing_text(disthtml, block_identifier, url_prefix=url_prefix)
        for block_url in block_urls:
            parent_location_code = district_code
            logger.debug(f"Block URL {block_url}")
            parsed = urlparse.urlparse(block_url.lower())
            block_dict = parse_qs(parsed.query)
            name = block_dict.get("block_name")[0]
            code = block_dict.get("block_code")[0]
            block_code = code
            slug = slugify(name)
            row1 = [parent_location_code, lobj.crawl_ip, lobj.state_short_code, lobj.is_nic, "block", name, code, is_english(name), slugify(name)]
            row2 = get_location_list_from_dict(block_dict)
            row = row1 + row2
            csv_array.append(row)
            parent_location_code = code
            res2 = requests.get(block_url)
            if res2.status_code != 200:
                return None
            block_html = res2.content
            panchayat_identifier = "IndexFrame.aspx"
            panchayat_urls = find_urls_containing_text(block_html, panchayat_identifier, url_prefix=url_prefix)
            for panchayat_url in panchayat_urls:
                parent_location_code = block_code
                parsed = urlparse.urlparse(panchayat_url.lower())
                panchayat_dict = parse_qs(parsed.query)
                name = panchayat_dict.get("panchayat_name")[0]
                code = panchayat_dict.get("panchayat_code")[0]
                slug = slugify(name)
                row1 = [parent_location_code, lobj.crawl_ip, lobj.state_short_code, lobj.is_nic, "panchayat", name, code, is_english(name), slugify(name)]
                row2 = get_location_list_from_dict(panchayat_dict)
                row = row1 + row2
                csv_array.append(row)
    dataframe = pd.DataFrame(csv_array, columns=column_headers)
    return dataframe

def download_muster_v2(logger, lobj, work_code_df, finyear, nic_url,
                       cookies=None):
    """Download musters as per new muster.aspx"""
    muster_column_config_file = f"{JSON_CONFIG_DIR}/muster_column_name_dict.json"
    logger.info(muster_column_config_file)
    with open(muster_column_config_file) as config_file:
        muster_column_dict = json.load(config_file)
    job_list = []
    for index, row in work_code_df.iterrows():
        work_code = row.get('work_code', None)
        work_name = row.get('work_name', None)
        func_args = [lobj, work_code, work_name, finyear, nic_url, muster_column_dict] 
        job_dict = {
            'func_name' : 'download_muster_for_work_code',
            'func_args' : func_args
        }
        job_list.append(job_dict)
    #dataframe = libtech_queue_manager(logger, job_list)
    dataframe = libtech_queue_manager(logger, job_list, num_threads=5)
    return dataframe
    #download_muster_for_work_code(logger, lobj, work_code, finyear,
    #                              nic_url)
    #break
def update_muster_transactions_v2(lobj, logger):
    """Going to update muster transactions """
    logger.info(f"Updating muster Transactions for {lobj.code}")
    ml_df = lobj.fetch_report_dataframe(logger, "muster_list_v2")
    nic_urls_df = lobj.fetch_report_dataframe(logger, 'nic_urls')
    all_panchayats = lobj.get_all_panchayats(logger)
    logger.info(f"All panchayats are {all_panchayats}")
    first_panchayat_code = all_panchayats[0]
    filtered_df = nic_urls_df[(nic_urls_df["report_slug"] == "muster-roll") &
                              (nic_urls_df["panchayat_code"] == int(first_panchayat_code))]
    logger.debug(f"Filtered DF shape is {filtered_df.shape}")
    df_grouped = ml_df.groupby(['finyear','work_code','work_name']).agg({'muster_code': ['count']})
    df_grouped.columns = ['count1'] 
    df_grouped = df_grouped.reset_index()
    logger.info(f"Shape of ml df is {ml_df.shape}")
    logger.info(f"Shape of df_grouped is {df_grouped.shape}")
    response = request_with_retry_timeout(logger, lobj.mis_state_url,
                                          method="get")
    if response is None:
        return
    cookies = response.cookies
    logger.debug(f"session cookies {cookies}")
    df_array = []
    for index, row in filtered_df.iterrows():
        nic_url = row.get("mis_url")
        finyear = row.get("finyear")
        df_grouped_filtered = df_grouped[df_grouped["finyear"] == int(finyear)]
        logger.debug(f"Processing finyear {finyear} url {nic_url}")
        dataframe = download_muster_v2(logger, lobj, df_grouped_filtered,
                                       finyear, nic_url, cookies=cookies)
        if dataframe is not None:
            df_array.append(dataframe)
    if len(df_array) == 0:
        return None
    dataframe = pd.concat(df_array)
    dataframe = insert_location_details(logger, lobj, dataframe)
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name"]
    col_list = ['finyear', 'muster_code', 'work_code', 'work_name', 'muster_no', 'date_from',
     'date_to', 'muster_index', 'name_relationship', 'name',
     'relationship', 'jobcard', 'm_caste', 'Village',
     'days_worked', 'day_wage', 'm_labour_wage', 'm_travel_cost',
     'm_tools_cost', 'total_wage', 'm_postoffice_bank_name',
     'm_pocode_bankbranch', 'm_poadd_bankbranchcode', 'm_wagelist_no',
     'muster_status', 'credited_date', 'thumb_impression', 'is_complete']
    all_cols = location_cols + col_list
    dataframe = dataframe[all_cols]
    return dataframe

def get_dynamic_work_report_r6_18(lobj, logger):
    """Will fetch dynamic work report"""
    state_code = lobj.state_code
    district_code = lobj.district_code
    block_code = lobj.block_code
    logger.info(f"Downloading Dynamic work report for {lobj.code}")
    finyear = get_current_finyear()
    url_text = "dynamic_work_details.aspx"
    filename = f"{NREGA_DATA_DIR}/misReport_{finyear}.html"
    logger.info(filename)
    with open(filename, "rb") as infile:
        myhtml = infile.read()
    mysoup = BeautifulSoup(myhtml, "lxml")
    elem = mysoup.find("a", href=re.compile(url_text))
    if elem is None:
        logger.debug("No such report found")
    if elem is not None:
        base_href = elem["href"]
    logger.info(base_href)
    url = base_href
    #url = 'http://mnregaweb4.nic.in/netnrega/dynamic_work_details.aspx?page=S&lflag=eng&state_name=RAJASTHAN&state_code=27&fin_year=2020-2021&source=national&Digest=iNIvQfUeFfm1zBPVK3uvFQ'
    session = requests.Session()
    response = get_request_with_retry_timeout(logger,url)
    soup = BeautifulSoup(response.text, 'lxml')

    VIEWSTATE=soup.find(id="__VIEWSTATE")['value']
    VIEWSTATEGENERATOR=soup.find(id="__VIEWSTATEGENERATOR")['value']
     

    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'X-MicrosoftAjax': 'Delta=true',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': '*/*',
        'Origin': 'http://mnregaweb4.nic.in',
        'Referer': url,
        'Accept-Language': 'en-US,en;q=0.9',
    }
    data = {
      'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddl_state',
      '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddl_state',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': VIEWSTATE,
      '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
      '__VIEWSTATEENCRYPTED': '',
      'ctl00$ContentPlaceHolder1$ddl_state': state_code,
      'ctl00$ContentPlaceHolder1$ddl_dist': 'ALL',
      'ctl00$ContentPlaceHolder1$ddl_blk': 'ALL',
      'ctl00$ContentPlaceHolder1$ddl_pan': 'ALL',
      'ctl00$ContentPlaceHolder1$Ddlworkcategory': 'ALL',
      'ctl00$ContentPlaceHolder1$ddlprostatus': 'ALL',
      'ctl00$ContentPlaceHolder1$ddlexpnana': 'ALL',
      'ctl00$ContentPlaceHolder1$ddlexpnest': 'ALL',
      'ctl00$ContentPlaceHolder1$ddlFin_year': '2020-2021',
      'ctl00$ContentPlaceHolder1$Ddlwork_status': '03',
      'ctl00$ContentPlaceHolder1$Ddlrailway_work': 'ALL',
      '__ASYNCPOST': 'true',
      '': ''
    }
    response = request_with_retry_timeout(logger,url, headers=headers, data=data)
    soup = BeautifulSoup(response.text, 'lxml')

    # Update the context
    body = soup.find('body')
    array = body.text.split('|')
    VIEWSTATE = array[array.index('__VIEWSTATE')+1]
    VIEWSTATEGENERATOR = array[array.index('__VIEWSTATEGENERATOR')+1]



    data = {
      'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddl_dist',
      '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddl_dist',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': VIEWSTATE,
      '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
      '__VIEWSTATEENCRYPTED': '',
      'ctl00$ContentPlaceHolder1$ddl_state': state_code,
      'ctl00$ContentPlaceHolder1$ddl_dist': district_code,
      'ctl00$ContentPlaceHolder1$ddl_blk': 'ALL',
      'ctl00$ContentPlaceHolder1$ddl_pan': 'ALL',
      'ctl00$ContentPlaceHolder1$Ddlworkcategory': 'ALL',
      'ctl00$ContentPlaceHolder1$ddlprostatus': 'ALL',
      'ctl00$ContentPlaceHolder1$ddlexpnana': 'ALL',
      'ctl00$ContentPlaceHolder1$ddlexpnest': 'ALL',
      'ctl00$ContentPlaceHolder1$ddlFin_year': '2020-2021',
      'ctl00$ContentPlaceHolder1$Ddlwork_status': '03',
      'ctl00$ContentPlaceHolder1$Ddlrailway_work': 'ALL',
      '__ASYNCPOST': 'true',
      '': ''
    }

    #response = session.post('http://mnregaweb4.nic.in/netnrega/dynamic_work_details.aspx', headers=headers, params=params, data=data, verify=False)
    response = request_with_retry_timeout(logger,url, headers=headers, data=data)
    soup = BeautifulSoup(response.text, 'lxml')

    # Update the context
    body = soup.find('body')
    #logger.warning(body.text)
    array = body.text.split('|')
    VIEWSTATE = array[array.index('__VIEWSTATE')+1]
    #logger.debug(self.view_state)
    VIEWSTATEGENERATOR = array[array.index('__VIEWSTATEGENERATOR')+1]
    #logger.debug(self.event_validation)

    data['ctl00$ContentPlaceHolder1$ScriptManager1'] = 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddl_blk'
    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ddl_blk'
    data['__VIEWSTATE'] = VIEWSTATE
    data['__VIEWSTATEGENERATOR'] = VIEWSTATEGENERATOR
    data['ctl00$ContentPlaceHolder1$ddl_blk'] = block_code

    #response = session.post('http://mnregaweb4.nic.in/netnrega/dynamic_work_details.aspx', headers=headers, params=params, data=data, verify=False)
    response = request_with_retry_timeout(logger, url, headers=headers, data=data)
    soup = BeautifulSoup(response.text, 'lxml')



    # Update the context
    body = soup.find('body')
    #logger.warning(body.text)
    array = body.text.split('|')
    VIEWSTATE = array[array.index('__VIEWSTATE')+1]
    #logger.debug(self.view_state)
    VIEWSTATEGENERATOR = array[array.index('__VIEWSTATEGENERATOR')+1]
    #logger.debug(self.event_validation)


    data['ctl00$ContentPlaceHolder1$ScriptManager1'] = 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddlFin_year'
    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$ddlFin_year'
    data['__VIEWSTATE'] = VIEWSTATE
    data['__VIEWSTATEGENERATOR'] = VIEWSTATEGENERATOR
    data['ctl00$ContentPlaceHolder1$ddlFin_year'] = 'ALL'


    #response = session.post('http://mnregaweb4.nic.in/netnrega/dynamic_work_details.aspx', headers=headers, params=params, data=data, verify=False)
    response = request_with_retry_timeout(logger, url, headers=headers, data=data)
    soup = BeautifulSoup(response.text, 'lxml')



    # Update the context
    body = soup.find('body')
    #logger.warning(body.text)
    array = body.text.split('|')
    VIEWSTATE = array[array.index('__VIEWSTATE')+1]
    #logger.debug(self.view_state)
    VIEWSTATEGENERATOR = array[array.index('__VIEWSTATEGENERATOR')+1]


    data['ctl00$ContentPlaceHolder1$ScriptManager1'] = 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$Ddlwork_status'
    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$Ddlwork_status'
    data['__VIEWSTATE'] = VIEWSTATE
    data['__VIEWSTATEGENERATOR'] = VIEWSTATEGENERATOR
    data['ctl00$ContentPlaceHolder1$Ddlwork_status'] = 'ALL'

    #response = session.post('http://mnregaweb4.nic.in/netnrega/dynamic_work_details.aspx', headers=headers, params=params, data=data, verify=False)
    response = request_with_retry_timeout(logger,url, headers=headers, data=data)
    soup = BeautifulSoup(response.text, 'lxml')



    # Update the context
    body = soup.find('body')
    #logger.warning(body.text)
    array = body.text.split('|')
    VIEWSTATE = array[array.index('__VIEWSTATE')+1]
    #logger.debug(self.view_state)
    VIEWSTATEGENERATOR = array[array.index('__VIEWSTATEGENERATOR')+1]
    #logger.debug(self.event_validation)


    ### Final request
    data['ctl00$ContentPlaceHolder1$ScriptManager1'] = 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$Button1'
    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$Ddlwork_status'
    data['__VIEWSTATE'] = VIEWSTATE
    data['__VIEWSTATEGENERATOR'] = VIEWSTATEGENERATOR
    data['ctl00$ContentPlaceHolder1$Button1'] = 'Submit'

    #response = session.post('http://mnregaweb4.nic.in/netnrega/dynamic_work_details.aspx', headers=headers, params=params, data=data, verify=False)
    response = request_with_retry_timeout(logger,url, headers=headers, data=data)

    myhtml = response.content
    extract_dict = {}
    column_headers = ["sno","district_name1","block_name1","panchayat_name","work_start_fin_year","work_status","work_code","work_name","master_work_category_name","work_category_name","work_type","agency_name","sanction_amount_in_lakh","total_amount_paid_since_inception_in_lakh","total_mandays","no_of_units","is_secure","is_convergence","work_started_date","work_physically_completed_date"]
    extract_dict['pattern'] = "Master Work Category Name"
    extract_dict['column_headers'] = column_headers
    extract_dict['data_start_row'] = 2
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
        dataframe = insert_finyear_in_dataframe(logger, dataframe, 'work_started_date',
                                date_format="%d-%m-%Y")
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name", "finyear"]
    all_cols = location_cols + column_headers
    dataframe = dataframe[all_cols]
    return dataframe

def get_worker_stats(lobj, logger, nic_urls_df):
    """This will fetch the worker stats"""
    use_state_server = False
    df_array = []
    column_headers = ['srno', 'jobcard', 'head_of_household', 'name',
                      'total_work_days']
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    all_cols = location_cols + ["finyear"] + column_headers
    empty_dataframe = pd.DataFrame(columns=all_cols)
    dataframe = None
    logger.info(f"Going to fetch the worker register for {lobj.block_code}")
    logger.debug(f"shape of urls df is {nic_urls_df.shape}")
    report_slug = "household-provided-employment-with-specified-no-of-days"
    filtered_df = nic_urls_df[nic_urls_df["report_slug"] == report_slug]
    logger.debug(f"shape of urls df is {filtered_df.shape}")
    logger.debug(f"Mis state URL {lobj.mis_state_url}")
    if (use_state_server == True):
        url = lobj.mis_state_url.replace("mnregaweb4.nic.in", lobj.crawl_ip)
        response = get_request_with_retry_timeout(logger, lobj.mis_state_url)
    else:
        response = get_request_with_retry_timeout(logger, lobj.mis_state_url)
        
    if response is None:
        logger.debug(f"response is now for {lobj.mis_state_url}")
        return empty_dataframe
    cookies = response.cookies
    logger.debug(f"Cookies are {cookies}")
    for index, row in filtered_df.iterrows():
        finyear = row['finyear']
        url = row['mis_url']
        if use_state_server == True:
            url = url.replace("mnregaweb4.nic.in", lobj.crawl_ip)
        headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15;rv:81.0) Gecko/20100101 Firefox/81.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'X-MicrosoftAjax': 'Delta=true',
                'Cache-Control': 'no-cache',
                'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
                'Origin': 'https://mnregaweb4.nic.in',
                'Connection': 'keep-alive',
                'Referer': url
        }
        for pobj in lobj.get_all_panchayat_objs(logger):
            response = get_request_with_retry_timeout(logger, url, cookies=cookies)
            if response is None:
                logger.debug(f"response is None for {url}")
                continue
            htmlsoup = BeautifulSoup(response.content, 'lxml')
            view_state  =  htmlsoup.find(id = '__VIEWSTATE').get('value')
            validation  =  htmlsoup.find(id = '__EVENTVALIDATION').get('value')
            data = {
                'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddr_panch',
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddr_panch',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': view_state,
                '__VIEWSTATEGENERATOR': '68012A6D',
                '__VIEWSTATEENCRYPTED': '',
                '__EVENTVALIDATION': validation,
                'ctl00$ContentPlaceHolder1$ddr_panch': pobj.code,
                'ctl00$ContentPlaceHolder1$ddr_cond': '',
                'ctl00$ContentPlaceHolder1$lbl_days': '100',
                'ctl00$ContentPlaceHolder1$rblRegWorker': 'Y',
                '__ASYNCPOST': 'true',
                '': ''
            }
            response = request_with_retry_timeout(logger, url, data=data,
                                                  headers=headers, cookies=cookies)
            if response is None:
                continue
            htmlsoup = BeautifulSoup(response.content, 'lxml')
            body = htmlsoup.find('body')
            #logger.warning(body.text)
            array = body.text.split('|')
            view_state = array[array.index('__VIEWSTATE')+1]
            validation = array[array.index('__EVENTVALIDATION')+1]
            data = {
                'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddr_cond',
                'ctl00$ContentPlaceHolder1$ddr_panch': pobj.code,
                'ctl00$ContentPlaceHolder1$ddr_cond': 'gte',
                'ctl00$ContentPlaceHolder1$lbl_days': '100',
                'ctl00$ContentPlaceHolder1$rblRegWorker': 'Y',
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddr_cond',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': view_state,
                '__VIEWSTATEGENERATOR': '68012A6D',
                '__VIEWSTATEENCRYPTED': '',
                '__EVENTVALIDATION': validation,
                '__ASYNCPOST': 'true',
                '': ''
            }
            response = request_with_retry_timeout(logger, url, data=data,
                                                  headers=headers, cookies=cookies)
            if response is None:
                continue
            htmlsoup = BeautifulSoup(response.content, 'lxml')
            body = htmlsoup.find('body')
            array = body.text.split('|')
            view_state = array[array.index('__VIEWSTATE')+1]
            validation = array[array.index('__EVENTVALIDATION')+1]
            data = {
                'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$btn_pro',
                'ctl00$ContentPlaceHolder1$ddr_panch': pobj.code,
                'ctl00$ContentPlaceHolder1$ddr_cond': 'gte',
                'ctl00$ContentPlaceHolder1$lbl_days': '0',
                'ctl00$ContentPlaceHolder1$rblRegWorker': 'N',
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btn_pro',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': view_state,
                '__VIEWSTATEGENERATOR': '68012A6D',
                '__EVENTVALIDATION': validation,
                '__VIEWSTATEENCRYPTED': '',
                '__ASYNCPOST': 'true',
                '': ''
            }
            response = request_with_retry_timeout(logger, url, data=data,
                                                  headers=headers, cookies=cookies)
            if response is None:
                continue

            myhtml = response.content
            htmlsoup = BeautifulSoup(response.content, 'lxml')
            body = htmlsoup.find('body')
            #logger.warning(body.text)
            array = body.text.split('|')
            page_url = array[array.index('pageRedirect')+2]
            url_prefix = "https://mnregaweb4.nic.in"
            page_url = url_prefix + page_url
            response = get_request_with_retry_timeout(logger, page_url,
                                                      cookies=cookies)
            if response is None:
                continue
            myhtml = response.content
            panchayat_code = ''
            logger.info(f"Page url for finyear {finyear} panchayat_cod {pobj.code} is {page_url}")
            extract_dict = {}
            extract_dict['table_id'] = 'ctl00_ContentPlaceHolder1_GridView1'
            extract_dict['data_start_row'] = 2
            extract_dict['column_headers'] = column_headers
            dataframe = get_dataframe_from_html(logger, myhtml,
                                        mydict=extract_dict)
            if dataframe is not None:
                dataframe['finyear'] = finyear
                dataframe['panchayat_code'] = pobj.panchayat_code
                dataframe['panchayat_name'] = pobj.panchayat_name
                df_array.append(dataframe)
    if len(df_array) == 0:
        return empty_dataframe
    dataframe = pd.concat(df_array)
    dataframe = insert_location_details(logger, lobj, dataframe)
    dataframe = dataframe[all_cols]
    return dataframe

def get_nic_locations(lobj, logger):
    """Will fetch all nic locations"""
    objs = lobj.get_all_panchayat_objs(logger)
    csv_array = []
    for obj in objs:
        row = [obj.code, obj.name]
        csv_array.append(row)
    dataframe = pd.DataFrame(csv_array, columns=["panchayat_code",
                                                 "panchayat_name"])
    dataframe = insert_location_details(logger, lobj, dataframe)
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name",
                     "panchayat_code", "panchayat_name"]
    dataframe = dataframe[location_cols]
    return dataframe

def get_fto_list(lobj, logger, rej_stat_df):
    """This will fetch the fto list for the block"""
    logger.info(f"Fetching fto list for {lobj.block_name}")
    filtered_df = rej_stat_df[rej_stat_df['block_code'] == int(lobj.block_code)]
    start_fin_year = get_default_start_fin_year()
    logger.info(f"Shape of filtered_df is {filtered_df.shape}")
    filtered_df = filtered_df[filtered_df['finyear'] >= int(start_fin_year)]
    filtered_df = rej_stat_df[rej_stat_df['block_code'] == int(lobj.block_code)]
    start_fin_year = get_default_start_fin_year()
    logger.info(f"Shape of filtered_df is {filtered_df.shape}")
    filtered_df = filtered_df[filtered_df['finyear'] >= int(start_fin_year)]
    column_headers = ["srno", "fto_no", "fto_url", "financial_institution",
                      "second_signatory_date", "5", "6", "7", "8", "9", "10",
                      "11", "12"]
    extract_dict = {}
    extract_dict["pattern"] = "Financial Institution"
    extract_dict['column_headers'] = column_headers
    extract_dict['extract_url_array'] = [1]
    extract_dict['url_prefix'] = "http://mnregaweb4.nic.in/netnrega/FTO/"
    df_array = []
    for index, row in filtered_df.iterrows():
        url = row.get("second_singnatory_fto_url", None)
        finyear = row.get("finyear", None)
        fin_agency = row.get("fin_agency", None)
        logger.info(url)
        response = get_request_with_retry_timeout(logger, url)
        if response is None:
            continue
        myhtml = response.content
        dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
        if dataframe is None:
            continue
        dataframe["finyear"] = finyear
        dataframe["fin_agency"] = fin_agency
        df_array.append(dataframe)
    if (len(df_array)) == 0:
        return None
    dataframe = pd.concat(df_array)
    other_cols = ["finyear", "fin_agency", "fto_no", "fto_url",
                  "financial_institution", "second_signatory_date"]
    dataframe = insert_location_details(logger, lobj, dataframe)
    location_cols = ["state_code", "state_name", "district_code",
                     "district_name", "block_code", "block_name"]
    all_cols = location_cols + other_cols
    dataframe = dataframe[all_cols]
    return dataframe



def get_nic_r14_5_urls(lobj, logger, report_type=None, url_text=None,
                      url_prefix=None):
    """This function will get the Urls at the block level"""
    state_pattern = f"state_code={lobj.state_code}"
    logger.info(f"Getting URLs from MIS Reports for {report_type} and pattern{url_text}")
    current_df = lobj.fetch_report_dataframe(logger, report_type)
    filtered_df = None
   #if current_df is not None:
   #  logger.info(f"Shape of current df is {current_df.shape}")
   #  current_finyear = get_current_finyear()
   #  filtered_df = current_df[current_df['finyear'] != current_finyear]
   #  logger.info(f"Shape of filtered df is {filtered_df.shape}")

    csv_array = []
    column_headers = ["state_code", "district_code", "block_code", "state_name",
                      "district_name", "block_name", "finyear", "url"]
    start_finyear = get_default_start_fin_year()
    end_finyear = get_current_finyear()
    for finyear in range(int(start_finyear), int(end_finyear)+1):
        logger.info(f"Downloading for FinYear {finyear}")
        filename = f"{NREGA_DATA_DIR}/misReport_{finyear}.html"
        logger.info(filename)
        with open(filename, "rb") as infile:
            myhtml = infile.read()
        mysoup = BeautifulSoup(myhtml, "lxml")
        elem = mysoup.find("a", href=re.compile(url_text))
        if elem is not None:
            base_href = elem["href"]
        logger.info(base_href)
        res = requests.get(base_href)
        myhtml = None
        if res.status_code == 200:
            myhtml = res.content
        if myhtml is not None:
            mysoup = BeautifulSoup(myhtml, "lxml")
            elems = mysoup.find_all("a", href=re.compile(url_text))
            for elem in elems:
                logger.info(elem)
                state_href = elem["href"]
                logger.info(f"State URL is {state_href}")
                if state_pattern not in state_href:
                    continue
                url = url_prefix + state_href
                response = requests.get(url)
                logger.info(url)
                dist_html = None
                if response.status_code == 200:
                    dist_html = response.content
                if dist_html is not None:
                    dist_soup = BeautifulSoup(dist_html, "lxml")
                    elems = dist_soup.find_all("a", href=re.compile(url_text))
                    for elem1 in elems:
                        dist_url = url_prefix + elem1["href"]
                        block_res = requests.get(dist_url)
                        if block_res.status_code == 200:
                            block_html = block_res.content
                            block_soup = BeautifulSoup(block_html, "lxml")
                            belems = block_soup.find_all("a", href=re.compile(url_text))
                            for belem in belems:
                                block_url = url_prefix + belem["href"]
                                #logger.info(dist_url)
                                parsed = urlparse.urlparse(block_url)
                                params_dict = parse_qs(parsed.query)
                                #logger.info(params_dict)
                                state_name = params_dict.get("state_name", [''])[0]
                                state_code = params_dict.get("state_code", [""])[0]
                                district_name = params_dict.get("district_name",
                                                                [""])[0]
                                district_code = params_dict.get("district_code",
                                                                [""])[0]
                                block_name = params_dict.get("block_name",
                                                                [""])[0]
                                block_code = params_dict.get("block_code",
                                                                [""])[0]
                                row = [state_code, district_code, block_code, state_name,
                                       district_name, block_name, finyear, block_url]
                                logger.info(row)
                                csv_array.append(row)

    dataframe = pd.DataFrame(csv_array, columns=column_headers)
    if filtered_df is not None:
        dataframe = pd.concat([filtered_df, dataframe])
    return dataframe


def get_nic_r14_5(lobj, logger, url_df, finyear):
    '''Will Download NIC4_1 MIS report'''
    if url_df is None:
        return None
    logger.info(f"Shape of url_df is {url_df.shape}")
    filtered_df = url_df[url_df['block_code']==int(lobj.block_code)]
    filtered_df = filtered_df[filtered_df['finyear']==int(finyear)]
    logger.info(f"Shape of filtered_df is {filtered_df.shape}")
    df_array = []
    for index, row in filtered_df.iterrows():
        url = row.get('url')
        finyear = row.get('finyear')
        logger.info(url)
        extract_dict = {}
        column_headers = ['S.No', 'location','Payment Between 0-8 Days-Total Transactions', '0_8_txns_urls',
                    'Payment Between 0-8 Days-Amount Involved','Payment Between 9-15 Days-Total Transactions','9_15_txns_urls',
                    'Payment Between 9-15 Days-Amount Involved','Payment Between 16-30 Days-Total Transactions','16_30_txns_urls',
                    'Payment Between 16-30 Days-Amount Involved','Payment Between 31-60 Days-Total Transactions','31_60_txns_urls',
                    'Payment Between 31-60 Days-Amount Involved','Payment Between 61-90 Days-Total Transactions','61_90_txns_urls',
                    'Payment Between 61-90 Days-Amount Involved','Delayed Payment more than 90 Days-Total Transactions','more_than_90_txns_urls',
                    'Delayed Payment more than 90 Days-Amount Involved','Total Delayed Payment-Total Transactions',
                    'Total Delayed Payment-Amount Involved','Total Payment For Financial Year-Total Transactions',
                    'Total Payment For Financial Year-Amount Involved']
        extract_dict['pattern'] = "Total Transactions"
        extract_dict['extract_url_array'] = [2,4,6,8,10,12]
        extract_dict['url_prefix'] = 'http://mnregaweb4.nic.in/netnrega/'
        extract_dict['column_headers'] = column_headers
        extract_dict['data_start_row'] = 4
        logger.info(url)
        response = requests.get(url)
        if response.status_code == 200:
            myhtml = response.content
            dataframe = get_dataframe_from_html(logger, myhtml,
                                                mydict=extract_dict)
            dataframe['finyear'] = finyear
            df_array.append(dataframe)
    dataframe = pd.concat(df_array)
    dataframe = dataframe[dataframe.location != 'Total'].reset_index(drop=True)
    if dataframe is not None:
        dataframe = insert_location_details(logger, lobj, dataframe)
    return dataframe

def get_fto_transactions(lobj, logger, finyear, fto_list_df):
    logger.info(f"goign to fetch fto transactions {lobj.name}")
    worker_df= lobj.fetch_report_dataframe(logger, "worker_register")
    worker_df_cols = ["state_code", "state_name", "district_code", "district_name", "block_code", "block_name", "panchayat_code", "panchayat_name", "village_name", "caste", "head_of_household", "jobcard"]
    worker_df = worker_df[worker_df_cols]
    worker_df = worker_df.drop_duplicates()
    filtered_df = fto_list_df[fto_list_df['block_code'] == int(lobj.block_code)]
    filtered_df = fto_list_df[filtered_df['finyear'] == int(finyear)]
    filtered_df = filtered_df.fillna('')
    logger.debug(f"shape of filtered_df is {filtered_df.shape}")
    job_list = [];
    column_headers = ["srno", "block", "job_card_no_panch", "reference_no",
                      "transaction_date", "fto_applicant_name", "wagelist_no",
                      "primary_account_holder", "bank_code", "ifsc_code",
                      "fto_amount_to_be_credit", "fto_credited_amount",
                      "fto_status", "processed_date",
                      "bank_to_coop_processed_date", "utr_no",
                      "rejection_reason", "favor_in_apb_transaction",
                      "bank_iin_in_apb_transaction"]
    extract_dict = {}
    extract_dict["pattern"] = "Reference No."
    extract_dict["column_headers"] = column_headers

    for index, row in filtered_df.iterrows():
        func_name = "fetch_fto_transactions"
        func_args = [];
        url = row.get("fto_url", None)
        if url is None:
            continue
        if ( (url == "") or ("http" not in url)):
            continue
        field_dict = {}
        field_dict["fin_agency"] = row.get("fin_agency", "")
        field_dict["fto_no"] = row.get("fto_no", "")
        field_dict["financial_institution"] = row.get("financial_institution", "")
        field_dict["second_signatory_date"] = row.get("second_signatory_date", "")
        field_dict["finyear"] = finyear
        field_dict["fto_url"] = url
        func_args = [lobj, url, extract_dict, field_dict] 
        job_dict = {
            'func_name' : func_name,
            'func_args' : func_args
        }
        job_list.append(job_dict)
    #dataframe = libtech_queue_manager(logger, job_list)
    dataframe = libtech_queue_manager(logger, job_list)
    if dataframe is None:
        return None
    dataframe = pd.merge(dataframe, worker_df, how='left',
                         on=['jobcard'])
    fto_columns = ["fto_no", "finyear", "second_signatory_date", "fin_agency",
                   "financial_institution", "fto_url"]
    column_headers = ["job_card_no_panch", "reference_no",
                      "transaction_date", "fto_applicant_name", "wagelist_no",
                      "primary_account_holder", "bank_code", "ifsc_code",
                      "fto_amount_to_be_credit", "fto_credited_amount",
                      "fto_status", "processed_date",
                      "bank_to_coop_processed_date", "utr_no",
                      "rejection_reason", "favor_in_apb_transaction",
                      "bank_iin_in_apb_transaction"]
    all_cols = worker_df_cols + fto_columns + column_headers
    dataframe = dataframe[all_cols]
        
    return dataframe
