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

def get_worker_register(lobj, logger):
    """This function will get the worker register from the nrega url"""
    dataframe = None
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
                name = name.replace('*', '')
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
    logger.info(f"we are going to fetch muster list")
    logger.info(f"panchayat page url is {lobj.panchayat_page_url}")
    job_list = []
    response = requests.get(lobj.panchayat_page_url)
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
    dataframe = libtech_queue_manager(logger, job_list)
    return dataframe

def get_muster_list(lobj, logger, jobcard_transactions_df):
    """From the jobcard transactions df this function will first get unique
    work urls and from work urls it will get all unique muster urls"""
    logger.info(f"lenth of dataframe is  {len(jobcard_transactions_df)}")
    work_url_array = jobcard_transactions_df.work_name_url.unique()
    logger.info(f"Number of unique work urls is {len(work_url_array)}")
    response = requests.get(lobj.panchayat_page_url)
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
    finyear_regex = re.compile(r'finyear=\d{4}-\d{4}')
    for index, row in dataframe.iterrows():
        url = row['muster_url']
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
        dataframe.loc[index, 'finyear'] = finyear
        dataframe.loc[index, 'date_from'] = date_from
        dataframe.loc[index, 'date_to'] = date_to
        dataframe.loc[index, 'work_name'] = work_name
        dataframe.loc[index, 'work_code'] = work_code
    dataframe['block_code'] = lobj.block_code
    logger.info(f"shape of dataframe is {dataframe.shape}")
    start_fin_year = get_default_start_fin_year()
    dataframe = dataframe[dataframe['finyear'] >= start_fin_year]
    logger.info(f"shape of dataframe is {dataframe.shape}")
    dataframe = dataframe.drop_duplicates()
    logger.info(f"shape of dataframe is {dataframe.shape}")
    dataframe = dataframe.reset_index(drop=True)
    return dataframe

def get_muster_transactions(lobj, logger, muster_list_df):
    """form the muster list dataframe this will get all the muster
    transactions"""
    worker_df = lobj.fetch_report_dataframe(logger, "worker_register")
    logger.info(f"shape of worker df is {worker_df.shape}")
    try:
        response = requests.get(lobj.panchayat_page_url, timeout = 5)
    except requests.exceptions.Timeout as e: 
        logger.error(e)
    cookies = response.cookies
    job_list = []
    func_name = "fetch_muster_details"
    MUSTER_COLUMN_CONFIG_FILE = f"json_config/muster_column_name_dict.json"
    with open(MUSTER_COLUMN_CONFIG_FILE) as config_file:
        muster_column_dict = json.load(config_file)
    logger.info(muster_column_dict)
    for index, row in muster_list_df.iterrows():
        url = row['muster_url']
        muster_no = row['muster_no']
        finyear = row['finyear']
        block_code = row['block_code']
        func_args = [lobj, url, cookies, muster_no, finyear, block_code, muster_column_dict]
        job_dict = {
            'func_name' : func_name,
            'func_args' : func_args
        }
        job_list.append(job_dict)
    dataframe = libtech_queue_manager(logger, job_list)
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
        name = name_relationship.replace(f"({relationship})","")
        dataframe.loc[index, 'name'] = name
        dataframe.loc[index, 'relationship'] = relationship
    
    dataframe = dataframe.drop(rows_to_delete)
    logger.info(f"dataframe shape is {dataframe.shape}")
    logger.info(dataframe.columns)
    logger.info(muster_list_df.columns)
    dataframe = pd.merge(dataframe, muster_list_df, how='left',
                         on=['block_code', 'finyear', 'muster_no'])
    logger.info(f"dataframe shape is {dataframe.shape}")
    drop_columns = ['caste', 'block_code']
    dataframe = dataframe.drop(columns=drop_columns)
    dataframe = pd.merge(dataframe, worker_df, how='left',
                         on=['jobcard', 'name'])
    logger.info(dataframe.columns)
    logger.info(f"dataframe shape is {dataframe.shape}")
    col_list = ['state_code', 'state_name', 'district_code', 'district_name',
                'block_code', 'block_name', 'panchayat_name', 'panchayat_code',
                'village_name', 'jobcard', 'name', 'relationship',
                'head_of_household', 'caste', 'IAY_LR', 'father_husband_name',
                'gender', 'age', 'jobcard_request_date', 'jobcard_issue_date', 'jobcard_remarks',
                'disabled', 'minority', 'jobcard_verification_date', 'work_code',
                'work_name', 'finyear', 'muster_no', 'muster_index',
                'date_from', 'date_to', 'muster_url',
                'days_worked', 'day_wage', 'm_labour_wage', 'm_travel_cost',
                'm_tools_cost', 'total_wage', 'm_postoffice_bank_name',
                'm_pocode_bankbranch', 'm_poadd_bankbranchcode',
                'm_wagelist_no', 'muster_status', 'credited_date',
                ]
    dataframe = dataframe[col_list]
    return dataframe

def get_block_rejected_transactions(lobj, logger):
    """This function will fetch all theblock rejected transactions"""
    #As a first step we need to get all the list of jobcards for that block
    panchayat_ids = lobj.get_all_panchayat_ids(logger)
    jobcard_df_array = []
    report_type = 'worker_register'
    for panchayat_id in panchayat_ids:
        cur_df = api_get_report_dataframe(logger, panchayat_id, report_type,
                                          finyear=None, index_col=0, dtype=None)
        if cur_df is not None:
            jobcard_df_array.append(cur_df)
    jobcard_df = pd.concat(jobcard_df_array)
    #As a second step we need to create array of urls that we need to download
    urls_to_download = []
    url_report_types = ["NICRejectedTransactionsURL",
                        "NICRejectedTransactionsPostURL",
                        "NICRejectedTransactionsCoBankURL"
                       ]
    start_fin_year = get_default_start_fin_year()
    current_fin_year = get_current_finyear()
    finyear_array = []
    for finyear in range(start_fin_year, int(current_fin_year) +1):
        finyear_array.append(finyear)
    ##Now we will loop thorugh finyear and urls to create an array of urls that
    ##needs to be fetched
    for url_report_type in url_report_types:
        for finyear in finyear_array:
            report_url = api_get_report_url(logger, lobj.id,
                                            url_report_type, finyear=finyear)
            if report_url is not None:
                urls_to_download.append(report_url)
    logger.info(urls_to_download)

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
    for url in urls_to_download:
        dataframe = get_dataframe_from_url(logger, url, mydict=extract_dict)
        dataframe_array.append(dataframe)
    rejected_df = pd.concat(dataframe_array, ignore_index=True)
    #dataframe = pd.read_csv("/tmp/rejected_transactions.csv", index_col=0)
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
                           'rejection_date']
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
                'disabled', 'minority', 'jobcard_verification_date', 'work_code',
                'work_name', 'muster_no', 'attempt_count', 'record_status',
                'wagelist_no', 'fto_no', 'fto_finyear', 'fto_amount',
                'transaction_date', 'process_date', 'reference_no',
                'rejection_status', 'rejection_reason', 'rejection_serial_no',
                'utr_no', 'primary_account_holder', 'bank_code', 'ifsc_code']
    dataframe = dataframe[col_list]
    logger.info(f"dataframe shape is {dataframe.shape}")
    return dataframe
