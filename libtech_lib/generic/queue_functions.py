"""This has all the functions that need tobe executed in the queue"""
import requests
import urllib.parse as urlparse
from urllib.parse import parse_qs
import pandas as pd
import json
from bs4 import BeautifulSoup
from libtech_lib.generic.html_functions import (get_dataframe_from_html,
                            get_dataframe_from_url,
                            get_urldataframe_from_url,
                            delete_divs_by_classes
                           )
from libtech_lib.generic.commons import (standardize_dates_in_dataframe,
                     insert_finyear_in_dataframe,
                     get_default_start_fin_year,
                     get_params_from_url,
                     get_percentage,
                     get_fto_finyear
                    )

def fetch_muster_details(logger, func_args, thread_name=None):
    """Given a muster URL, this program will fetch muster details"""
    lobj = func_args[0]
    url = func_args[1]
    cookies = func_args[2]
    muster_no = func_args[3]
    finyear = func_args[4]
    block_code = func_args[5]
    muster_column_name_dict = func_args[6]
    extract_dict = {}
    extract_dict['pattern'] = f"{lobj.state_short_code}-"
    extract_dict['table_id'] = "ctl00_ContentPlaceHolder1_grdShowRecords"
    extract_dict['split_cell_array'] = [1]
    logger.info(f"Currently processing {url}")
    dataframe = get_dataframe_from_url(logger, url, mydict=extract_dict,
                                       cookies=cookies)
    columns_to_keep = []
    for column_name in dataframe.columns:
        if not column_name.isdigit():
            columns_to_keep.append(column_name)
    dataframe = dataframe[columns_to_keep]
    dataframe['muster_no'] = muster_no
    dataframe['finyear'] = finyear
    dataframe['block_code'] = block_code
    ##Now we will have to build a dictionary to rename the columns
    column_keys = muster_column_name_dict.keys()
    rename_dict = {}
    for column_name in dataframe.columns:
        if column_name in column_keys:
            rename_dict[column_name] = muster_column_name_dict[column_name]
    dataframe = dataframe.rename(columns=rename_dict)
    return dataframe

def fetch_muster_urls(logger, func_args, thread_name=None):
    """This will get muster url from work urls"""
    lobj = func_args[0]
    url = func_args[1]
    cookies = func_args[2]
    extract_dict = {}
    extract_dict['pattern'] = "musternew.aspx"
    extract_dict['url_prefix'] = f"http://{lobj.crawl_ip}/Netnrega/placeHolder1/placeHolder2/"
    dataframe = get_urldataframe_from_url(logger, url, mydict=extract_dict,
                                          cookies=cookies)
    return dataframe
def fetch_rejection_details(logger, func_args, thread_name=None):

    """This will fetch rejection details given the reference url"""
    lobj = func_args[0]
    url = func_args[1]
    dataframe = None
    column_headers = ["wagelist_no", "jobcard", "applicant_no", "name",
                      "work_code", "work_name", "muster_no", "reference_no",
                      "rejection_status", "rejection_reason", "process_date", "fto_no",
                      "rejection_serial_no"]
    extract_dict = {}
    extract_dict['pattern'] = "Reference No"
    extract_dict['column_headers'] = column_headers
    dataframe = get_dataframe_from_url(logger, url, mydict=extract_dict)
    attempt_count = 0
    for index, row in dataframe.iterrows():
        fto_no = row.get("fto_no", None)
        if fto_no is None:
            finyear = ''
        else:
            finyear = get_fto_finyear(logger, fto_no)
        attempt_count = attempt_count + 1
        if attempt_count == len(dataframe):
            record_status = "current"
        else:
            record_status = "archive"
        dataframe.loc[index, 'attempt_count'] = attempt_count
        dataframe.loc[index, 'record_status'] = record_status
        dataframe.loc[index, 'fto_finyear'] = finyear
    return dataframe
def fetch_jobcard_details(logger, func_args, thread_name=None):
    """Will fetch jobcard_details from the jobcard url"""
    column_headers = ['srno', 'name', 'work_date', 'noOfDays', 'work_name',
                      'work_name_url', 'muster_no', 'muster_no_url',
                      'amount', 'payment_due']
    lobj = func_args[0]
    url = func_args[1]
    cookies = func_args[2]
    response = requests.get(url, cookies=cookies)
    dataframe = None
    if response.status_code == 200:
        myhtml = response.content
        extract_dict = {}
        extract_dict['pattern'] = "Payment Due"
        extract_dict['column_headers'] = column_headers
        extract_dict['extract_url_array'] = [4, 5]
        extract_dict['url_prefix'] = f'http://{lobj.crawl_ip}/netnrega/placeHolder1/'
        dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    if dataframe is not None:
        ###Now we will have to delete unnecessary rows
        rows_to_delete = []
        for index, row in dataframe.iterrows():
            srno = row.get('srno', None)
            if srno is None:
                rows_to_delete.append(index)
            elif not srno.isdigit():
                rows_to_delete.append(index)
        dataframe = dataframe.drop(rows_to_delete)
        dataframe = dataframe.reset_index(drop=True)
        # Standarize all dates
        #date_dict = {}
        #date_dict['work_date'] = '%d/%m/%Y'
        #dataframe = standardize_dates_in_dataframe(logger, dataframe, date_dict)
        #Insert Financial Year
        dataframe = insert_finyear_in_dataframe(logger, dataframe, "work_date")
        dataframe = dataframe.astype({"finyear": int})
        dataframe = dataframe[dataframe['finyear'] >=
                              get_default_start_fin_year()]
    else:
        dataframe = None
    return dataframe

def fetch_rejected_stats(logger, func_args, thread_name=None):
    """This function will fetch Rejected statistics"""
    lobj = func_args[0]
    url = func_args[1]
    finyear = func_args[2]
    extract_dict = func_args[3]
    response = requests.get(url)
    logger.info(url)
    dataframe = None
    fin_agency = "bank"
    param_name_array = ["state_code", "district_code", "block_code",
                        "state_name", "district_name", "block_name"]
    col_list = ['state_code', 'state_name', 'district_code', 'district_name',
                'block_code', 'block_name', 'finyear', 'fin_agency',
                'total', 'rejected', 'invalid', 'processed',
                'rejected_percentage', 'invalid_percentage',
                'second_singnatory_fto_url', 'invalid_url', 'rejected_url',
                'total_fto_generated', 'ss_fto_signed'
                ]
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'http://mnregaweb4.nic.in',
        'Connection': 'keep-alive',
        'Referer': url,
        'Upgrade-Insecure-Requests': '1',
    }

    data = {
      '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$RBtnLstIsEfms$1',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE':'',
      '__VIEWSTATEENCRYPTED': '',
      'ctl00$ContentPlaceHolder1$RBtnLst': 'W',
      'ctl00$ContentPlaceHolder1$RBtnLstIsEfms': 'P',
      'ctl00$ContentPlaceHolder1$HiddenField1': ''
    }
    
    
    dataframe_dict = {}
    dataframe_array = []
    view_state = None
    if response.status_code == 200:
        myhtml = response.content
        mysoup = BeautifulSoup(myhtml, "lxml")
        view_state = mysoup.find(id='__VIEWSTATE').get('value')
        dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
        if dataframe is not None:
            dataframe_dict['bank'] = dataframe
    po_view_state = None
    if view_state is not None:
        data['__VIEWSTATE'] = view_state
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            myhtml = response.content
            mysoup = BeautifulSoup(myhtml, "lxml")
            po_view_state = mysoup.find(id='__VIEWSTATE').get('value')
            dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
            if dataframe is not None:
                dataframe_dict['postoffice'] = dataframe
    if po_view_state is not None:
        data['__VIEWSTATE'] = po_view_state
        data['ctl00$ContentPlaceHolder1$RBtnLstIsEfms'] = "C"
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            myhtml = response.content
            mysoup = BeautifulSoup(myhtml, "lxml")
            dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
            if dataframe is not None:
                dataframe_dict['cobank'] = dataframe


    for fin_agency, dataframe in dataframe_dict.items():
        to_delete_rows = []
        for index, row in dataframe.iterrows():
            block = row.get("block", None)
            if block == "Total":
                to_delete_rows.append(index)
            fto_url = row.get("second_singnatory_fto_url", url)
            param_dict = get_params_from_url(logger, fto_url, param_name_array)
            for param_name in param_name_array:
                dataframe.loc[index, param_name] = param_dict.get(param_name,
                                                                  "")
            total = int(row.get("total", 0))
            rejected = int(row.get("rejected", 0))
            invalid = int(row.get("invalid", 0))
            rejected_percentage = get_percentage(rejected, total)
            invalid_percentage = get_percentage(invalid, total)
            dataframe.loc[index, "rejected_percentage"] = rejected_percentage
            dataframe.loc[index, "invalid_percentage"] = invalid_percentage

            dataframe.loc[index, "finyear"] = finyear
            dataframe.loc[index, "fin_agency"] = fin_agency
            
        dataframe = dataframe.drop(to_delete_rows)
        if len(dataframe) > 0:
            dataframe = dataframe[col_list]
            dataframe_array.append(dataframe)
    dataframe = pd.concat(dataframe_array)
    return dataframe

def get_block_rejected_transactions(lobj, logger):
    """This function will fetch all theblock rejected transactions"""
    #As a first step we need to get all the list of jobcards for that block
    panchayat_ids = lobj.get_all_panchayat_ids(logger)
    return dataframe

def fetch_table_from_url(logger, func_args, thread_name=None):
    """This function will fetch the table from URL based on extract Dict"""
    lobj = func_args[0]
    url = func_args[1]
    extract_dict = func_args[3]
    response = requests.get(url)
    logger.info(url)
    logger.info(response.status_code)
    dataframe = None
    if response.status_code == 200:
        myhtml = response.content
        dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
        logger.info(dataframe.columns)
        logger.info(dataframe.head())
    return dataframe
def parse_save_insidene(logger, func_args, thread_name=None):
    """Downloads and Saves data from Inside NE Magazine"""
    headers = func_args[0]
    outfile = func_args[1]
    post_dict = func_args[2]
    url = post_dict.get("post_link", None)
    post_title = post_dict.get("post_title", '')
    post_date = post_dict.get("post_date", '')
    post_content = ''
    post_div = None
    if url is not None:
        response = requests.get(url, headers=headers)
        logger.info(response.status_code)
        if response.status_code == 200:
            mysoup = BeautifulSoup(response.content, "lxml")
            post_div = mysoup.find("div", attrs={"class" : "td-post-content"})
        if post_div is not None:
            class_array = ["code-block", "google-auto-placed",
                           "addtoany_share_save_container",
                           "jp-relatedposts"]
            post_div = delete_divs_by_classes(logger, post_div, class_array)
            code_div = post_div.find("div", attrs={"class" : "code-block"})
            strong_p = post_div.findAll("strong")
            for strong in strong_p:
                strong_text = strong.text.lstrip().rstrip()
                strong_a = strong.find("a")
                if ("ALSO READ:" in strong_text) and (strong_a is not None):
                    parent_p = strong.parent
                    parent_p.decompose()
            em_p = post_div.findAll("em")
            for strong in em_p:
                strong_text = strong.text.lstrip().rstrip()
                if ("Support Inside Northeast" in strong_text):
                    parent_p = strong.parent
                    parent_p.decompose()


            paras = post_div.findAll("p")
            for para in paras:
                para_text = para.text.lstrip().rstrip()
                post_content = f"{post_content}{para_text}\n"
        post_data = {}
        post_data['post_title'] = post_title
        post_data['post_link'] = url
        post_data['post_date'] = post_date
        post_data['post_content'] = post_content
        with open(outfile, 'w', encoding='utf8') as json_file:
            json.dump(post_data, json_file, indent=4, ensure_ascii=False)
    return None
 
