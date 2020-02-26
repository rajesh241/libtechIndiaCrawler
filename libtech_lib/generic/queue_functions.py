"""This has all the functions that need tobe executed in the queue"""
import requests
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
 
