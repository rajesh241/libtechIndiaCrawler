"""This has all the functions that need tobe executed in the queue"""
#pylint: disable-msg = too-many-locals
#pylint: disable-msg = too-many-branches
#pylint: disable-msg = too-many-statements
#pylint: disable-msg = line-too-long
import json
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from libtech_lib.generic.html_functions import (get_dataframe_from_html,
                                                get_dataframe_from_url,
                                                get_urldataframe_from_url,
                                                delete_divs_by_classes
                                               )
from libtech_lib.generic.commons import (insert_finyear_in_dataframe,
                                         get_default_start_fin_year,
                                         get_params_from_url,
                                         get_percentage,
                                         ap_nrega_download_page,
                                         get_date_object,
                                         get_fto_finyear
                                        )

def fetch_new_muster(logger, func_args, thread_name=None):
    """This will fetch muster from new url"""
    url = func_args[0]
    cookies = func_args[1]
    view_state = func_args[2]
    validation = func_args[3]
    logger.info(url)
    logger.info(cookies)
   #finyear = func_args[5]
   #muster_code = func_args[4]
   #work_code = func_args[6]
   #muster_param = func_args[7]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:80.0) Gecko/20100101 Firefox/80.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'X-MicrosoftAjax': 'Delta=true',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
        'Origin': 'https://mnregaweb4.nic.in',
        'Connection': 'keep-alive',
        'Referer': url
    }


    data = {
        'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel2|ctl00$ContentPlaceHolder1$ddlMsrno',
        'ctl00$ContentPlaceHolder1$ddlFinYear': '2020-2021',
        'ctl00$ContentPlaceHolder1$btnfill': 'btnfill',
        'ctl00$ContentPlaceHolder1$txtSearch': '',
        'ctl00$ContentPlaceHolder1$ddlwork': '2724007282/DP/112908165812',
        'ctl00$ContentPlaceHolder1$ddlMsrno': '6240~~20/10/2018~~4/11/2018',
        '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlMsrno',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': view_state,
        '__EVENTVALIDATION': validation,
        '__VIEWSTATEGENERATOR': '75DEE431',
        '__VIEWSTATEENCRYPTED': '',
        '__ASYNCPOST': 'true',
        '': ''
    }

    response = requests.post(url, headers=headers,  cookies=cookies, data=data)
    logger.info(f" response status code is {response.status_code}")
    if response.status_code == 200:
        myhtml = response.content
        with open("/tmp/muster.html", "wb") as f:
            f.write(myhtml)

#NB. Original query string below. It seems impossible to parse and
#reproduce query strings 100% accurately so the one below is given
#in case the reproduced version is not "correct".
# response = requests.post('https://mnregaweb4.nic.in/netnrega/Citizen_html/Musternew.aspx?id=2&lflag=eng&ExeL=GP&fin_year=2018-2019&state_code=27&district_code=27&block_code=2724007&panchayat_code=2724007283&State_name=RAJASTHAN&District_name=BHILWARA&Block_name=SAHADA&panchayat_name=%u0905%u0930%u0928%u093f%u092f%u093e+%u0916%u093e%u0932%u0938%u093e&Digest=wkiBNlowEM0kJzPNrxXgqA', headers=headers, cookies=cookies, data=data)




def fetch_muster_details(logger, func_args, thread_name=None):
    """Given a muster URL, this program will fetch muster details"""
    lobj = func_args[0]
    url = func_args[1]
    cookies = func_args[2]
    muster_no = func_args[3]
    finyear = func_args[4]
    block_code = func_args[5]
    muster_column_name_dict = func_args[6]
    muster_code = func_args[7]
    extract_dict = {}
    extract_dict['pattern'] = f"{lobj.state_short_code}-"
    extract_dict['table_id_array'] = ["ctl00_ContentPlaceHolder1_grdShowRecords",
                                      "ContentPlaceHolder1_grdShowRecords"]
    extract_dict['split_cell_array'] = [1]
    logger.debug(f"Currently processing {url} by {thread_name}")
    dataframe = get_dataframe_from_url(logger, url, mydict=extract_dict,
                                       cookies=cookies)
    #logger.info(f"extracted dataframe columns {dataframe.columns}")
    columns_to_keep = []
    for column_name in dataframe.columns:
        if not column_name.isdigit():
            columns_to_keep.append(column_name)
    dataframe = dataframe[columns_to_keep]
    #dataframe['muster_no'] = muster_no
    #dataframe['finyear'] = finyear
    #dataframe['block_code'] = block_code
    dataframe['muster_code'] = muster_code
    ##Now we will have to build a dictionary to rename the columns
    column_keys = muster_column_name_dict.keys()
    rename_dict = {}
    for column_name in dataframe.columns:
        if column_name in column_keys:
            rename_dict[column_name] = muster_column_name_dict[column_name]
    dataframe = dataframe.rename(columns=rename_dict)
    rows_to_delete = []
    is_complete = 1
    for index, row in dataframe.iterrows():
        sr_no = row.get("muster_index", None)
        credited_date = row.get("credited_date", None)
        if (sr_no is None) or (not sr_no.isdigit()):
            rows_to_delete.append(index)
        else:
            credited_date_object = get_date_object(credited_date)
            if credited_date_object is None:
                is_complete = 0
        name_relationship = row['name_relationship']
        try:
            relationship = re.search(r'\((.*?)\)', name_relationship).group(1)
        except:
            relationship = ''
        name = name_relationship.replace(f"({relationship})", "")
        dataframe.loc[index, 'name'] = name
        dataframe.loc[index, 'relationship'] = relationship
    dataframe = dataframe.drop(rows_to_delete)
    dataframe['is_complete'] = is_complete
    return dataframe

def fetch_muster_urls(logger, func_args, thread_name=None):
    """This will get muster url from work urls"""
    lobj = func_args[0]
    url = func_args[1]
    cookies = func_args[2]
    logger.debug(f"fetch muster url {url} in {thread_name}")
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
    logger.debug(f"Fetch rejection details for {lobj.code} in {thread_name}")
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
    logger.debug(f"Fetch data for {lobj.code} with {thread_name}")
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
    logger.debug(f"Fetch data for {lobj.code} with {thread_name}")
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
    logger.info(f"Processing {lobj.code}")
    dataframe = None
   # panchayat_ids = lobj.get_all_panchayat_ids(logger)
    return dataframe

def ap_fetch_table_from_url(logger, func_args, thread_name):
    """This function is specifically for AP and it would get the table from the
    given url"""
    lobj = func_args[0]
    url = func_args[1]
    session = func_args[2]
    headers = func_args[3]
    params = func_args[4]
    cookies = func_args[5]
    extract_dict = func_args[6]
    static_col_names = func_args[7]
    static_col_values = func_args[8]
    response = ap_nrega_download_page(
        logger, url, session=session,
        headers=headers, params=params,
        cookies=cookies)

    if (not response) or (response.status_code != 200):
        return None
    myhtml = response.content
    dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
    for index, col_name in enumerate(static_col_names):
        dataframe[col_name] = static_col_values[index]
    return dataframe

def fetch_table_from_url(logger, func_args, thread_name=None):
    """This function will fetch the table from URL based on extract Dict"""
    lobj = func_args[0]
    url = func_args[1]
    logger.debug(f"running {lobj.code} in {thread_name}")
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
    logger.info(f"Executing in {thread_name}")
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
            #code_div = post_div.find("div", attrs={"class" : "code-block"})
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
                if "Support Inside Northeast" in strong_text:
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
