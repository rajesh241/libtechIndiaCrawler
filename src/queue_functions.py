"""This has all the functions that need tobe executed in the queue"""
import requests
import pandas as pd
from html_functions import get_dataframe_from_html, get_dataframe_from_url
from commons import (standardize_dates_in_dataframe,
                     insert_finyear_in_dataframe,
                     get_default_start_fin_year,
                     get_fto_finyear
                    )
def fetch_rejection_details(logger, func_args, thread_name=None):

    """This will fetch rejection details given the reference url"""
    lobj = func_args[0]
    url = func_args[1]
    base_reference_no = func_args[3]
    amount = func_args[2]
    info_dict = func_args[4]
    dataframe = None
    column_headers = ["wagelist_no", "jobcard", "applicant_no", "name",
                      "work_code", "work_name", "muster_no", "reference_no",
                      "rejection_status", "rejection_reason", "process_date", "fto_no",
                      "rejection_serial_no"]
    extract_dict = {}
    extract_dict['pattern'] = "Reference No"
    extract_dict['column_headers'] = column_headers
    dataframe = get_dataframe_from_url(logger, url, mydict=extract_dict)
    dataframe['amount'] = amount
    keys = info_dict.keys()
    for key in keys:
        dataframe[key] = ''
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
        reference_no = row['reference_no']
        if reference_no == base_reference_no:
            for key, value in info_dict.items():
                dataframe.loc[index, key] = value
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
        date_dict = {}
        date_dict['work_date'] = '%d/%m/%Y'
        dataframe = standardize_dates_in_dataframe(logger, dataframe, date_dict)
        #Insert Financial Year
        dataframe = insert_finyear_in_dataframe(logger, dataframe, "work_date")
        dataframe = dataframe.astype({"finyear": int})
        dataframe = dataframe[dataframe['finyear'] >=
                              get_default_start_fin_year()]
    else:
        dataframe = None
    return dataframe
