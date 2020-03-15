"""This module has common functions which are used
throughout the library"""
#pylint: disable-msg = too-many-locals
#pylint: disable-msg = too-many-nested-blocks
#pylint: disable-msg = too-many-branches
#pylint: disable-msg = too-many-statements
import logging
import urllib.parse as urlparse
from urllib.parse import parse_qs
import datetime
import time
import re
def logger_fetch(level=None):
    """Initialization of Logger, which can be used by all functions"""
    logger = logging.getLogger(__name__)
    default_log_level = "debug"
    if not level:
        level = default_log_level

    log_format = ('%(asctime)s:[%(name)s|%(module)s|%(funcName)s'
                  '|%(lineno)s|%(levelname)s]: %(message)s')
    if level:
        numeric_level = getattr(logging, level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % level)
        logger.setLevel(numeric_level)
    console_logger = logging.StreamHandler()
    formatter = logging.Formatter(log_format)
    console_logger.setFormatter(formatter)
    logger.addHandler(console_logger)
    return logger

def get_current_finyear():
    """Returns the current financial year in two digit format"""
    now = datetime.datetime.now()
    if now.month > 3:
        year = now.year + 1
    else:
        year = now.year
    return year % 100

def get_default_start_fin_year():
    """Gets the default start finyear,
    which is three years before
    for example if current finyear is 20,
    start fin year would be 17"""
    cur_fin_year = get_current_finyear()
    start_fin_year = cur_fin_year - 3
    return start_fin_year

def get_full_finyear(finyear):
    """Returns the full financial year as is used in NIC Website
    for example finyear of 19 would return 2018-2019"""
    finyear_minus1 = int(finyear) -1
    full_finyear = f"20{finyear_minus1}-20{finyear}"
    return full_finyear

def insert_finyear_in_dataframe(logger, dataframe, date_column_name,
                                date_format=None):
    """This function inserts the financial yar in the dataframe,
    based on the date value in date_column_name
    """
    if date_format is None:
        date_format = '%d/%m/%Y'
    dataframe['finyear'] = ''
    for index, row in dataframe.iterrows():
        cur_date = row.get(date_column_name, None)
        if cur_date is None:
            finyear = ''
        else:
            cur_date_object = get_date_object(cur_date, date_format)
            finyear = get_finyear_from_date(date_object=cur_date_object)
        dataframe.loc[index, "finyear"] = finyear
    return dataframe

def standardize_dates_in_dataframe(logger, dataframe, date_dict):
    """This module will standardize dates in dataframe
    It takes in date dict, which has column_name as key
    and value is the format of date that is input"""
    column_names = date_dict.keys()
    for index, row in dataframe.iterrows():
        for column_name in column_names:
            input_date_format = date_dict.get(column_name, None)
            input_date_string = row.get(column_name, "")
            out_date_string = correct_date_format(logger, input_date_string,
                                                  date_format=input_date_format)
            dataframe.loc[index, column_name] = out_date_string
    return dataframe

def correct_date_format(logger, input_date_string, date_format=None):
    """puts the incoming date string, in particular dateformat in to standard
    date
    if date_format is abset it assumes %d/%m/%Y or %d-%m-%Y
    """
    if input_date_string != '':
        try:
            if date_format is not None:
                out_date = time.strptime(input_date_string, date_format)
            elif "/" in input_date_string:
                out_date = time.strptime(input_date_string, '%d/%m/%Y')
            else:
                out_date = time.strptime(input_date_string, '%d-%m-%Y')
            out_date = time.strftime('%d/%m/%Y', out_date)
        except:
            out_date = None
    else:
        out_date = None
    return out_date

def get_finyear_from_date(date_object=None):
    """Returns the financial year as two digit year from the date"""
    if date_object is None:
        now = datetime.datetime.now()
    else:
        now = date_object
    if now.month > 3:
        year = now.year + 1
    else:
        year = now.year
    finyear = str(year % 100)
    if len(finyear) == 1:
        finyear = "0" + finyear
    return finyear

def get_date_object(date_string, date_format=None):
    """returns a date object from python date"""
    try:
        if date_format is not None:
            my_date = datetime.datetime.strptime(date_string, date_format).date()
        elif "/" in date_string:
            my_date = datetime.datetime.strptime(date_string, '%d/%m/%Y').date()
        else:
            my_date = datetime.datetime.strptime(date_string, '%d-%m-%Y').date()
    except:
        my_date = None
    return my_date

def insert_location_details(logger, lobj, dataframe):
    """Will insert the location columns in the dataframe
    based on the class attributes"""
    if lobj.location_type == 'state':
        column_array = ['state_code', 'state_name']
    elif lobj.location_type == 'district':
        column_array = ['state_code', 'state_name',
                        'district_code', 'district_name']
    elif lobj.location_type == 'block':
        column_array = ['state_code', 'state_name',
                        'district_code', 'district_name',
                        'block_code', 'block_name']
    elif lobj.location_type == 'panchayat':
        column_array = ['state_code', 'state_name',
                        'district_code', 'district_name',
                        'block_code', 'block_name',
                        'panchayat_name', 'panchayat_code']
    else:
        column_array = []
    for column_name in column_array:
        dataframe[column_name] = getattr(lobj, column_name)
    return dataframe

def get_fto_finyear(logger, fto_no):
    """This function uses regular expressions to extract the financial year
    from fto_no"""
    date_regex = re.compile(r'_\d{6}((FTO)|(APB))')
    finyear = ''
    match_object = date_regex.search(fto_no)
    if match_object is not None:
        date_string = match_object.group()
        date_string = date_string[1:7]
        date_object = get_date_object(date_string, "%d%m%y")
        finyear = get_finyear_from_date(date_object)
    return finyear

def get_finyear_from_muster_url(logger, url, finyear_regex):
    """Will extract the financial year from muster url using regex"""
    match_object = finyear_regex.search(url)
    finyear = None
    if match_object is not None:
        pattern = match_object.group()
        finyear = pattern[-2:]
    return finyear

def get_percentage(value, total, round_digits=0):
    """Returns the percentage"""
    if total == 0:
        return None
    percentage = round(value*100/total, round_digits)
    return percentage

def get_params_from_url(logger, url, param_name_array):
    """This function will return the parameters from the url based on the
    param_name_array"""
    param_dict = {}
    parsed = urlparse.urlparse(url)
    params_dict = parse_qs(parsed.query)
    for param_name in param_name_array:
        param_value  = params_dict.get(param_name, [''])[0]
        param_dict[param_name] = param_value
    return param_dict

