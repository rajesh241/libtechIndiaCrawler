"""Functions related to html processing"""
#pylint: disable-msg = too-many-locals
#pylint: disable-msg = too-many-nested-blocks
#pylint: disable-msg = too-many-branches
#pylint: disable-msg = too-many-statements
from urllib.parse import urljoin
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

def request_with_retry_timeout(logger, url, data=None, headers=None, params=None, cookies=None,
                 timeout = 5, max_retry=5, method="post"):
    """This is the wrapper function for request post method."""
    retry = 0
    res = None
    sleep_timeout = 2
    response = None
    while (retry < max_retry):
        try:
            if method == "post":
                response = requests.post(url, data=data, timeout=timeout, params=params, cookies=cookies, headers=headers)
            else:
                response = requests.get(url, timeout=timeout, params=params, cookies=cookies, headers=headers)
            if response.status_code == 200:
                error = False
            else:
                error = True
        except requests.Timeout:
            # back off and retry
            error = True
        except requests.ConnectionError:
            error = True
        if (error == True):
            retry = retry + 1
            time.sleep(sleep_timeout)
            sleep_timeout += 5
        else:
            retry = max_retry
    return response
def nic_download_page(logger, url, session=None, cookies=None, params=None, headers=None):
    max_retry = 5
    retry = 0
    res = None
    timeout = 2
    while (retry < max_retry):
        try:
            if session:
                logger.debug(f'Attempting using *session* to fetch the URL[{url}] for the {retry+1} time')
                res = session.get(url, cookies=cookies, params=params,
                                   headers=headers, verify=False)
            else:
                logger.debug(f'Attempting using *requests* to fetch the URL[{url}] for the {retry+1} time')
                res = requests.get(url, cookies=cookies, params=params,
                                   headers=headers, verify=False)
            if res.status_code == 200:
                retry = max_retry
            else:
                retry = retry + 1
                timeout += 5
                time.sleep(timeout)
        except Exception as e:
            retry = retry + 1
            timeout += 5
            time.sleep(timeout)
            logger.warning(f'Need to retry. Failed {retry} time(s). Exception[{e}]')
            logger.warning(f'Waiting for {timeout} seconds...')
    return res

def find_urls_containing_text(myhtml, mytext, url_prefix=None):
    """This function will find all the urls cotaining the mentioned text"""
    url = None
    urls = []
    if url_prefix is None:
        url_prefix = ''
    mysoup = BeautifulSoup(myhtml, "lxml")
    links = mysoup.findAll("a")
    for link in links:
        href = link.get("href", "")
        if mytext in href:
            url = url_prefix + href
            urls.append(url)
    return urls
 
def find_url_containing_text(myhtml, mytext, url_prefix=None):
    """This function will find the first url cotaining the mentioned text"""
    url = None
    if url_prefix is None:
        url_prefix = ''
    mysoup = BeautifulSoup(myhtml, "lxml")
    links = mysoup.findAll("a")
    for link in links:
        href = link.get("href", "")
        if mytext in href:
            url = url_prefix + href
            break
    return url
def get_urldataframe_from_html(logger, myhtml, mydict=None):
    """this will harvest urls from html based on the prameters specified in
    mydict"""
    dataframe = None
    csv_array = []
    col_headers = ["muster_no", "muster_url"]
    text_pattern = mydict.get('pattern', None)
    url_prefix = mydict.get('url_prefix', None)
    mysoup = BeautifulSoup(myhtml, "html5lib")
    urls = mysoup.findAll("a")
    for url in urls:
        href = url.get('href')
        if text_pattern in href:
            muster_url = url_prefix + href
            muster_no = url.text
            csv_array.append([muster_no, muster_url])
    dataframe = pd.DataFrame(csv_array, columns=col_headers)
    return dataframe

def get_urldataframe_from_url(logger, url, mydict=None, cookies=None):
    """This will harvest urls from the given url based on extract dict
    parameters"""
    if cookies is None:
        response = requests.get(url)
    else:
        response = requests.get(url, cookies=cookies)
    dataframe = None
    if response.status_code == 200:
        myhtml = response.content
        dataframe = get_urldataframe_from_html(logger, myhtml, mydict=mydict)
    return dataframe

def get_dataframe_from_url(logger, url, mydict=None, cookies=None):
    """Gets the dataframe from the url based on the parameters specified
    in mydict
    It uses get_dataframe_from_html function
    for details on parameters refer to get_dataframe_from_html docstring
    """
    if cookies is not None:
        response = requests.get(url, cookies=cookies)
    else:
        response = requests.get(url)
    dataframe = None
    if response.status_code == 200:
        myhtml = response.content
        dataframe = get_dataframe_from_html(logger, myhtml, mydict=mydict)
    return dataframe

def get_dataframe_from_html(logger, myhtml, mydict=None):
    """This function will create a dataframe from html table
    mydict has set of values which are used to determine which
    table to extract.
    mydict parameters may contain either or all of
    'pattern' : Any text pattern that can be matched to the table
    'extract_url_array' : The array which specifies from which column
                          to extract url
    'column_headers' : The option column headers for the table
    'header_row' : The row nuber from which column header needs
            to be picked, defaults to 0
    'base_url' : base url is used in case of relative url paths.
              the parsed url from the html table will be joined
              with base_url
    In case of multiple matches the program will return dataframe
    from the first matched table
    """
    dataframe = None
    text_pattern = mydict.get('pattern', None)
    table_id = mydict.get('table_id', None)
    table_id_array = mydict.get('table_id_array', None)
    mysoup = BeautifulSoup(myhtml, "lxml")
    mysoup = BeautifulSoup(myhtml, "html5lib")
    tables = mysoup.findAll('table')
    logger.debug(f"Number of tables found is {len(tables)}")
    matched_tables = []
    #Match the table agains the specified pattern
    if table_id_array is None:
        if table_id is not None:
            table_id_array = [table_id]
    #logger.info(f"table id array is {table_id_array}")
    if table_id_array is not None:
        for table_id in table_id_array:
            matched_tables = mysoup.findAll('table', id=table_id)
            if len(matched_tables) > 0:
                break
    elif text_pattern is None:
        matched_tables = tables
    else:
        for table in tables:
            if text_pattern in str(table):
                matched_tables.append(table)

    #We will now see if any matched table has been found
    if len(matched_tables) > 0:
        my_table = matched_tables[0]
    else:
        my_table = None
        logger.debug("Did not find any table !!!")

    #If table found we will extract the rows and columns
    if my_table is not None:
        dataframe_columns = []
        dataframe_array = []
        base_url = mydict.get("base_url", None)
        url_prefix = mydict.get("url_prefix", None)
        header_row = mydict.get("header_row", 0)
        data_start_row = mydict.get("data_start_row", 1)
        column_headers = mydict.get("column_headers", None)
        extract_url_array = mydict.get("extract_url_array", [])
        split_cell_array = mydict.get("split_cell_array", [])
        rows = my_table.findAll('tr')
        for i, row in enumerate(rows):
            ##Extracting the table headers
            if i == header_row:
                if column_headers is not None:
                    dataframe_columns = column_headers
                else:
                    cols = row.findAll('th')
                    if len(cols) == 0:
                        cols = row.findAll('td')
                    for j, col in enumerate(cols):
                        col_text = col.text.lstrip().rstrip()
                        dataframe_columns.append(col_text)
                        if j in extract_url_array:
                            dataframe_columns.append(f"{col_text}_url")
                        if j in split_cell_array:
                            dataframe_columns.append(f"{col_text}_1")

            ##Extracting the table data
            if i >= data_start_row:
                row_data = []
                cols = row.findAll(['th', 'td'])
                if len(cols) > 0:
                    for j, col in enumerate(cols):
                        if j in split_cell_array:
                            col_contents = col.get_text(strip=True, separator=',')
                            col_array = col_contents.split(",")
                            row_data.append(col_array[0])
                            if len(col_array) == 1:
                                row_data.append('')
                            else:
                                row_data.append(col_array[1])
                        else:
                            col_text = col.text.lstrip().rstrip()
                            row_data.append(col_text)
                        if j in extract_url_array:
                            elem = col.find("a")
                            if elem is not None:
                                col_url = elem['href']
                                if url_prefix is not None:
                                    col_url = url_prefix + col_url
                                elif base_url is not None:
                                    col_url = urljoin(base_url, col_url)
                            else:
                                col_url = ""
                            row_data.append(col_url)
                    dataframe_array.append(row_data)
        if len(dataframe_array) > 0:
            if len(dataframe_array[0]) > len(dataframe_columns):
                #logger.info("Length Mismatch")
                diff = len(dataframe_array[0]) - len(dataframe_columns)
                for i in range(1,diff+1):
                    dataframe_columns.append("")
        dataframe = pd.DataFrame(dataframe_array, columns=dataframe_columns)
    return dataframe



def delete_divs_by_classes(logger, mysoup, class_array):
    '''This function will Delete all the divs which have the class attributes
    as in class_array'''
    logger.info(f"Will delete unnecessary divs")
    for my_class in class_array:
        my_div = mysoup.find("div", attrs={"class" : my_class})
        if my_div is not None:
            my_div.decompose()
    return mysoup

def get_options_list(logger, mysoup, select_id=None):
    """This will fetch the option dict given teh select id"""
    my_select = mysoup.find("select", id=select_id)
    options_list = []
    #my_select = mysoup.find("select")
    if my_select is not None:
        options = my_select.findAll("option")
        for option in options:
            my_dict = {}
            my_dict["name"] = option.text
            my_dict["value"] = option["value"]
            options_list.append(my_dict)

    return options_list
