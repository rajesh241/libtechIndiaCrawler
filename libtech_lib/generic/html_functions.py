"""Functions related to html processing"""
#pylint: disable-msg = too-many-locals
#pylint: disable-msg = too-many-nested-blocks
#pylint: disable-msg = too-many-branches
#pylint: disable-msg = too-many-statements
from urllib.parse import urljoin
import requests
import pandas as pd
from bs4 import BeautifulSoup

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
    mysoup = BeautifulSoup(myhtml, "lxml")
    mysoup = BeautifulSoup(myhtml, "html5lib")
    tables = mysoup.findAll('table')
    matched_tables = []
    #Match the table agains the specified pattern
    if table_id is not None:
        matched_tables = mysoup.findAll('table', id=table_id)
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
    if my_table is not None:
        logger.debug(f"found the table")
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
                cols = row.findAll('td')
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

        dataframe = pd.DataFrame(dataframe_array, columns=dataframe_columns)
    return dataframe