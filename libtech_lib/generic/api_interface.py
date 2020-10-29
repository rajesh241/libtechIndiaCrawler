"""This module has all the functions required to access REST API
for the Django interface"""
import os
import datetime
from dateutil.parser import parse
from pathlib import Path
import json
import requests
import pandas as pd
import pytz

from libtech_lib.generic.aws import upload_s3

#HOMEDIR = str(Path.home())
#JSONCONFIGFILE = f"{HOMEDIR}/.libtech/crawlerConfig.json"
#with open(JSONCONFIGFILE) as CONFIG_file:
#    CONFIG = json.load(CONFIG_file)

#BASEURL = CONFIG['baseURL']
#BASEURL = "http://b.libtech.in:8181"
#API_USER_NAME = CONFIG['apiusername']
#API_PASSWORD = CONFIG['apipassword']
BASEURL = os.environ.get('LIBTECH_BACKEND_URL', None)
API_USER_NAME = os.environ.get('LIBTECH_API_USERNAME', None)
API_PASSWORD = os.environ.get('LIBTECH_API_PASSWORD', None)


AUTHENDPOINT = '%s/api/user/token/' % (BASEURL)
LOCATIONDATASTATUSURL = '%s/api/dataStatus/' % (BASEURL)
LOCATIONURL = '%s/api/public/location/' % (BASEURL)
TAGURL = '%s/api/public/tag/' % (BASEURL)
REPORTURL = '%s/api/public/report/' % (BASEURL)
GETREPORTURL = "%s/api/getReport/" % (BASEURL)
TASKQUEUEURL = '%s/api/public/queue/' % (BASEURL)

def get_authentication_token():
    """This function will get the authentication token based on the username
    and password as stored in the CONFIG file
    It is required to have a CONFIG file in the home directory which has
    username and password
    """
    data = {
        'email' : API_USER_NAME,
        'password' : API_PASSWORD
        }
    try:
        req = requests.post(AUTHENDPOINT, data=data)
        token = req.json()['access']
    except:
        token = None
    return token

def get_authentication_header(token=None):
    """Will create a authentication header from the token"""
    if token is None:
        token = get_authentication_token()
    headers = {
        'content-type':'application/json',
        "Authorization" : "Bearer " + token
        }
    return headers

def create_task(logger,  data):
    """This would create a task"""
    headers = get_authentication_header()
    res = requests.post(TASKQUEUEURL, headers=headers,
                         data=json.dumps(data))
    logger.debug(f"Patch status {res.status_code} and response {res.content}")
def update_task(logger,  patch_data):
    """function to update the task"""
    headers = get_authentication_header()
    res = requests.patch(TASKQUEUEURL, headers=headers,
                         data=json.dumps(patch_data))
    logger.debug(f"Patch status {res.status_code} and response {res.content}")
 
def get_task(logger, task_id=None):
    """This function will get the first task from the queue to execute"""
    headers = get_authentication_header()
    if task_id is not None:
      url="%s?id=%s" % (TASKQUEUEURL, str(task_id))
    else:
      #url="%s?is_done=0&status=inQueue&ordering=-priority,updated&limit=1" % (TASKQUEUEURL)
      url="%s?is_done=0&in_progress=0&ordering=-priority,updated&limit=1" % (TASKQUEUEURL)
    logger.info(url)
    res = requests.get(url, headers=headers)
    logger.info
    response = res.json()
    count = response.get("count", None)
    if task_id is not None:
      return_id = response.get("id", None)
      if return_id is None:
        task_dict = None
      else:
        task_dict = response
    elif ((count is None) or (count == 0)):
      task_dict= None
    else:
      task_dict = response['results'][0]
    return task_dict
 
def fetch_data(logger, url, return_type='json', params=None):
    """This implements requests get functionality and by default will return
    the json, if content is required, return_type has to be set to content"""
    if params is not None:
        res = requests.get(url, params=params)
    else:
        res = requests.get(url)
    if res.status_code == 200:
        if return_type == 'json':
            response = res.json()
        else:
            response = res.content
    else:
        logger.error(f"Get Failed {url} return status {res.status_code}")
        response = None
    return response

def api_update_crawl_accuracy(logger, location_id, accuracy, scheme="nrega"):
    """Given location ID, this function will update the crawl accuracy"""
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    india_now = utc_now.astimezone(pytz.timezone("Asia/Calcutta"))
    india_now_isoformat = india_now.isoformat()
    headers = get_authentication_header()
    data = {
        "id" : location_id,
        "accuracy" : accuracy,
        "last_crawl_date" : india_now_isoformat,
        "is_data_available" : 1
    }
    res = requests.patch(LOCATIONURL, headers=headers, data=json.dumps(data))
    logger.info(f"Patch status for data accuracy is {res.status_code}")
    
def api_location_update(logger, location_id, data, scheme="nrega"):
    """Given location ID, this function will update the data_json field"""
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    india_now = utc_now.astimezone(pytz.timezone("Asia/Calcutta"))
    india_now_isoformat = india_now.isoformat()
    headers = get_authentication_header()
    data = {
        "id" : location_id,
        "data_json" : data
    }
    res = requests.patch(LOCATIONURL, headers=headers, data=json.dumps(data))
def get_location_dict(logger, location_code=None, location_id=None, scheme=None):
    """Given a location Code for a database Location ID, this function will
    return a dict containing all the meta data avilable by querying the API"""
    if scheme is None:
        scheme = 'nrega'
    if (location_code is None) and (location_id is None):
        location_dict = None
    if location_id is not None:
        url = f"{LOCATIONURL}?id={location_id}"
        location_dict = fetch_data(logger, url)
    if location_code is not None:
        url = f"{LOCATIONURL}?code={location_code}&scheme={scheme}"
        response = fetch_data(logger, url)
        results = response.get("results", None)
        if results is None:
            location_dict = None
        elif len(results) != 1:
            location_dict = None
        else:
            location_dict = results[0]
    return location_dict

def api_get_tag_id(logger, tag_name):
    """Given a tag name this function will return tag ID"""
    params = {
        'name' : tag_name
    }
    response = fetch_data(logger, TAGURL, params=params)
    count = response.get("count", 0)
    child_location_array = []
    if count == 1:
        results = response.get('results')
        tag_id = results[0].get('id')
    else:
        tag_id = None
    return tag_id

def api_get_locations_by_params(logger, params):
    """Will get locations by params"""
    response = fetch_data(logger, LOCATIONURL, params=params)
    count = response.get("count", 0)
    child_location_array = []
    if count > 0:
        results = response.get('results')
        for res in results:
            code = res.get('code')
            child_location_array.append(code)
    return child_location_array
    
def api_get_tagged_locations(logger, tag_id, scheme=None):
    """This will fetch all the location Codes given a tag id
    """
    if scheme is None:
        scheme = 'nrega'
    params = {
        'libtech_tag' : tag_id,
        'scheme' : scheme,
        'limit' : 500
    }
    response = fetch_data(logger, LOCATIONURL, params=params)
    count = response.get("count", 0)
    child_location_array = []
    if count > 0:
        results = response.get('results')
        for res in results:
            code = res.get('code')
            child_location_array.append(code)
    return child_location_array


def api_get_tagged_locations_by_tag_name(logger, tag_name, scheme=None):
    """This will fetch all the location Codes given a tag id
    """
    if scheme is None:
        scheme = 'nrega'
    params = {
        'libtech_tag__name' : tag_name,
        'scheme' : scheme,
        'limit' : 500
    }
    response = fetch_data(logger, LOCATIONURL, params=params)
    count = response.get("count", 0)
    child_location_array = []
    if count > 0:
        results = response.get('results')
        for res in results:
            code = res.get('code')
            child_location_array.append(code)
    return child_location_array

def api_get_child_locations(logger, location_code, scheme=None):
    """Given a location code, it will return all the child locations
    Given a state code, it will retunr all district codes under that state
    given  a district code, it will retunr all block codes under that district
    """
    if scheme is None:
        scheme = 'nrega'
    params = {
        'parent_location__code' : location_code,
        'scheme' : scheme,
        'limit' : 500
    }
    response = fetch_data(logger, LOCATIONURL, params=params)
    count = response.get("count", 0)
    child_location_array = []
    if count > 0:
        results = response.get('results')
        for res in results:
            code = res.get('code')
            child_location_array.append(code)
    return child_location_array

def api_get_child_location_ids(logger, location_code, scheme=None):
    """Given a location code, it will return all the child location IDs
    Given a state code, it will retunr all district codes under that state
    given  a district code, it will retunr all block codes under that district
    """
    if scheme is None:
        scheme = 'nrega'
    params = {
        'parent_location__code' : location_code,
        'scheme' : scheme
    }
    response = fetch_data(logger, LOCATIONURL, params=params)
    count = response.get("count", 0)
    child_location_array = []
    if count > 0:
        results = response.get('results')
        for res in results:
            lid = res.get('id')
            child_location_array.append(lid)
    return child_location_array


def get_obj_id(logger, url, params=None):
    """Will return the object from the given URL
    If multiple objects are return from the url,
    this would return the first object"""
    response = fetch_data(logger, url, params=params)
    count = response.get("count", None)
    obj_id = None
    if count > 0:
        obj_id = response['results'][0]['id']
    return obj_id

def api_get_report_url(logger, location_id, report_type, finyear=None):
    """This function will get the report dataframe from the amazon S3"""
    report_url = None
    params = {
        'location' : location_id,
        'report_type' : report_type
    }
    if finyear is not None:
        params['finyear'] = finyear
    response = fetch_data(logger, REPORTURL, params=params)
    results = response.get("results", [])
    if len(results) > 0:
        report_dict = results[0]
        report_url = report_dict.get('report_url', None)
    return report_url

def api_get_report_last_updated(logger, location_code, report_type, finyear=None):
    """This function will get the report dataframe from the amazon S3"""
    last_updated = None
    params = {
        'location_code' : location_code,
        'report_type' : report_type
    }
    if finyear is not None:
        params['finyear'] = finyear
    response = fetch_data(logger, REPORTURL, params=params)
    results = response.get("results", [])
    if len(results) > 0:
        report_dict = results[0]
        last_updated = report_dict.get('updated', None)
        last_updated = parse(last_updated) 
    return last_updated


def api_get_report_urls(logger, location_id, report_type, finyear=None):
    """Gets the report urls """
    report_urls = []
    params = {
        'location' : location_id,
        'report_type' : report_type
    }
    if finyear is not None:
        params['finyear'] = finyear
    response = fetch_data(logger, REPORTURL, params=params)
    results = response.get("results", [])
    if len(results) > 0:
        report_dict = results[0]
        report_url = report_dict.get('report_url', None)
        excel_url = report_dict.get('excel_url', None)
        report_urls.append(report_url)
        report_urls.append(excel_url)
    return report_urls


def api_get_report_dataframe(logger, location_id, report_type,
                             finyear=None, index_col=0, dtype=None):
    """Gets the report dataframe"""
    report_url = api_get_report_url(
        logger, location_id, report_type, finyear=finyear)
    if report_url is not None:
        if dtype is None:
            dataframe = pd.read_csv(report_url, index_col=index_col)
        else:
            dataframe = pd.read_csv(
                report_url, index_col=index_col, dtype=dtype)
    else:
        dataframe = None
    return dataframe
def create_update_report(logger, location_id, report_type, data,
                         filename, finyear=None, health="unknown", remarks=''):
    """Updates report URL to the API Meta data
    if the report does not exists this will create the report
    or update to an existing report"""
    headers = get_authentication_header()
    #logger.debug(headers)
    if finyear is None:
        finyear = 'NA'
    #Upload report to s3 and get the url
    report_url = upload_s3(logger, filename, data)
    if isinstance(data, pd.DataFrame):
        excel_url = report_url.rstrip('csv')+"xlsx"
    logger.debug(f"Location ID is {location_id}")
    #First check if report Exists
    data = {
        'report_type' : report_type,
        'location' : location_id,
        'finyear' : finyear,
        }
    report_id = get_obj_id(logger, REPORTURL, params=data)
    logger.debug(f"report id is {report_id}")
    if report_id is None:
        #We need to create the report
        post_data = {
            'report_type' : report_type,
            'location' : location_id,
            'finyear' : finyear,
            'report_url': report_url,
            'excel_url': excel_url,
            'remarks' : remarks,
            'health' : health
            }
        res = requests.post(REPORTURL, headers=headers, data=json.dumps(post_data))
        logger.info(f"Post status {res.status_code} and response {res.content}")
    else:
        patch_data = {
            "id" : report_id,
            "report_url" : report_url,
            'excel_url': excel_url,
            'remarks' : remarks,
            'health' : health
            }
        res = requests.patch(REPORTURL, headers=headers,
                             data=json.dumps(patch_data))
        logger.info(f"Patch status {res.status_code} and response {res.content}")
    return report_url
