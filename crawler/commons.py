import os
import re
import sys
import requests
import json
import logging
import datetime
import pytz
from bs4 import BeautifulSoup
from logging.handlers import RotatingFileHandler
from slugify import slugify
import boto3
from io import StringIO,BytesIO
import time
import pandas as pd

from defines import apiusername,apipassword,AUTHENDPOINT,baseURL,REPORTURL,AWS_PROFILE_NAME,AWS_DATA_BUCKET_BASEURL,AWS_DATA_BUCKET,TASKQUEUEURL,defaultReDownloadThreshold,reportReDownloadThresholds,LOCATIONURL,reportReDownloadThresholdsCurYear,LOCATIONDATASTATUSURL


def loggerFetch(level=None,filename=None):
  defaultLogLevel="debug"
  logFormat = '%(asctime)s:[%(name)s|%(module)s|%(funcName)s|%(lineno)s|%(levelname)s]: %(message)s' #  %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s"
  if filename is not None:
    logger = logging.getLogger(filename)
  else:
    logger = logging.getLogger(__name__)

  if not level:
    level = defaultLogLevel
  
  if level:
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
      raise ValueError('Invalid log level: %s' % level)
    else:
      logger.setLevel(numeric_level)
  ch = logging.StreamHandler()
  formatter = logging.Formatter(logFormat)
  ch.setFormatter(formatter)
  logger.addHandler(ch)

  if filename is not None:
    filename1="%s/%s/%s" % (crawlerLogDir,"info",filename)
    fh = RotatingFileHandler(filename1, maxBytes=5000000, encoding="utf-8",backupCount=10)
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
  if filename is not None:
    filename1="%s/%s/%s" % (crawlerLogDir,"debug",filename)
    fhd = RotatingFileHandler(filename1, maxBytes=5000000, encoding="utf-8",backupCount=10)
    fhd.setFormatter(formatter)
    fhd.setLevel(logging.DEBUG)
    logger.addHandler(fhd)
  return logger

def awsInit(useEV=None):
  aws_access_key_id = os.environ.get('AWS_ACCESS_KEY')
  aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
  region=os.environ.get('AWS_REGION')
  useEV=os.environ.get('USE_ENVIRONMENT_VARIABLE')
  if useEV is None:
    boto3.setup_default_session(profile_name=AWS_PROFILE_NAME)
  elif (useEV == "1"):
    boto3.setup_default_session(
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      region_name=region
  )

  s3 = boto3.resource('s3', region_name='ap-south-1')
  bucket = s3.Bucket(AWS_DATA_BUCKET)
  return bucket

def daysSinceModifiedS3(logger,filename,bucket=None):
  boto3.setup_default_session(profile_name=AWS_PROFILE_NAME)
  s3 = boto3.resource('s3', region_name='ap-south-1')
  obj= s3.Object(AWS_DATA_BUCKET, filename)
  try: 
    obj.load()
    modifiedDate=obj.last_modified
    timeDiff=datetime.datetime.now(datetime.timezone.utc)-modifiedDate
    daysDiff = timeDiff.days
  except:
    daysDiff=None
  return daysDiff

def uploadS3(logger,filename,data=None,df=None,bucket=None,contentType=None):
  excelfilename=filename.rstrip('csv')+"xlsx"
  if bucket is None:
    bucket=awsInit()
  if contentType is None:
    contentType='test/csv'
  if df is not None:
    df['lastUpdateDate'] = datetime.datetime.now().date()
  if df is not None:
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,encoding='utf-8-sig')
    data=csv_buffer.getvalue()
  response = bucket.put_object(
          Body=data, 
          Key=filename,
          ContentType = contentType,
         # CannedACL = S3CannedACL.PublicRead
          ACL='public-read'

          )
  reportURL=AWS_DATA_BUCKET_BASEURL+filename
  if df is not None:
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter',options={'strings_to_urls': False})
    df.to_excel(writer)
    writer.save()
    data = output.getvalue()
    response = bucket.put_object(
          Body=data, 
          Key=excelfilename,
          ContentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
         # CannedACL = S3CannedACL.PublicRead
          ACL='public-read'

          )
  excelURL=AWS_DATA_BUCKET_BASEURL+excelfilename
  reportURL=AWS_DATA_BUCKET_BASEURL+filename
  

  return reportURL,excelURL

def getAWSFileURL(filename):
  return AWS_DATA_BUCKET_BASEURL+filename


def getAuthenticationToken():
  data={
      'username' : apiusername,
      'password' : apipassword
            }
  try:
    r=requests.post(AUTHENDPOINT,data=data)
    token=r.json()['token']
  except:
    token=None
  return token

def getLocationDict(logger,locationCode=None,locationID=None,scheme=None):
  if scheme is None:
    scheme='nrega'
  if locationID is not None: 
    url="%s/api/location/?id=%s" % (baseURL,locationID)
    r=requests.get(url)
    return r.json()
  elif locationCode is not None:
    url="%s/api/location/?code=%s&scheme=%s" % (baseURL,locationCode,scheme)
  else:
    return None
  r=requests.get(url)
  try:
    response=r.json()
    results=response.get("results",None)
  except:
    results=None
  if results is None:
    return None
  if len(results) != 1:
    return None
  ldict=results[0]
  #logger.debug(ldict)
  return ldict

def getTask(logger,taskID=None):
  token=getAuthenticationToken()
  if taskID is not None:
    url="%s/api/queue/?id=%s" % (baseURL,str(taskID))
  else:
    url="%s/api/queue/?isDone=0&ordering=-priority,updated&limit=1" % (baseURL)
  logger.info(url)
  headers={
      'content-type':'application/json',
      "Authorization" : "JWT " + token
    }
  r=requests.get(url,headers=headers)
  response=r.json()
  count=response.get("count",None)
  if taskID is not None:
    returnID=response.get("id",None)
    if returnID is None:
      taskDict=None
    else:
      taskDict=response
  elif ((count is None) or (count == 0)):
    taskDict= None
  else:
    taskDict=response['results'][0]
  return taskDict
  exit(0)
  taskID=response.get("id",None)
  locationCode=response.get("locationCode",None)
  if (taskID is None) or (locationCode is None):
    return None
  ldict=getLocationDict(logger,locationCode=locationCode)
  if ldict is None:
     return None
  response['ldict']=ldict
  return response 


def getCurrentFinYear():
  now = datetime.datetime.now()
  month=now.month
  if now.month > 3:
    year=now.year+1
  else:
    year=now.year
  return year% 100

def getFullFinYear(shortFinYear):
  shortFinYear_1 = int(shortFinYear) -1
  fullFinYear="20%s-20%s" % (str(shortFinYear_1), str(shortFinYear))
  return fullFinYear

def stripTableAttributesPreserveLinks(inhtml,tableID,baseURL):
  tableHTML=''
  classAtt='id = "%s" border=1 class = " table table-striped"' % tableID
  tableHTML+='<table %s>' % classAtt
  rows=inhtml.findAll('tr')
  for eachRow in rows:
    thCols=eachRow.findAll('th')
    if len(thCols) >= 1:
     tableHTML+='<tr>'
     for eachTD in thCols:
       tableHTML+='<th>%s</th>' % eachTD.text
     tableHTML+='</tr>'

    tdCols=eachRow.findAll('td')
    if len(tdCols) >= 1:
      tableHTML+='<tr>'
      for eachTD in tdCols:
        a=eachTD.find("a")
        if a is not None:
          myLink="%s%s" %(baseURL,a['href'])
          tableHTML+='<td><a href="%s">%s</a></td>' % (myLink,eachTD.text)
        else:
          tableHTML+='<td>%s</td>' % eachTD.text
      tableHTML+='</tr>'

  tableHTML+='</table>'
  return tableHTML


def stripTableAttributes(inhtml,tableID):
  tableHTML=''
  classAtt='id = "%s" border=1 class = " table table-striped"' % tableID
  tableHTML+='<table %s>' % classAtt
  rows=inhtml.findAll('tr')
  for eachRow in rows:
    thCols=eachRow.findAll('th')
    if len(thCols) >= 1:
     tableHTML+='<tr>'
     for eachTD in thCols:
       tableHTML+='<th>%s</th>' % eachTD.text
     tableHTML+='</tr>'

    tdCols=eachRow.findAll('td')
    if len(tdCols) >= 1:
      tableHTML+='<tr>'
      for eachTD in tdCols:
        tableHTML+='<td>%s</td>' % eachTD.text
      tableHTML+='</tr>'

  tableHTML+='</table>'
  return tableHTML



def getCenterAlignedHeading(text):
  return '<div align="center"><h2>%s</h2></div>' % text


def validateNICReport(logger,myhtml,tableIdentifier=''):
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  error="table not found"
  myTable=None
  tables=htmlsoup.findAll("table")
  if tableIdentifier == '':
    return error,myTable
  for eachTable in tables:
    if tableIdentifier in str(eachTable):
      myTable=eachTable
      error=None
  return error,myTable


def validateAndSave(logger,ldict,myhtml,tableIdentifier=None,preserveLinks=False,validate=True):
    error=None
    outhtml=None
    if tableIdentifier is None:
      stateShortCode=ldict.get("stateShortCode",None)
      tableIdentifier=f"{stateShortCode}-"
    locationName=ldict.get("displayName",None)
    if validate == True:
      outhtml=''
      outhtml+=getCenterAlignedHeading(locationName)
        
      error,myTable=validateNICReport(logger,myhtml,tableIdentifier=tableIdentifier)
      if error is None:
        if preserveLinks == False:
          outhtml+=stripTableAttributes(myTable,"myTable")
        else:
          outhtml+=stripTableAttributesPreserveLinks(myTable,"myTable")
    else:
      outhtml=myhtml
      error=None
    
    if error is None:
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
    return outhtml 

def correctDateFormat(myDateString,dateFormat=None):
  if myDateString != '':
    try:
      if dateFormat is not None:
        myDate=time.strptime(myDateString,dateFormat)
      elif "/" in myDateString:
        myDate = time.strptime(myDateString, '%d/%m/%Y')
      else:
        myDate = time.strptime(myDateString, '%d-%m-%Y')
      myDate = time.strftime('%Y-%m-%d', myDate)
    except:
      myDate=None
  else:
    myDate=None
  return myDate

def getjcNumber(jobcard):
  jobcardArray=jobcard.split('/')
  if len(jobcardArray) > 1:
    jcNumber=re.sub("[^0-9]", "", jobcardArray[1])
  else:
    jcNumber='0'
  try:
    jcNumber=str(int(jcNumber))
  except:
    jcNumber='0'
  return jcNumber

def getFilePath(logger,ldict,locationType=None):
  scheme=ldict.get("scheme",None)
  filepath="india"
  if (scheme == "pds"):
    filepath="pds/india"
  if locationType is None:
    locationType=ldict.get("locationType",None)
  if locationType == 'state':
    stateName=slugify(ldict.get("stateName",None))
    filepath="%s/%s/" % (filepath,stateName)
  elif locationType == 'district':
    stateName=slugify(ldict.get("stateName",None))
    districtName=slugify(ldict.get("districtName",None))
    filepath="%s/%s/%s/" % (filepath,stateName,districtName)
  elif ( (locationType == 'block') or (locationType == 'pdsBlock')):
    stateName=slugify(ldict.get("stateName",None))
    districtName=slugify(ldict.get("districtName",None))
    blockName=slugify(ldict.get("blockName",None))
    filepath="%s/%s/%s/%s/" % (filepath,stateName,districtName,blockName)
  elif locationType == 'panchayat':
    stateName=slugify(ldict.get("stateName",None))
    districtName=slugify(ldict.get("districtName",None))
    blockName=slugify(ldict.get("blockName",None))
    panchayatName=slugify(ldict.get("panchayatName",None))
    filepath="%s/%s/%s/%s/%s/" % (filepath,stateName,districtName,blockName,panchayatName)
  elif locationType == 'pdsVillage':
    stateName=slugify(ldict.get("stateName",None))
    districtName=slugify(ldict.get("districtName",None))
    blockName=slugify(ldict.get("blockName",None))
    villageName=slugify(ldict.get("name",None))
    filepath="%s/%s/%s/%s/%s/" % (filepath,stateName,districtName,blockName,villageName)
  else:
    filepath=None
  return filepath

def getAuthenticationHeader():
  data={
      'username' : apiusername,
      'password' : apipassword
            }
  r=requests.post(AUTHENDPOINT,data=data)
  token=r.json()['token']
  headers={
      'content-type':'application/json',
      "Authorization" : "JWT " + token
    }
  return headers

def is_json(json_data):
  try:
    real_json=json.loads(json_data)
    valid_json = True
  except ValueError:
    valid_json=False
  return valid_json

def getIDFromParams(logger,url,params):
  objID=None
  r=requests.get(url,params=params)
  if is_json(r.content):
    responseDict=r.json()
    count=responseDict.get("count",None)
    if count == 1:
      objID=responseDict['results'][0]['id']
      return objID
  return objID
def getReportFilePath(logger,ldict,reportType,finyear):
    filepath=getFilePath(logger,ldict)
    name=ldict.get("name",None)
    if finyear == '':
      filename="%sDATA/reports/%s_%s.csv" % (filepath,reportType,slugify(name))
    else:
      filename="%sDATA/reports/%s_%s_%s.csv" % (filepath,reportType,finyear,slugify(name))
    return filename

def getReportDF(logger,locationCode=None,reportType=None,finyear=None,filename=None,index_col=None):
  if filename is not None:
    awsURL=getAWSFileURL(filename)
  else:
    ldict=getLocationDict(logger,locationCode=locationCode)
    if finyear is None:
      finyear=''
    awsURL=getReportFileURL(logger,ldict,reportType,finyear)
  logger.info(f"Aws url is {awsURL}")
  try:
    if index_col is not None:
      df=pd.read_csv(awsURL,index_col=index_col)
    else:
      df=pd.read_csv(awsURL)
  except:
    df=None
  return df

def getReportFileURL(logger,ldict,reportType,finyear):
  filename=getReportFilePath(logger,ldict,reportType,finyear)
  return getAWSFileURL(filename)

def createUpdateDjangoReport(logger,reportType,finyear,reportURL,excelURL=None,lcode=None,ldict=None):
    error=None
    if (ldict is None) and (lcode is None):
      return "Either Location dict or location Code is required"
    if ldict is None:
      ldict=getLocationDict(logger,locationCode=lcode)
    lcode=ldict.get("code",None)
    lid=ldict.get("id",None)
    headers=getAuthenticationHeader()
    #Check if report Exists
    data={
      'reportType' : reportType,
      'location__code' : lcode,
      'finyear' : finyear,
    }
    reportID=getIDFromParams(logger,REPORTURL,data)
    logger.debug(f"report id is {reportID}")
    if reportID is None:
      postData={
      'reportType' : reportType,
      'location' : lid,
      'finyear' : finyear,
      'reportURL': reportURL,
      'excelURL': excelURL,

              }
      r=requests.post(REPORTURL,headers=headers,data=json.dumps(postData))
      logger.debug(f"Post status {r.status_code} and response {r.content}")
    else:
      patchData={
        "id" : reportID,
        "reportURL" : reportURL,
        'excelURL': excelURL,
      }
      r=requests.patch(REPORTURL,headers=headers,data=json.dumps(patchData))
      logger.debug(f"Patch status {r.status_code} and response {r.content}")
    return reportURL
 
def saveLocationStatus(logger,locationCode,finyear,accuracy):
  ldict=getLocationDict(logger,locationCode=locationCode) 
  lcode=ldict.get("code",None)
  lid=ldict.get("id",None)
  headers=getAuthenticationHeader()
  #Check if report Exists
  data={
    'dataType' : 'nrega',
    'location__code' : lcode,
    'finyear' : finyear,
  }
  reportID=getIDFromParams(logger,LOCATIONDATASTATUSURL,data)
  logger.debug(f"report id is {reportID}")
  if reportID is None:
    postData={
    'location' : lid,
    'finyear' : finyear,
    'accuracy': accuracy,

            }
    r=requests.post(LOCATIONDATASTATUSURL,headers=headers,data=json.dumps(postData))
    logger.debug(f"Post status {r.status_code} and response {r.content}")
  else:
    patchData={
      "id" : reportID,
      "accuracy" : accuracy,
    }
    r=requests.patch(LOCATIONDATASTATUSURL,headers=headers,data=json.dumps(patchData))
    logger.debug(f"Patch status {r.status_code} and response {r.content}")
  return None
 
def saveReport(logger,ldict,reportType,finyear,df):
    name=ldict.get("name",None)
    lcode=ldict.get("code",None)
    lid=ldict.get("id",None)
#    filename="%s%s_%s.csv" % (filepath,reportType,slugify(name))
    filename=getReportFilePath(logger,ldict,reportType,finyear)
    reportURL,excelURL=uploadS3(logger,filename,df=df)
    logger.info(f"Report url is {reportURL}")
    createUpdateDjangoReport(logger,reportType,finyear,reportURL,excelURL=excelURL,ldict=ldict)
    return reportURL

def NREGANICServerStatus(logger,locationCode,scheme=None):
  if scheme == "pds":
    return True
  ldict=getLocationDict(logger,locationCode=locationCode)
  stateName=ldict.get("stateName",None)
  stateCode=ldict.get("stateCode",None)
  crawlIP=ldict.get("crawlIP",None)
  url=f"http://{crawlIP}/netnrega/homestciti.aspx?state_code={stateCode}&state_name={stateName}"
  r=requests.get(url)
  logger.info(r.status_code)
  if r.status_code == 200:
    return True
  else:
    return False

def updateTask(logger,taskID,reportURL=None,inProgress=None,parked=None,processName=None,startTime=None,endTime=None,duration=None,remarks=None):
  headers=getAuthenticationHeader()
  now=datetime.datetime.now()
  if inProgress is not None:
    isError=0
    isDone=0
    priority=0
    status='inProgress'
  elif parked is not None:
    isError=0
    isDone=0
    priority=20
    status='parked'
  elif reportURL is not None:
    isError=0
    isDone=1
    priority=0
    status='completed'
  else:
    isError=1
    isDone=0
    priority=20
    status='error'
  patchData={
    'id' : taskID,
    'isError' : isError,
    'isDone': isDone,
    'status' : status,
    'priority':priority,
    'reportURL':reportURL,
    'processName':processName,
    }
  if startTime is not None:
    patchData['startTime']=startTime
  if endTime is not None:
    patchData['endTime']=endTime
  if duration is not None:
    patchData['duration']=duration
  if remarks is not None:
    patchData['remarks']=remarks
  r=requests.patch(TASKQUEUEURL,headers=headers,data=json.dumps(patchData))
  logger.debug(f"Patch status {r.status_code} and response {r.content}")
 
def getShortFinYear(fullFinYear):
  return fullFinYear[-2:]

def getFinYear(dateObj=None):
  if dateObj is None:
    now = datetime.datetime.now()
  else:
    now=dateObj
  month=now.month
  if now.month > 3:
    year=now.year+1
  else:
    year=now.year
  finyear=str(year % 100)
  if len(finyear) == 1:
    finyear="0"+finyear
  return finyear

def getDateObj(myDateString,dateFormat=None):
  try:
    if dateFormat is not None:
      myDate=datetime.datetime.strptime(myDateString,dateFormat).date()
    elif "/" in myDateString:
      myDate = datetime.datetime.strptime(myDateString, '%d/%m/%Y').date()
    else:
      myDate = datetime.datetime.strptime(myDateString, '%d-%m-%Y').date()
  except:
    myDate=None
  return myDate

def htmlWrapperLocal(title = None, head = None, body = None):
  html_text = '''
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- The above 3 meta tags *must* come first in the head; any other head content must come *after* these tags -->
    
    <title>title_text</title>

    <!-- Bootstrap -->

    <!-- Latest compiled and minified CSS -->
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">

    <div align="center">head_text</div>

  </head>
    
  <body>

    body_text
    
    <!-- jQuery (necessary for Bootstrap"s JavaScript plugins) -->
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js"></script>
    <!-- Include all compiled plugins (below), or include individual files as needed -->

    <!-- Latest compiled and minified JavaScript -->
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>

  </body>
</html>
''' 
  html_text = html_text.replace('title_text', title)
  html_text = html_text.replace('head_text', head)
  html_text = html_text.replace('body_text', body)

  return html_text

def getDefaultStartFinYear():
  endFinYear=getCurrentFinYear()
  startFinYear = str ( int(endFinYear) -1 )
  return startFinYear


def getReportURL(logger,reportType,finyear=None,ldict=None,lcode=None):
  reportURL=None
  if finyear is None:
    finyear=''
  if lcode is None:
    if ldict is not None:
      lcode=ldict.get("code",None)
  if lcode is not None:
    url=f"{REPORTURL}?reportType={reportType}&location__code={lcode}&finyear={finyear}"
    logger.info(url)
    r=requests.get(url)
    if r.status_code == 200:
      response=r.json()
      count=response.get("count",0)
      if count == 1:
        reportURL=response['results'][0]['reportURL']
        logger.info(reportURL)
  return reportURL

def isReportUpdated(logger,reportType,locationCode,finyear=None):
  updateStatus=False
  reportURL=None
  if finyear is None:
    finyear=''
  url=f"{REPORTURL}?reportType={reportType}&location__code={locationCode}&finyear={finyear}"
  logger.info(url)
  r=requests.get(url)
  if r.status_code == 200:
    response=r.json()
    count=response.get("count",0)
    if count == 1:
      updatedString=response['results'][0]['updated']
      reportURL=response['results'][0]['reportURL']
      updatedDateString=updatedString.split("T")[0]
      myDateTime=datetime.datetime.strptime(updatedDateString,'%Y-%m-%d').date()
      diffDays=dateDifference(myDateTime)
      if str(finyear) == str(getCurrentFinYear()):
        threshold=reportReDownloadThresholdsCurYear.get(reportType,defaultReDownloadThreshold)
      else:
        threshold=reportReDownloadThresholds.get(reportType,defaultReDownloadThreshold)
      if diffDays < threshold:
        updateStatus=True
    
  return updateStatus,reportURL


def dateDifference(fromDate,toDate=None):
  if toDate is None:
    toDate=datetime.datetime.now().date()
  dateDiff=toDate-fromDate
  return dateDiff.days

def getChildLocations(logger,locationCode,scheme=None):
  lArray=[]
  if scheme is not None:
    url=f"{LOCATIONURL}?parentLocation__code={locationCode}&scheme={scheme}&limit=10000"
  else:
    url=f"{LOCATIONURL}?parentLocation__code={locationCode}&limit=10000"
  logger.info(url)
  r=requests.get(url)
  if r.status_code == 200:
    data=r.json()
    count=data.get('count',None)
    if ( (count is not None) or (count > 0)):
      results=data.get('results',None)
      for res in results:
        lCode=res.get("code",None)
        if lCode is not None:
          lArray.append(lCode)
  return lArray

def computePercentage(input1,input2):
  if ((input1 is None) or (input2 is None)):
    accuracy=0
  elif ((input1 == 0) and (input2 == 0)):
    accuracy=100
  elif ((input1 == 0) or (input2 == 0)):
    accuracy = 0
  elif (input1 >= input2):
    accuracy = int(input2*100/input1)
  elif (input2 > input1):
    accuracy = int(input1*100/input2)
  else:
    accuracy=0
  return accuracy    

def getCurrentDateTime():
  utc_now = pytz.utc.localize(datetime.datetime.utcnow())
  india_now = utc_now.astimezone(pytz.timezone("Asia/Calcutta"))
  india_now_isoformat=india_now.isoformat()
  return india_now,india_now_isoformat
