import os
import re
import sys
import csv
import requests
from bs4 import BeautifulSoup
import urllib.parse as urlparse
import pandas as pd
#from defines import djangoSettings
from slugify import slugify
from pathlib import Path
import json
#Settingup Project DIr
baseDir =os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))+"/crawler"
sys.path.append(baseDir)
#Custom Imports
from commons import loggerFetch,getAuthenticationToken,getTask,updateTask,getLocationDict,uploadS3,getAuthenticationHeader,getReportDF,getChildLocations,fetchHTML,getDateObj,getFinYear
from defines import LOCATIONURL,GETREPORTURL

dateRegex=re.compile(r'RJ\d{7}_\d{6}')

def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='This is the main script which fetches and executes CrawlRequests')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-t', '--test', help='Test Loop', required=False,action='store_const', const=1)
  parser.add_argument('-ic','--initiateCrawl', help='Test Loop', required=False,action='store_const', const=1)
  parser.add_argument('-d', '--debug', help='Execute the code', required=False,action='store_const', const=1)
  parser.add_argument('-aj', '--affectedJobcards', help='Execute the code', required=False,action='store_const', const=1)
  parser.add_argument('-lc', '--locationCode', help='Test Input 1', required=False)
  parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
  parser.add_argument('-fn', '--filename', help='File name for locations', required=False)
  parser.add_argument('-bc', '--blockCode', help='File name for locations', required=False)
  parser.add_argument('-dc', '--districtCode', help='File name for locations', required=False)
  parser.add_argument('-f', '--finyear', help='Test Input 1', required=False)
  parser.add_argument('-sf', '--startFinYear', help='Test Input 1', required=False)
  parser.add_argument('-sch', '--scheme', help='Test Input 1', required=False)
  parser.add_argument('-pr', '--priority', help='Test Input 1', required=False)
  parser.add_argument('-rt', '--reportType', help='Test Input 1', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
  args = vars(parser.parse_args())
  return args
def getDistrictCodeArray(logger,stateCode):
  locationType='district'
  codeArray=[]
  url=f"{LOCATIONURL}?scheme=nrega&locationType={locationType}&stateCode={stateCode}&limit=10000"
  logger.info(url)
  results=fetchHTML(logger,url,format="json")
  count=results.get("count",0)
  if count > 0:
    items=results['results']
    for item in items:
      code=item['code']
      codeArray.append(code)
  return codeArray
def getBlockCodeArray(logger,districtCode):
  locationType='block'
  codeArray=[]
  url=f"{LOCATIONURL}?scheme=nrega&locationType={locationType}&districtCode={districtCode}&limit=10000"
  logger.info(url)
  results=fetchHTML(logger,url,format="json")
  count=results.get("count",0)
  if count > 0:
    items=results['results']
    for item in items:
      code=item['code']
      codeArray.append(code)
  return codeArray

def getFTOFinYear(logger,ftoNo):
  s=None
  finyear=None
  dateFormat="%d%m%y"
  mo=dateRegex.search(ftoNo)
  if mo is not None:
    s=mo.group()
    s=s[10:]
  if s is not None:
    dateObj=getDateObj(s,dateFormat)
    finyear=getFinYear(dateObj)
  return finyear

def processRejDF(logger,df):
  validFinYears=["18","19","20"]
  logger.info(f"INitia shape {df.shape}")
  df=df[df['status']=="Rejected"]
  logger.info(f"Shape after rejected Filering {df.shape}")
  df['finyear']=0
  for i,row in df.iterrows():
    ftoNo=row['ftoNo']
    finyear=getFTOFinYear(logger,ftoNo) 
    if finyear is not None:
      df.loc[i,'finyear']=finyear
  df=df[df['finyear'].isin(validFinYears)]
  logger.info(f"Shape after finyear Filering {df.shape}")
  return df
def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))

  if args['affectedJobcards']:
    csvArray=[]
    colHeaders=["districtCode","blockCode","finyear","uniqueJobcards"]
    reportType="blockRejectedTransactions"
    fArray=["18","19","20"]
    fArray=["20"]
    aggDFs=[]
    districtCode=args['districtCode']
    districtCodes=getDistrictCodeArray(logger,'27')
    for districtCode in districtCodes:
      blockCodes=getBlockCodeArray(logger,districtCode)
      distDFs=[]
      for blockCode in blockCodes:
        rejDFs=[]
        for finyear in fArray:
          logger.info(f"Processing {blockCode} - finyear {finyear}")
          df=getReportDF(logger,locationCode=blockCode,reportType=reportType,finyear=finyear,index_col=0) 
          if df is not None:
            logger.info(f"DF shape {df.shape} for finyear {finyear}")
            rejDFs.append(df)
          else:
            missingData+=f"{blockCode},{finyear}"
        rejDF=pd.concat(rejDFs,ignore_index=True)
        logger.info(f"Shape before drop duplicates {rejDF.shape}")
        rejDF=rejDF.drop_duplicates()
        logger.info(f"Shape after drop duplicates {rejDF.shape}")
        processedDF=processRejDF(logger,rejDF)
        processedDF.to_csv("/tmp/p.csv")
        for finyear in fArray:
          df=processedDF[processedDF['finyear']==finyear]
          uniqueJobcards=df['jobcard'].nunique()
          a=[districtCode,blockCode,finyear,uniqueJobcards]
          csvArray.append(a)
          logger.info(f"blockCode {blockCode} unique Jobcards {uniqueJobcards}")
    df=pd.DataFrame(csvArray,columns=colHeaders)
    df.to_csv("uniqueJobcards.csv")
    df.to_csv("/tmp/u.csv")
  if args['test']:
    missingData=''
    reportType="blockRejectedTransactions"
    fArray=["18","19","20"]
    aggDFs=[]
    districtCode=args['districtCode']
    districtCodes=getDistrictCodeArray(logger,'27')
    for districtCode in districtCodes:
      blockCodes=getBlockCodeArray(logger,districtCode)
      distDFs=[]
      for blockCode in blockCodes:
        rejDFs=[]
        for finyear in fArray:
          logger.info(f"Processing {blockCode} - finyear {finyear}")
          df=getReportDF(logger,locationCode=blockCode,reportType=reportType,finyear=finyear,index_col=0) 
          if df is not None:
            logger.info(f"DF shape {df.shape} for finyear {finyear}")
            rejDFs.append(df)
          else:
            missingData+=f"{blockCode},{finyear}"
        rejDF=pd.concat(rejDFs,ignore_index=True)
        logger.info(f"Shape before drop duplicates {rejDF.shape}")
        rejDF=rejDF.drop_duplicates()
        logger.info(f"Shape after drop duplicates {rejDF.shape}")
        distDF=processRejDF(logger,rejDF)
        distDFs.append(distDF)
      distDF=pd.concat(distDFs,ignore_index=True)
      index_col=["district","rejectionReason","finyear"]
      aggDF = distDF.groupby(index_col).size().reset_index(name='counts')
      aggDFs.append(aggDF) 
    aggDF=pd.concat(aggDFs,ignore_index=True)
    aggDF.to_csv("/tmp/agg.csv")
    with open("/tmp/missingData.csv","w") as f:
      f.write(missingData)
    exit(0)
    reportType="blockRejectedTransactions"
    startFinYear='18'
    endFinYear='20'
    rejDFs=[]
    for finyear in range(int(startFinYear),int(endFinYear)+1):
      finyear=str(finyear)
      reportType="blockRejectedTransactions"
      df=getReportDF(logger,locationCode=blockCode,reportType=reportType,finyear=finyear,index_col=0) 
      if df is not None:
        logger.info(f"DF shape {df.shape} for finyear {finyear}")
        rejDFs.append(df)
    rejDF=pd.concat(rejDFs,ignore_index=True)
    logger.info(f"Saape of combined DF is {rejDF.shape}")
 
if __name__ == '__main__':
  main()
