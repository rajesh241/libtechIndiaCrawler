import os
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
from commons import loggerFetch,getAuthenticationToken,getTask,updateTask,getLocationDict,uploadS3,getAuthenticationHeader,getReportDF,getChildLocations
from defines import LOCATIONURL,GETREPORTURL
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
  parser.add_argument('-lc', '--locationCode', help='Test Input 1', required=False)
  parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
  parser.add_argument('-fn', '--filename', help='File name for locations', required=False)
  parser.add_argument('-bc', '--blockCode', help='File name for locations', required=False)
  parser.add_argument('-f', '--finyear', help='Test Input 1', required=False)
  parser.add_argument('-sf', '--startFinYear', help='Test Input 1', required=False)
  parser.add_argument('-sch', '--scheme', help='Test Input 1', required=False)
  parser.add_argument('-pr', '--priority', help='Test Input 1', required=False)
  parser.add_argument('-rt', '--reportType', help='Test Input 1', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
  args = vars(parser.parse_args())
  return args

def getCode(row):
        jobcard=row['jobcard']
        musterNo=row['musterNo']
        name=row['name']
        code=f"{jobcard}_{musterNo}_{name}"
        return code
def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    blockCode=args['blockCode']
    reportType="blockRejectedTransactions"
    fArray=["18","19","20"]
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
    rejectedDF=rejDF.drop_duplicates()
    rejectedDF['panchayatCode1'] = ''
    rejectedDF['panchayatName1']=''
    rejectedDF['village1']=''
    rejectedDF['headOfHousehold']=''
    rejectedDF['caste']=''
    logger.info(rejectedDF.columns)
    reportType="jobcardRegister"
    pCodes=getChildLocations(logger,blockCode,scheme='nrega')
    jcDF=None
    for panchayatCode in pCodes:
      reportDF=getReportDF(logger,locationCode=panchayatCode,reportType=reportType,index_col=0)
      if jcDF is None:
        jcDF=reportDF
      else:
        jcDF=pd.concat([jcDF,reportDF])
      logger.info(f"reportDF shape {reportDF.shape} consolidated Shape {jcDF.shape}")

    j=len(rejectedDF)
    for i,row in rejectedDF.iterrows():
      logger.info(j)
      j=j-1
      jobcard=row['jobcard']
      matchedDF=jcDF[jcDF['jobcard']==jobcard]
      if len(matchedDF) == 1:
        rejectedDF.loc[i, 'panchayatCode1'] = matchedDF.iloc[0].panchayatCode
        rejectedDF.loc[i, 'village1'] = matchedDF.iloc[0].village
        rejectedDF.loc[i, 'panchayatName1'] = matchedDF.iloc[0].panchayat
        rejectedDF.loc[i, 'headOfHousehold'] = matchedDF.iloc[0].headOfHousehold
        rejectedDF.loc[i, 'caste'] = matchedDF.iloc[0].caste
    rejectedDF.to_csv(f"/tmp/blockRejectedTransactions_{blockCode}.csv")
    rejectedDF.to_excel(f"/tmp/blockRejectedTransactions_{blockCode}.xlsx")
  logger.info("...END PROCESSING") 

if __name__ == '__main__':
  main()
