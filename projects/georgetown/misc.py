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
from commons import loggerFetch,getAuthenticationToken,getTask,updateTask,getLocationDict,uploadS3,getAuthenticationHeader
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
  parser.add_argument('-f', '--finyear', help='Test Input 1', required=False)
  parser.add_argument('-sf', '--startFinYear', help='Test Input 1', required=False)
  parser.add_argument('-sch', '--scheme', help='Test Input 1', required=False)
  parser.add_argument('-pr', '--priority', help='Test Input 1', required=False)
  parser.add_argument('-rt', '--reportType', help='Test Input 1', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
  args = vars(parser.parse_args())
  return args
def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['initiateCrawl']:
    scheme=args.get("scheme","nrega")
    reportType=args.get("reportType","panchayatReferenceDocument")
    priority=args.get("priority",100)
    startFinYear=args.get("startFinYear",18)
    filename=args.get("filename",None)
    if filename is not None:
      selectedBlocksDF=pd.read_csv(f'data/{filename}')
      for index,row in selectedBlocksDF.iterrows():
        blockCode=str(int(row['blockCode']))
        if len(blockCode)==6:
          blockCode="0"+blockCode
        print(f"{655-index}-{blockCode}")
        url=f"{GETREPORTURL}?reportType=blockRejectedTransactions&startFinYear={startFinYear}&location__code={blockCode}&scheme={scheme}&priority=100"
        headers=getAuthenticationHeader()
        r=requests.get(url,headers=headers)
        #print(r.json())  
        url=f"{LOCATIONURL}?locationType=panchayat&limit=10000&blockCode={blockCode}"
        r=requests.get(url)
        response=r.json()
        count=response.get("count",0)
        if count > 0:
          #print(f"the number of panchacyats is {count}")
          results=response['results']
          for res in results:
            panchayatCode=res.get("panchayatCode")
            #print(panchayatCode)
            url=f"{GETREPORTURL}?reportType={reportType}&startFinYear={startFinYear}&location__code={panchayatCode}&scheme={scheme}&priority={priority}"
            headers=getAuthenticationHeader()
            r=requests.get(url,headers=headers)
            #print(r.json())
    logger.info("...END PROCESSING") 

if __name__ == '__main__':
  main()
