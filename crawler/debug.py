import os
import sys
import csv
import requests
from bs4 import BeautifulSoup
import urllib.parse as urlparse
import pandas as pd
#from defines import djangoSettings
from commons import loggerFetch,getAuthenticationToken,getTask,updateTask,getLocationDict
import tasks
def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='This is the main script which fetches and executes CrawlRequests')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-t', '--test', help='Test Loop', required=False,action='store_const', const=1)
  parser.add_argument('-d', '--debug', help='Execute the code', required=False,action='store_const', const=1)
  parser.add_argument('-lc', '--locationCode', help='Test Input 1', required=False)
  parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
  parser.add_argument('-fn', '--funcName', help='Test Input 1', required=False)
  parser.add_argument('-f', '--finyear', help='Test Input 1', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
  args = vars(parser.parse_args())
  return args

def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['debug']:
#   funcName="getBlockRejectedTransactions"
#   lcode='0424001'
#   ldict=getLocationDict(logger,locationCode=lcode)
#   logger.info(ldict)
#   funcArgs=[ldict,'16']
#   error=getattr(tasks, funcName)(logger,funcArgs)
#   exit(0)
    funcName="extractMISReportURL"
    funcName=args['funcName']
    finyear=args['finyear']
    locationCode=args['locationCode']
    error=getattr(tasks,funcName)(logger,locationCode)
    exit(0)
    #error=getattr(tasks, funcName)(logger,stateCode='27')
    #error=getattr(tasks, funcName)(logger,limit=1,num_threads=1)
    #error=getattr(tasks, funcName)(logger)
    error=getattr(tasks, funcName)(logger,stateCode='27',startFinYear=finyear,endFinYear=finyear,num_threads=10)
    #error=getattr(tasks, funcName)(logger,locationType='district',locationCodeParam='block_code',limit=100000,num_threads=50)
    #error=getattr(tasks, funcName)(logger,locationType='district',locationCodeParam='block_code',limit=1,num_threads=1)
    exit(0)

    logger.info("Testing phase") 
    lcode=args['locationCode']
    ldict=getLocationDict(logger,lcode)
    logger.debug(f" Location Dict is {ldict}")
    funcName=args['funcName']
    arg2=args['testInput2']
    if funcName=='getJobcardDetails':
      jobcard=args['testInput2']
      jdict={
        'jobcard':jobcard,
        'village':'testVillage',
        'caste':'gujjar',
        }

      funcArgs=[ldict,jdict]
      error,df = getattr(tasks, funcName)(logger,funcArgs)
    else:
      error=getattr(tasks, funcName)(logger,ldict,arg2)
    logger.info(f"Function Executed with Error {error}")
    exit(0)
    funcName='downloadMusters'
    finyear=''
    error=getattr(tasks, funcName)(logger,ldict,finyear)
    exit(0) 

    funcName='getMusterDF'
    musterURL=args['testInput2']
    error,df = getattr(tasks, funcName)(logger,ldict,musterURL)
    logger.info(f"Function Executed with Error {error}")
    exit(0)

     
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
