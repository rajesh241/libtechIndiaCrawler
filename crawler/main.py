import os
import sys
import csv
import requests
from bs4 import BeautifulSoup
import urllib.parse as urlparse
import pandas as pd
#from defines import djangoSettings
from commons import loggerFetch,getAuthenticationToken,getTask,updateTask,getLocationDict,NREGANICServerStatus
import tasks
def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='This is the main script which fetches and executes CrawlRequests')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-t', '--test', help='Test Loop', required=False,action='store_const', const=1)
  parser.add_argument('-e', '--execute', help='Execute the code', required=False,action='store_const', const=1)
  parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
  parser.add_argument('-lf', '--logFile', help='Test Input 1', required=False)
  parser.add_argument('-fn', '--funcName', help='Test Input 1', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
  args = vars(parser.parse_args())
  return args

def executeTask(logger):
  token=getAuthenticationToken()
  if token is None:
    return "Authentication has been unsuccessful"
  logger.debug(f"Token is {token}")
  response=getTask(logger,token) 
  if response['count'] > 0:
    logger.info(response)
  else:
    logger.info("Queue is empty")
    return "No Tasks to be completed"
  taskDict=response['results'][0]
  reportType=taskDict.get("reportType",None)
  locationCode=taskDict.get("locationCode",None)
  startFinYear=taskDict.get("startFinYear",None)
  endFinYear=taskDict.get("endFinYear",None)
  taskID=taskDict.get("id",None)
  isServerRunning=NREGANICServerStatus(logger,locationCode)
  logger.debug(taskDict)
  logger.debug(isServerRunning)
  if (isServerRunning):
    updateTask(logger,taskID,inProgress=True)
  else:
    updateTask(logger,taskID,parked=True)
    return "Task is parked"
  reportURL = getattr(tasks, reportType)(logger,locationCode,startFinYear=startFinYear,endFinYear=endFinYear)
  updateTask(logger,taskID,reportURL)
  return None
def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    logger.info("Testing phase") 
    lcode=args['testInput1']
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

  if args['execute']:
    logger.info("Executing from Crawl Queue")
    message=executeTask(logger)
    logger.debug(f"Task Executed with message {message}")
     
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
