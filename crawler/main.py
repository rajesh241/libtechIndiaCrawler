import os
import sys
import csv
import requests
from bs4 import BeautifulSoup
import urllib.parse as urlparse
import pandas as pd
#from defines import djangoSettings
from commons import loggerFetch,getAuthenticationToken,getTask,updateTask,getLocationDict,NREGANICServerStatus,getCurrentDateTime
import tasks
import time
import datetime
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
  parser.add_argument('-pn', '--processName', help='ProcessName', required=False)
  parser.add_argument('-tid', '--taskID', help='Optional Task ID', required=False)
  parser.add_argument('-fn', '--funcName', help='Test Input 1', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
  args = vars(parser.parse_args())
  return args

def executeTask(logger,taskID=None,processName=None):
  if processName is None:
    processName='default'
  taskDict=getTask(logger,taskID=taskID)
  startTimeObj,startTime=getCurrentDateTime()
  if taskDict is None:
    logger.info("Queue is empty")
    return "No Tasks to be completed"
  logger.info(taskDict)
  reportType=taskDict.get("reportType",None)
  locationCode=taskDict.get("locationCode",None)
  startFinYear=taskDict.get("startFinYear",None)
  endFinYear=taskDict.get("endFinYear",None)
  taskID=taskDict.get("id",None)
  isServerRunning=NREGANICServerStatus(logger,locationCode)
  logger.debug(taskDict)
  logger.debug(isServerRunning)
  if (isServerRunning):
    updateTask(logger,taskID,inProgress=True,processName=processName,startTime=startTime)
  else:
    updateTask(logger,taskID,parked=True,processName=processName)
    return "Task is parked"
  try:
    reportURL = getattr(tasks, reportType)(logger,locationCode,startFinYear=startFinYear,endFinYear=endFinYear)
    remarks=''
  except Exception as e:
    remarks=str(e)
    logger.info(f"Remarks are {remarks}")
    reportURL=None
  endTimeObj,endTime=getCurrentDateTime()
  duration=int(((endTimeObj-startTimeObj).total_seconds())/60)
  logger.info(f"Duration is {duration}")
  updateTask(logger,taskID,reportURL,processName=processName,endTime=endTime,duration=duration,remarks=remarks)
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
    taskID=args['taskID']
    processName=args['processName']
    message=executeTask(logger,taskID=taskID,processName=processName)
    logger.debug(f"Task Executed with message {message}")
     
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
