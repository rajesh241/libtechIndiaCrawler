import os
import sys
import csv
import requests
from bs4 import BeautifulSoup
import urllib.parse as urlparse
import pandas as pd
#from defines import djangoSettings
from commons import loggerFetch,getAuthenticationToken,getTask,updateTask,getLocationDict,uploadS3
import tasks
import mysql.connector
from mysql.connector import Error
from slugify import slugify
from pathlib import Path
import json
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
  parser.add_argument('-sf', '--startFinYear', help='Test Input 1', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
  args = vars(parser.parse_args())
  return args
def connect():
    """ Connect to MySQL database """
    conn = None
    homeDir = str(Path.home())
    jsonConfigFile=f"{homeDir}/.libtech/callmgrConfig.json"
    with open(jsonConfigFile) as config_file:
      config=json.load(config_file)
    try:
        conn = mysql.connector.connect(host=config['host'],
                                       database=config['database'],
                                       user=config['user'],
                                       password=config['password'])
        if conn.is_connected():
            print('Connected to MySQL database')
 
    except Error as e:
        print(e)
    return conn 
   #finally:
   #    if conn is not None and conn.is_connected():
   #        conn.close()
 
def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    logger.info("Testing")
    header1=["groupName","addressbook"]
    csvArray1=[]
    conn=connect()
    cursor = conn.cursor()
    headers=["phone","district","block","panchayat","groups","cicle","operatorName","dnd"]
    cursor.execute("SELECT name FROM groups")
    rows=cursor.fetchall()
    for row in rows:
      csvArray=[]
      name=row[0]
      nameSlug=slugify(name)    
      logger.info(f"{name}-{nameSlug}")
      query=f"select phone,district,block,panchayat,groups,circle,operatorName,dnd from addressbook where groups like '%~{name}~%'"
      cursor.execute(query)
      rows1=cursor.fetchall()
      for row1 in rows1:
        csvArray.append(row1)
      df = pd.DataFrame(csvArray,columns=headers)
      filename=f"callmgr/addressbook/{nameSlug}.csv"
      reportURL,excelURL=uploadS3(logger,filename,df=df)
      a=[name,excelURL]
      logger.info(excelURL)
      csvArray1.append(a)
    df = pd.DataFrame(csvArray1,columns=header1)
    df.to_csv("/tmp/addressbook.csv")
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
