import os
import sys
import csv
import requests
from bs4 import BeautifulSoup
import urllib.parse as urlparse
import pandas as pd
#from defines import djangoSettings
from commons import loggerFetch
import boto3
from io import StringIO
AWS_DATA_BUCKET="libtech-india-data"
AWS_PROFILE_NAME="libtechIndia"
AWS_DATA_BUCKET_BASEURL="https://libtech-india-data.s3.ap-south-1.amazonaws.com/"

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

def uploadS3(logger,filename,data=None,df=None,bucket=None,contentType=None):
  if bucket is None:
    bucket=awsInit()
  if contentType is None:
    contentType='test/csv'
  if df is not None:
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    data=csv_buffer.getvalue()
  response = bucket.put_object(
          Body=data, 
          Key=filename,
          ContentType = contentType,
         # CannedACL = S3CannedACL.PublicRead
          ACL='public-read'

          )
  return AWS_DATA_BUCKET_BASEURL+filename

def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='These scripts will initialize the Database for the district and populate relevant details')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-t', '--test', help='Test Loop', required=False,action='store_const', const=1)
  parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
  args = vars(parser.parse_args())
  return args


def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    logger.info("Testing phase")
    filename="test/a1.csv"
    content="<h1>Test html </h1>"
    content="a,b\n1,2\n"
    bucket=awsInit()
    response=uploadS3(logger,filename,data=content,bucket=bucket)
    logger.info(response)
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
