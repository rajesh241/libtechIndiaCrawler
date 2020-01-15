"""This module has functions related to AWS"""
#pylint: disable=E1101
#pylint: disable-msg=bare-except

import os
import datetime
from io import StringIO, BytesIO
import boto3
import pandas as pd

AWS_PROFILE_NAME = "libtechIndia"
AWS_DATA_BUCKET = "libtech-india-data"
AWS_REGION = "ap-south-1"

def aws_init(use_env=None):
    """Initializes the AWS bucket. It can either pick up AWS credentials from
    the environment variables or it can pick up from the profile in the .aws
    directory location in HOME Directory"""
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY')
    aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
    region = os.environ.get('AWS_REGION')
    use_env = os.environ.get('USE_ENVIRONMENT_VARIABLE')
    if use_env is None:
        boto3.setup_default_session(profile_name=AWS_PROFILE_NAME)
    elif use_env == "1":
        boto3.setup_default_session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )

    s3_instance = boto3.resource('s3', region_name=AWS_REGION)
    return s3_instance

def days_since_modified_s3(logger, filename):
    """Given the filepath in AWS Bucket, this function gives the time since the
    file as last modified"""
    logger.debug(f"Getting last modifed data for {filename}")
    s3_instance = aws_init()
    obj = s3_instance.Object(AWS_DATA_BUCKET, filename)
    try:
        obj.load()
        modified_date = obj.last_modified
        time_diff = datetime.datetime.now(datetime.timezone.utc) - modified_date
        days_diff = time_diff.days
    except:
        days_diff = None
    return days_diff

def put_object_s3(bucket, filename, filedata, content_type):
    """Putting object in amazon S3"""
    bucket.put_object(
        Body=filedata,
        Key=filename,
        ContentType=content_type,
        ACL='public-read'
        )


def upload_s3(logger, filename, data, bucket_name=None):
    """
    This function will upload to amazon S3, it can take data either as
    string or as data frame
       filename: filename along with file path where file needs tobe created
       for example abcd/efg/hij.csv
       data : content can be a string or pandas data frame
       bucket: Optional bucket name, in which file needs to be created else
       will default to the AWS_DATA_BUCKET
    """
    logger.debug(f"Uploading file {filename} to Amazon S3")
    s3_instance = aws_init()
    if bucket_name is None:
        bucket_name = AWS_DATA_BUCKET
    bucket = s3_instance.Bucket(bucket_name)
    if isinstance(data, pd.DataFrame):
        #If the data passed is a pandas dataframe
        data['lastUpdateDate'] = datetime.datetime.now().date()
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, encoding='utf-8-sig')
        filedata = csv_buffer.getvalue()
        content_type = 'text/csv'
        put_object_s3(bucket, filename, filedata, content_type)
        excelfilename = filename.rstrip('csv')+"xlsx"
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter', options={'strings_to_urls': False})
        data.to_excel(writer)
        writer.save()
        filedata = output.getvalue()
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        put_object_s3(bucket, excelfilename, filedata, content_type)
    else:
        content_type = 'text/html'
        filedata = data
        put_object_s3(bucket, filename, filedata, content_type)

    report_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{filename}"

    return report_url

def get_aws_file_url(filename, bucket_name=None):
    """Get the AWS File URL given a filepath"""
    if bucket_name is None:
        bucket_name = AWS_DATA_BUCKET
    report_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{filename}"
    return report_url
