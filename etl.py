import requests
import json
import pandas as pd
import os
import ast
import psycopg2
from datetime import datetime
from io import StringIO
import io
import boto3
from dotenv import dotenv_values
dotenv_values()
from util import get_api_credential, get_redshift_connection, execute_sql


config = dotenv_values('.env')
api_id = get_api_credential()[0]
api_key = get_api_credential()[1]
url = 'https://xecdapi.xe.com/v1/convert_from.json/?from=USD&to=NGN,GHS,KES,UGX,MAD,EGP,XAF&amount=1'
# conn = get_redshift_connection()

s3_client = boto3.client('s3') #s3 client object is instantiated here
s3_resource = boto3.resource('s3') #s3 resource object is instantiated here
bucket_name = 'muyiibucket'#the bucket name in asw S3 
data_staging_path = 'raw_data' # the folder to stage the extracted data from the API
archive_path = 'archived_data' # the folder in S3 used to archive copy of raw data from API
transformed_data = 'transformed_data' # the folder to store transformed data before final load to Redshift


def raw_job_data(): #data is extracted and laoded to s3
    '''
    This function sends request and retrieve exchnage rate data for all the specified countries from XE REST API and
    write the data to an external JSON file. 
    Parameter: Takes in two parameters - XE account ID and API key
    Return value: None. Does not return a value
    Return type: None
    '''
    response = requests.get(url, auth=(api_id, api_key) ).json() #data extraction from API
    json_data = json.dumps(response) #extracted data is converted to JSON format
    # Define the bucket name and key (object key) for the JSON file
    file_name = f"raw_data_{datetime.now().strftime('%Y%m%d%H%M')}" #file name is defined per time
    bucket = bucket_name
    path = data_staging_path
    # using the upload operation to write the data into s3
    try:
        s3_client.put_object(Bucket=bucket, Key=f'{path}/{file_name}', Body= json_data)
        
    except Exception as e:
        print(f"Error writting JSON data to S3: {str(e)}")
    print('raw data job is extracted from api and written to S3')
raw_job_data()