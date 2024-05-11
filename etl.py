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
from util import get_api_credentials, get_redshift_connection, execute_sql


config = dotenv_values('.env')
api_id = get_api_credentials()[0]
api_key = get_api_credentials()[1]
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

def transform_data (bucket_name, data_staging_path):
    '''
    This function reads foreing exchange (fx) rate data in a JSON file from a folder in S3 bucket,
    perform some transformations on the data to put in a structured and desired format and 
    write the final output into another folder in the same s3 bucket.
    Parameters: Takes no parameter
    Return value: Does not return a value
    Return type: None  
    '''
    # Define the bucket name and key (object key) to read the JSON file
    bucket = bucket_name
    raw_path =  data_staging_path# Specify foler/path
    objects_list = s3_client.list_objects(Bucket = bucket, Prefix = raw_path) # List the objects in the bucket
    file = objects_list.get('Contents')[1]
    object_key = file.get('Key') # Get file path or key
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        json_content = [json.loads(content)]
  
    except Exception as e:
        print(f"Error reading JSON file from S3: {e}")
    
    rates_data = pd.DataFrame(json_content)[['timestamp', 'from', 'to', 'amount']]
    # Rename columns
    rates_data.rename(columns= {'amount': 'usd_to_currency_rate', 'from': 'currency_from'}, inplace= True)
    # Transform the desired column data
    rates_data['currency_to'] = rates_data['to'].apply(lambda x: [item['quotecurrency'] for item in x])
    rates_data['currency_to_usd_rate'] = rates_data['to'].apply(lambda x: [item['mid'] for item in x])
    rates_data['timestamp'] = rates_data['timestamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
    # Retrieve all currencies
    currencies = [item.get('quotecurrency') for item in json_content[0].get('to')]
    country_xchange_rates_df = []
    for i in range(len(currencies)):
        country_xchange_rates = rates_data[rates_data.columns] # Create a copy of rates_data dataframe
        country_xchange_rates['currency_to'] = country_xchange_rates['currency_to'].apply(lambda x: x[i]) #Extract country currency name into a column 
        country_xchange_rates['currency_to_usd_rate'] = country_xchange_rates['currency_to_usd_rate'].apply(lambda x: x[i]) #Extract country exchange rate value into a column 
        country_xchange_rates = country_xchange_rates[['timestamp', 'currency_from', 'usd_to_currency_rate', 'currency_to_usd_rate', 'currency_to']]
        country_xchange_rates_df.append(country_xchange_rates) # Aggregate currency data into a dataframe
    #Combine all the country exchange rates data
    country_xchange_rates = pd.concat(country_xchange_rates_df)
        #Write transformed data to an external csv file
    csv_buffer = StringIO() # Create a string buffer to collect csv string
    country_xchange_rates.to_csv(csv_buffer, index=False) # Convert dataframe to CSV file and add to buffer
    csv_str = csv_buffer.getvalue() # Get the csv string
    transformed_path = transformed_data # the Specif path is defined
    file_name = f"transformed_data_{datetime.now().strftime('%Y%m%d%H%M')}.csv" # Create a file name
    # using the put_object(write) operation to write the data into s3
    try:
        s3_client.put_object(Bucket=bucket, Key=f'{transformed_path}/{file_name}', Body=csv_str ) 
        print('File successfully written to transformed folder')
    except Exception as e:
        print(f"Error writting CSV file to S3: {e}")
        

#data from transformed layer is laoded to REDSHIFT
def load_to_redshift(transformed_data, bucket_name):
    '''
    This function reads transformed fx rate data from a folder in S3 bucket then loads it
    into redshift.
    Parameters: Takes no parameter
    Return value: Does not return a value
    Return type: None  
    '''
    table_name = 'job_data'
    bucket = bucket_name
    path = transformed_data
    #the object list in the transformed folder is called here
    objects_list = s3_client.list_objects(Bucket = bucket, Prefix = path) # List the objects in the bucket
    file = objects_list.get('Contents')[1]
    object_key = file.get('Key') # Get file path or key
    s3_path = f's3://{bucket}/{object_key}' # Replace this with your file path (bucket name, folder & file name)
    iam_role = config.get('IAM_ROLE')
    conn = get_redshift_connection()
    #A copy query to copy csv files from S3 bucket to Redshift.
    copy_query = f"""
    copy {table_name}
    from '{s3_path}'
    IAM_ROLE '{iam_role}'
    csv
    IGNOREHEADER 1;
    """
    execute_sql(copy_query, conn)
    print('Data successfully loaded to Redshift')
load_to_redshift(transformed_data, bucket_name)