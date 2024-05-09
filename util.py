import requests
import pandas as pd
import os
import ast
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import dotenv_values
dotenv_values()


'''this utility file is used to complement the etl.py file. The functions pull hidden credentials from
the environment variable file and execute defined SQL queries(.env)'''

# Get credentials from environment variable file
config = dotenv_values('.env')

#the function is used to get the api credentials from the environment variable file
def get_api_credentials():
    account_id = config.get('ACCOUNT_ID')
    api_key = config.get('API_KEY')
    print(account_id)
    print(api_key)
    return account_id, api_key


#get the connection to AWS redshift instance is established here
def get_redshift_connection(): 
    #the IAM variables are instantiated here:
    user = config.get('USER')
    password = config.get('PASSWORD')
    host = config.get('HOST')
    database_name = config.get('DATABASE_NAME')
    port = config.get('PORT')
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    return conn #conn is returned and ready for use


#the function is to execute SQL queries
def execute_sql(sql_query, conn):
    conn = get_redshift_connection()
    cur = conn.cursor() # Creating a cursor object for executing SQL query
    cur.execute(sql_query)
    conn.commit()
    cur.close() # Close cursor
    conn.close() # Close connection