#!/usr/bin/env python
# coding: utf-8

# Check for spark dependency (ONLY for LOCAL SPARK INSTALLATION)

import findspark
findspark.init()

#Import PySpark dependencies
from pyspark.sql.functions import split, col, regexp_replace, trim

import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import paramiko
import yaml
import os
import shutil
import gzip
import pandas as pd
import csv

# Widen the display of Pandas DataFrame output
pd.set_option('display.max_colwidth', None)

# #### Read SFTP credentials from YAML file

sftp_cred = yaml.load(open('sftp_credentials.yml'), yaml.FullLoader)
hostname = sftp_cred['hostname']
username = sftp_cred['username']


# #### Establish Connection to SFTP Server

k = paramiko.RSAKey.from_private_key_file("./sftp_ssh.pem")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

print("Connecting to host...")
ssh.connect(hostname = hostname, username = username, pkey = k)

print("Authenticated.\nStarting the session...")
sftp = ssh.open_sftp()
print("Connected.")

# Get list of files in the sftp directory
files = sftp.listdir()
print(str(len(files)) + " Files to be processed:")
print(files)

# Truncate and create a folders to store raw and curated data 
if os.path.exists('./tmp/'):
    shutil.rmtree('./tmp/')
    os.mkdir('./tmp/')
    os.mkdir('./tmp/raw/')
    os.mkdir('./tmp/curated/')

def transform_data(filename):
    '''Takes in filename to be processed
  
    Transforms the data as follows:
    - Updates column datatypes
    - Column enrichments
    
    Loads data to '/tmp/curated/' local folder 
  
    Parameters:
    filename (string): Name of the file
  
    '''
    df = spark.read.option("header", "true")                .option("quote", "\"")                .option("multiline","true")                .csv("./tmp/raw/" + filename, header=True)
    
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    df = df.withColumn("STORY_DATE_TIME",df['STORY_DATE_TIME']                       .cast('timestamp'))

    df = df.withColumn("TAKE_DATE_TIME",df['TAKE_DATE_TIME']                       .cast('timestamp'))
    
    df = df.withColumn("ACCUMULATED_STORY_TEXT", regexp_replace(col("ACCUMULATED_STORY_TEXT"), "[\n\r]", " "))
    df = df.withColumn("TAKE_TEXT", regexp_replace(col("TAKE_TEXT"), "[\n\r]", " "))
    
    df = df.withColumn('ACCUMULATED_STORY_TEXT_KEYWORDS', trim(split(df['ACCUMULATED_STORY_TEXT'], 'Keywords:')                   .getItem(1)))
    df = df.withColumn('TAKE_TEXT_KEYWORDS', trim(split(df['TAKE_TEXT'], 'Keywords:')                   .getItem(1)))
    
    df = df.select("DATE","TIME","UNIQUE_STORY_INDEX","EVENT_TYPE", "PNAC", "STORY_DATE_TIME", "TAKE_DATE_TIME",                   "HEADLINE_ALERT_TEXT", "ACCUMULATED_STORY_TEXT", "ACCUMULATED_STORY_TEXT_KEYWORDS", "TAKE_TEXT",                   "TAKE_TEXT_KEYWORDS", "PRODUCTS", "RELATED_RICS", "NAMED_ITEMS", "HEADLINE_SUBTYPE", "STORY_TYPE",                   "TABULAR_FLAG", "ATTRIBUTION", "LANGUAGE")
    
    df.toPandas().to_csv("./tmp/curated/" + filename[:-3], quoting=csv.QUOTE_ALL)

#Fetch files from SFTP Server
for f in files:
    print("Downloading " + f + " ...")
    sftp.get(f, "./tmp/raw/" + f)
    print("Raw data downloaded.\nTransforming data...")
    transform_data(f)
    print("Curated data loaded.")


# ### Future Scope
# 
# - Column enrichments like extracing Page Editor, Reporting by, Editing by, etc. (from "ACCUMULATED_STORY_TEXT" or "TAKE_TEXT" attribute) which would simplify querying and searching through the data.
# - Few attributes have records containing articles and tables, these could be stored in document databases.
# - The data contains text from multiple languages - Arabic, Spanish, etc.; these could be translated using APIs and standardized to English.




