import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer
from sqlalchemy.types import String
import os
import boto3
import pandas as pd

#Credentials
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIARZMM3I65JS3OPKD5'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
engine = create_engine('postgresql://postgres:Nasus1994.@globantdb.c1fhwpqnf58i.us-east-1.rds.amazonaws.com:5432/globantdb')


s3 = boto3.client('s3')

bucket_name = 'globantdb-backup'
departments= 'source/departments.csv'
jobs = 'source/jobs.csv'
hired = 'source/hired_employees.csv'

# read the CSV file from S3 into a DataFrame
obj = s3.get_object(Bucket=bucket_name, Key=departments)
departments_df = pd.read_csv(obj['Body'],  header=None, names=['id', 'department'])

# read the CSV file from S3 into a DataFrame
obj = s3.get_object(Bucket=bucket_name, Key=jobs)
jobs_df = pd.read_csv(obj['Body'],  header=None, names=['id', 'job'])

# read the CSV file from S3 into a DataFrame
obj = s3.get_object(Bucket=bucket_name, Key=hired)
hired_df = pd.read_csv(obj['Body'], header=None, names=['id', 'name', 'datetime', 'department_id', 'job_id'])


dt = {'id': Integer(), 'department': String(50)}
departments_df.to_sql('departments', con=engine, if_exists='append', index=False, dtype=dt) #columns: id and department

dt = {'id': Integer(), 'name': String(50),'datetime': String(50), 'department_id': Integer(), 'job_id': Integer()} 
hired_df.to_sql('hired_employees', con=engine, if_exists='append', index=False, dtype=dt) #columns: id, name, datetime, department_id, job_id

dt = {'id': Integer(), 'job': String(50)}
jobs_df.to_sql('jobs', con=engine, if_exists='append', index=False, dtype=dt) #columns: id, job 


