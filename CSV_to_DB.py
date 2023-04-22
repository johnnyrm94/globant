import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer
from sqlalchemy.types import String

DeparDF = pd.read_csv('departments.csv', header=None, names=['id', 'department'])
HiredDF2 = pd.read_csv('hired_employees.csv', header=None, names=['id', 'name', 'datetime', 'department_id', 'job_id'])
JobsDF3 = pd.read_csv('jobs.csv', header=None, names=['id', 'job'])

#print(JobsDF3.to_string()) 

#print(HiredDF2.to_string()) 

#print(DeparDF.to_string()) 

engine = create_engine('postgresql://postgres:Nasus1994.@globantdb.c1fhwpqnf58i.us-east-1.rds.amazonaws.com:5432/globantdb')

dt = {'id': Integer(), 'department': String(50)}
DeparDF.to_sql('departments', con=engine, if_exists='append', index=False, dtype=dt) #columns: id and department

dt = {'id': Integer(), 'name': String(50),'datetime': String(50), 'department_id': Integer(), 'job_id': Integer()} 
HiredDF2.to_sql('hired_employees', con=engine, if_exists='append', index=False, dtype=dt) #columns: id, name, datetime, department_id, job_id

dt = {'id': Integer(), 'job': String(50)}
JobsDF3.to_sql('jobs', con=engine, if_exists='append', index=False, dtype=dt) #columns: id, job 



