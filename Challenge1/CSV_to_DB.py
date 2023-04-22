import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

DeparDF = pd.read_csv('departments.csv')
HiredDF2 = pd.read_csv('hired_employees.csv')
JobsDF3 = pd.read_csv('jobs.csv')

#print(JobsDF3.to_string()) 

#print(HiredDF2.to_string()) 

#print(DeparDF.to_string()) 

engine = create_engine('postgresql://postgres:Nasus1994.@globantdb.c1fhwpqnf58i.us-east-1.rds.amazonaws.com:5432/globantdb')

dt = {'id': sqlalchemy.types.INTEGER(), 'department': sqlalchemy.types.String(length=20)}
DeparDF.to_sql('departments', con=engine, if_exists='append', index=False, dtype=dt) #columns: id and department


dt = {'id': sqlalchemy.types.INTEGER(), 'name': sqlalchemy.types.String(length=8),'datetime': sqlalchemy.types.String(length=20), 'department_id': sqlalchemy.types.INTEGER(), 'job_id': sqlalchemy.types.INTEGER()} 
HiredDF2.to_sql('hired_employees', con=engine, if_exists='append', index=False, dtype=dt) #columns: id, name, datetime, department_id, job_id

dt = {'id': sqlalchemy.types.INTEGER(), 'job': sqlalchemy.types.String(length=20)}
JobsDF3.to_sql('jobs', con=engine, if_exists='append', index=False, dtype=dt) #columns: id, job 



