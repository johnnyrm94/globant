import psycopg2
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
import boto3

# Connect to database   
conn_info = {
    "host": "globantdb.c1fhwpqnf58i.us-east-1.rds.amazonaws.com",
    "database": "globantdb",
    "user": "postgres",
    "password": "Nasus1994.",
    "port": "5432"
}
conn = psycopg2.connect(**conn_info)
cur = conn.cursor()

#S3
#Bucket
s3 = boto3.client('s3')
bucket_name = 'globantdb-backup'
departments= 'backup/departments.avro'
jobs = 'backup/jobs.avro'
hired = 'backup/hired_employees.avro'
prefix = 'backup/'

# Schemas
obj = s3.get_object(Bucket=bucket_name, Key=departments)
departments_avro = avro.schema.parse(obj['Body'].read())

obj = s3.get_object(Bucket=bucket_name, Key=jobs)
jobs_schema = avro.schema.parse(obj['Body'].read())

obj = s3.get_object(Bucket=bucket_name, Key=hired)
hired_employees_schema = avro.schema.parse(obj['Body'].read())

# Check table
cur.execute("SELECT COUNT(*) FROM departments")
if cur.fetchone()[0] != 0:
    raise Exception("Departments table is not empty")
else:
    # Restore table
    departments_data = []
    obj = s3.get_object(Bucket=bucket_name, Key=departments)
    with DataFileReader(obj['Body'], DatumReader()) as reader:
        for row in reader:
            departments_data.append(row)

    # Insert data from Back up
    for row in departments_data:
        cur.execute("INSERT INTO departments (id, department) VALUES (%s, %s)", (row["id"], row["department"]))

conn.commit()
cur.close()
conn.close()
