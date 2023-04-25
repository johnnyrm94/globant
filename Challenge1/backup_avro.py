import psycopg2
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from datetime import datetime



conn_info = {
    "host": "globantdb.c1fhwpqnf58i.us-east-1.rds.amazonaws.com",
    "database": "globantdb",
    "user": "postgres",
    "password": "Nasus1994.",
    "port": "5432"
}

# Schemas
departments_schema = avro.schema.parse(open("departments-schema.avsc", "rb").read())

jobs_schema = avro.schema.parse(open("jobs-schema.avsc", "rb").read())

hired_employees_schema = avro.schema.parse(open("hired_employees_schema.avsc", "rb").read())

conn = psycopg2.connect(**conn_info)
cur = conn.cursor()

# Main method
with open("departments.avro", "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), departments_schema)
    cur.execute("SELECT * FROM departments")
    for row in cur.fetchall():
        writer.append({"id": row[0], "department": row[1]})
    writer.close()

with open("jobs.avro", "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), jobs_schema)
    cur.execute("SELECT * FROM jobs")
    for row in cur.fetchall():
        writer.append({"id": row[0], "job": row[1]})
    writer.close()

with open("hired_employees.avro", "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), hired_employees_schema)
    cur.execute("SELECT * FROM hired_employees")
    for row in cur.fetchall():
        
        name = row[1] if row[1] is not None else "no name" # Reemplazar los valores nulos con -1
        dt = row[2] if row[2] is not None else "no datetime" # Reemplazar los valores nulos con -1
        department_id = row[3] if row[3] is not None else -1  # Reemplazar los valores nulos con -1
        job_id = row[4] if row[4] is not None else -1  # Reemplazar los valores nulos con -1
        writer.append({"id": row[0], "name": name, "datetime": dt, "department_id": department_id, "job_id": job_id})
    writer.close()


cur.close()
conn.close()

# Read AVRO
with open("departments.avro", "rb") as f:
    reader = DataFileReader(f, DatumReader())
    for row in reader:
        print(row)
    reader.close()

with open("jobs.avro", "rb") as f:
    reader = DataFileReader(f, DatumReader())
    for row in reader:
        print(row)
    reader.close()

with open("hired_employees.avro", "rb") as f:
    reader = DataFileReader(f, DatumReader())
    for row in reader:
        print(row)
    reader.close()
