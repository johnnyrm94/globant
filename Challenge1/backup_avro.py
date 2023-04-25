import psycopg2
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from datetime import datetime

# Definir la información de la conexión a la base de datos
conn_info = {
    "host": "globantdb.c1fhwpqnf58i.us-east-1.rds.amazonaws.com",
    "database": "globantdb",
    "user": "postgres",
    "password": "Nasus1994.",
    "port": "5432"
}

# Definir el esquema AVRO para cada tabla
departments_schema = avro.schema.parse(open("departments-schema.avsc", "rb").read())

jobs_schema = avro.schema.parse(open("jobs-schema.avsc", "rb").read())

hired_employees_schema = avro.schema.parse(open("hired_employees-schema.avsc", "rb").read())

# Conectar a la base de datos y crear el cursor
conn = psycopg2.connect(**conn_info)
cur = conn.cursor()

# Hacer una copia de seguridad de cada tabla y guardarla en un archivo AVRO
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
        writer.append({"id": row[0], "name": row[1], "datetime": datetime.strftime(row[2], '%Y-%m-%d %H:%M:%S'), "department_id": row[3], "job_id": row[4]})
    writer.close()

# Cerrar el cursor y la conexión
cur.close()
conn.close()

# Leer los archivos AVRO generados
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
