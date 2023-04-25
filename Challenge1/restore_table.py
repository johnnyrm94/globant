import psycopg2
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

# Definir la información de la conexión a la base de datos
conn_info = {
    "host": "globantdb.c1fhwpqnf58i.us-east-1.rds.amazonaws.com",
    "database": "globantdb",
    "user": "postgres",
    "password": "Nasus1994.",
    "port": "5432"
}
conn = psycopg2.connect(**conn_info)
cur = conn.cursor()

# Comprobar si la tabla "departments" está vacía
cur.execute("SELECT COUNT(*) FROM departments")
if cur.fetchone()[0] != 0:
    raise Exception("Departments table is not empty")
else:

    # Leer el archivo AVRO y almacenar los datos en una lista
    departments_data = []
    with open("departments.avro", "rb") as f:
        reader = DataFileReader(f, DatumReader())
        for row in reader:
            departments_data.append(row)
        reader.close()


    # Insert data from Back up
    for row in departments_data:
        cur.execute("INSERT INTO departments (id, department) VALUES (%s, %s)", (row["id"], row["department"]))

conn.commit()
cur.close()
conn.close()
