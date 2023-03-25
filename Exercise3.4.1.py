#3 Consulta de rutas: Dado un par origen-destino, se debe obtener el siguiente detalle:

	# 3.1 Duracion promedio del vuelo para esta ruta.
	# 3.2 Nombre de la aerolinea que ofrece dicha ruta, media de retraso en la hora de salida, media de retraso en la hora de llegada.
	# 3.3 Los resultados deben devolverse ordenados de forma que las aerolíneas que vuelan con mas frecuencia la ruta, aparezcan en primer lugar.
	# 3.4 Como extra sería interesante obtener el modelo de aeronave más usado por la aerolínea para dicha ruta.

# Crear una tabla que tenga como key origen-destino
import pandas as pd
import numpy as np 

import happybase
import csv

def getConn():
    return happybase.Connection(host='0.0.0.0')
connection=getConn()
#Si al conectar da el error -> thriftpy2.transport.base.TTransportException: TTransportException(type=1, message="Could not connect to ('0.0.0.0', 9090)")
#ejecutar en una terminal: hbase thrift start -p 9090

#funcion auxiliar para convertir a bytes lo que queremos almacenar en HBase
def to_bytes(value):
  return bytes(str(value), 'utf-8')
# Nombre del namespace que se va a crear +nombre de la tabla
table_name = 'mbd10_30:planes'
# Función auxiliar para convertir una cadena a bytes
def to_bytes(string):
    return string.encode('utf-8')
# Familias de columnas que se van a crear
column_families = {
    'datos': dict()
}
connection.create_table(table_name, column_families)
print(f'Tabla {table_name} creada en el namespace mbd10_30')

arr = [1000*i for i in range(1, 11)]

# Ruta del archivo CSV
csv_file = '/tmp/nosql/airData/plane-data.csv'
count=0
print("Siguiente tabla")
# Abrir el archivo CSV y leerlo con csv.reader
with open(csv_file, 'r') as csvfile:
    csvreader = csv.reader(csvfile)

    # Ignorar la primera línea (cabeceras)
    next(csvreader)

    # Para cada línea en el archivo CSV, insertar un registro en la tabla HBase
    with connection.table(table_name).batch(batch_size=1000) as table:
        
        for row in csvreader:
            # La primera columna "Year" se usará como la clave de fila en HBase
            row_key = to_bytes(row[0])
            if len(row) < 9: # Verificar si hay campos faltantes
                pass
            else:
                data = {
                    'datos:type': to_bytes(row[1]),
                    'datos:manufacturer': to_bytes(row[2]),
                    'datos:issue_date': to_bytes(row[3]),
                    'datos:model': to_bytes(row[4]),
                    'datos:status': to_bytes(row[5]),
                    'datos:aircraft_type': to_bytes(row[6]),
                    'datos:engine_type': to_bytes(row[7]),
                    'datos:year': to_bytes(row[8]),
                }
            table.put(row_key, data)
            count=count+1
            if count in arr:
                print(count)
connection.close()
