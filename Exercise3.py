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
    return happybase.Connection(host='localhost')
connection=getConn()
#Si al conectar da el error -> thriftpy2.transport.base.TTransportException: TTransportException(type=1, message="Could not connect to ('0.0.0.0', 9090)")
#ejecutar en una terminal: hbase thrift start -p 9090

#funcion auxiliar para convertir a bytes lo que queremos almacenar en HBase
def to_bytes(value):
  return bytes(str(value), 'utf-8')
# Nombre del namespace que se va a crear +nombre de la tabla
table_name = 'mbd10_30:origen-destino'
# Función auxiliar para convertir una cadena a bytes
def to_bytes(string):
    return string.encode('utf-8')
# Familias de columnas que se van a crear
column_families = {
    'datos': dict()
}
connection.create_table(table_name, column_families)
print(f'Tabla {table_name} creada en el namespace mbd10_30')
# Ruta del archivo CSV
csv_file = '/tmp/nosql/airData/2007.csv'

# Abrir el archivo CSV y leerlo con csv.reader
with open(csv_file, 'r') as csvfile:
    csvreader = csv.reader(csvfile)

    # Ignorar la primera línea (cabeceras)
    next(csvreader)

    # Para cada línea en el archivo CSV, insertar un registro en la tabla HBase
    with connection.table(table_name).batch(batch_size=100) as table:
        for row in csvreader:
            # La primera columna "Year" se usará como la clave de fila en HBase
            row_key = to_bytes(row[16]+'-'+row[17]+'_'+row[0]+row[1].zfill(2)+row[2].zfill(2)+row[4])

            data = {
                #'info:Year': to_bytes(row[0]),
                #'info:Month': to_bytes(row[1]),
                #'info:DayofMonth': to_bytes(row[2]),
                #'info:DayOfWeek': to_bytes(row[3]),
                #'info:DepTime': to_bytes(row[4]),
                #'info:CRSDepTime': to_bytes(row[5]),
                #'info:ArrTime': to_bytes(row[6]),
                #'info:CRSArrTime': to_bytes(row[7]),
                'info:NombreAerolinea': to_bytes(row[8]),#UniqueCarrier
                'info:FlightNum': to_bytes(row[9]),
                'info:TailNum': to_bytes(row[10]),
                'info:Duracion': to_bytes(row[11]),#ActualElapsedTime 
                #'info:CRSElapsedTime': to_bytes(row[12]),
                #'info:AirTime': to_bytes(row[13]),
                'info:LlegRetraso': to_bytes(row[14]),#ArrDelay
                'info:SalRetraso': to_bytes(row[15]),#DepDelay
                'info:Origin': to_bytes(row[16]),
                'info:Dest': to_bytes(row[17]),
                'info:Distance': to_bytes(row[18]),
                #'info:TaxiIn': to_bytes(row[19]),
                #'info:TaxiOut': to_bytes(row[20]),
                'info:Cancelled': to_bytes(row[21])
                #'info:CancellationCode': to_bytes(row[22]),
                #'info:Diverted': to_bytes(row[23]),
                #'info:CarrierDelay': to_bytes(row[24]),
                #'info:WeatherDelay': to_bytes(row[25]),
                #'info:NASDelay': to_bytes(row[26]),
                #'info:SecurityDelay': to_bytes(row[27]),
                #'info:LateAircraftDelay': to_bytes(row[28]),
            }

            # Insertar el registro en la tabla HBase
            table.put(row_key, data)
csv_file = '/tmp/nosql/airData/2008.csv'

# Abrir el archivo CSV y leerlo con csv.reader
with open(csv_file, 'r') as csvfile:
    csvreader = csv.reader(csvfile)

    # Ignorar la primera línea (cabeceras)
    next(csvreader)

    # Para cada línea en el archivo CSV, insertar un registro en la tabla HBase
    with connection.table(table_name).batch(batch_size=100) as table:
        for row in csvreader:
            # La primera columna "Year" se usará como la clave de fila en HBase
            row_key = to_bytes(row[16]+'-'+row[17]+'_'+row[0]+row[1].zfill(2)+row[2].zfill(2)+row[4])

            data = {
                #'info:Year': to_bytes(row[0]),
                #'info:Month': to_bytes(row[1]),
                #'info:DayofMonth': to_bytes(row[2]),
                #'info:DayOfWeek': to_bytes(row[3]),
                #'info:DepTime': to_bytes(row[4]),
                #'info:CRSDepTime': to_bytes(row[5]),
                #'info:ArrTime': to_bytes(row[6]),
                #'info:CRSArrTime': to_bytes(row[7]),
                'info:NombreAerolinea': to_bytes(row[8]),#UniqueCarrier
                'info:FlightNum': to_bytes(row[9]),
                'info:TailNum': to_bytes(row[10]),
                'info:Duracion': to_bytes(row[11]),#ActualElapsedTime 
                #'info:CRSElapsedTime': to_bytes(row[12]),
                #'info:AirTime': to_bytes(row[13]),
                'info:LlegRetraso': to_bytes(row[14]),#ArrDelay
                'info:SalRetraso': to_bytes(row[15]),#DepDelay
                'info:Origin': to_bytes(row[16]),
                'info:Dest': to_bytes(row[17]),
                'info:Distance': to_bytes(row[18]),
                #'info:TaxiIn': to_bytes(row[19]),
                #'info:TaxiOut': to_bytes(row[20]),
                'info:Cancelled': to_bytes(row[21])
                #'info:CancellationCode': to_bytes(row[22]),
                #'info:Diverted': to_bytes(row[23]),
                #'info:CarrierDelay': to_bytes(row[24]),
                #'info:WeatherDelay': to_bytes(row[25]),
                #'info:NASDelay': to_bytes(row[26]),
                #'info:SecurityDelay': to_bytes(row[27]),
                #'info:LateAircraftDelay': to_bytes(row[28]),
            }

            # Insertar el registro en la tabla HBase
            table.put(row_key, data)
