#2 Detalle de vuelos

	#2.1 

#2007.csv 
#Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,
#CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,
#WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay

import happybase
import csv

# Configuración de conexión a HBase
connection = happybase.Connection('localhost', port=9090)
table_name = 'mbd10_30:airData'
connection.create_table(
    table_name,
    {'info': dict()},  # familia de columnas
)
print(f'Tabla {table_name} creada en el namespace mbd10_30')
# Función auxiliar para convertir una cadena a bytes
def to_bytes(string):
    return string.encode('utf-8')

# Ruta del archivo CSV
csv_file = '/tmp/nosql/airData/2007.csv'
arr = [1000000*i for i in range(1, 11)]
# Abrir el archivo CSV y leerlo con csv.reader
with open(csv_file, 'r') as csvfile:
    csvreader = csv.reader(csvfile)

    # Ignorar la primera línea (cabeceras)
    next(csvreader)

    # Para cada línea en el archivo CSV, insertar un registro en la tabla HBase
    with connection.table(table_name).batch(batch_size=1000) as table:
        count=0
        for row in csvreader:
            # La primera columna "Year" se usará como la clave de fila en HBase
            row_key = to_bytes(row[0]+row[1].zfill(2)+row[2].zfill(2)+row[4]+'_'+str(count))

            # Los datos de cada columna se almacenarán en la familia de columnas "info"
            # Consulta de vuelos por mes o día: Posibilidad de obtener los vuelos de aun día o mes específico (YYYYMMDD o YYYYMM)
		    # Como detalle de información se deben obtener siempre todos los siguientes datos: hora de salida, hora de llegada, 
            # número de vuelo, origen, destino, número de aeronave y distancia recorrida.
            data = {
                'info:Year': to_bytes(row[0]),
                'info:Month': to_bytes(row[1]),
                'info:DayofMonth': to_bytes(row[2]),
                #'info:DayOfWeek': to_bytes(row[3]),
                'info:DepTime': to_bytes(row[4]),
                #'info:CRSDepTime': to_bytes(row[5]),
                'info:ArrTime': to_bytes(row[6]),
                #'info:CRSArrTime': to_bytes(row[7]),
                'info:UniqueCarrier': to_bytes(row[8]),
                'info:FlightNum': to_bytes(row[9]),
                'info:TailNum': to_bytes(row[10]),
                #'info:ActualElapsedTime': to_bytes(row[11]),
                #'info:CRSElapsedTime': to_bytes(row[12]),
                #'info:AirTime': to_bytes(row[13]),
                #'info:ArrDelay': to_bytes(row[14]),
                #'info:DepDelay': to_bytes(row[15]),
                'info:Origin': to_bytes(row[16]),
                'info:Dest': to_bytes(row[17]),
                'info:Distance': to_bytes(row[18]),
                #'info:TaxiIn': to_bytes(row[19]),
                #'info:TaxiOut': to_bytes(row[20]),
                'info:Cancelled': to_bytes(row[21]),
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
            count=count+1
            if count in arr:
                print(count)

print("Siguiente tabla")

csv_file2='/tmp/nosql/airData/2008.csv'
with open(csv_file2, 'r') as csvfile:
    csvreader = csv.reader(csvfile)

    # Ignorar la primera línea (cabeceras)
    next(csvreader)

    # Para cada línea en el archivo CSV, insertar un registro en la tabla HBase
    with connection.table(table_name).batch(batch_size=1000) as table:
        count=0
        for row in csvreader:
            # La primera columna "Year" se usará como la clave de fila en HBase
            row_key = to_bytes(row[0]+row[1].zfill(2)+row[2].zfill(2)+row[4]+'_'+str(count))

            # Los datos de cada columna se almacenarán en la familia de columnas "info"
            data = {
                'info:Year': to_bytes(row[0]),
                'info:Month': to_bytes(row[1]),
                'info:DayofMonth': to_bytes(row[2]),
                #'info:DayOfWeek': to_bytes(row[3]),
                'info:DepTime': to_bytes(row[4]),
                #'info:CRSDepTime': to_bytes(row[5]),
                'info:ArrTime': to_bytes(row[6]),
                #'info:CRSArrTime': to_bytes(row[7]),
                'info:UniqueCarrier': to_bytes(row[8]),
                'info:FlightNum': to_bytes(row[9]),
                'info:TailNum': to_bytes(row[10]),
                #'info:ActualElapsedTime': to_bytes(row[11]),
                #'info:CRSElapsedTime': to_bytes(row[12]),
                #'info:AirTime': to_bytes(row[13]),
                #'info:ArrDelay': to_bytes(row[14]),
                #'info:DepDelay': to_bytes(row[15]),
                'info:Origin': to_bytes(row[16]),
                'info:Dest': to_bytes(row[17]),
                'info:Distance': to_bytes(row[18]),
                #'info:TaxiIn': to_bytes(row[19]),
                #'info:TaxiOut': to_bytes(row[20]),
                'info:Cancelled': to_bytes(row[21]),
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
            count=count+1
            if count in arr:
                print(count)
connection.close()