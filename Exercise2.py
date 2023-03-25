# Detalle Vuelos: Genera la tabla mbd10:airData
# Arguments: --namespace --table
# Ej. Ejecución: /opt/miniconda3/bin/python3.8 ./Exercise2.py --namespace mbd10 --table airData

import happybase
import argparse
import csv
from Exercise1 import getConn, to_bytes, create_table

parser = argparse.ArgumentParser()

parser.add_argument('--namespace', 
                    type=str, 
                    required=True,
                    help='specify namespace to use')

parser.add_argument('--table', 
                    type=str,
                    required=True, 
                    help='specify table name')

args = parser.parse_args()

def load_data(conn, csv_path, table_name):
    arr = [1000000*i for i in range(1, 11)]
    with open(csv_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Ignore headers

        print('Iniciando carga de datos')
        with conn.table(table_name).batch(batch_size=1000) as table:
            count = 0
            for row in csvreader:
                row_key = to_bytes(row[0] + row[1].zfill(2) + row[2].zfill(2) + row[4] + '_' + str(count))

                data = {
                    'info:Year': to_bytes(row[0]),
                    'info:Month': to_bytes(row[1]),
                    'info:DayofMonth': to_bytes(row[2]),
                    'info:DepTime': to_bytes(row[4]),
                    'info:ArrTime': to_bytes(row[6]),
                    'info:UniqueCarrier': to_bytes(row[8]),
                    'info:FlightNum': to_bytes(row[9]),
                    'info:TailNum': to_bytes(row[10]),
                    'info:Origin': to_bytes(row[16]),
                    'info:Dest': to_bytes(row[17]),
                    'info:Distance': to_bytes(row[18]),
                    'info:Cancelled': to_bytes(row[21]),
                }

                table.put(row_key, data)
                count += 1
                if count in arr:
                    print(f'{count} registros procesados')
        print('Carga finalizada')

if __name__ == '__main__':
    conn = getConn('0.0.0.0', args.namespace)

    if conn.transport.is_open():
        conn.close()

    conn.open()

    column_families = {
        'info': dict()
    }

    file_path = ['/tmp/nosql/airData/2007.csv', '/tmp/nosql/airData/2008.csv']

    create_table(conn, args.table, column_families)
    
    for csv_year in file_path: 
        print('Datos:', csv_year)
        load_data(conn, csv_year, args.table)

    conn.close()
