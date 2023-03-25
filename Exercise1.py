# Detalle Airports: Genera la tabla mbd10:airport
# Arguments: --namespace --table
# Ej. Ejecución: /opt/miniconda3/bin/python3.8 ./script_ex1.py --namespace mbd10 --table airport

import happybase
import csv
import argparse
import Hbase_thrift

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

def getConn(host, namespace):
    return happybase.Connection(host=host,
                                port=9090,
                                table_prefix=namespace,
                                table_prefix_separator=':')

def to_bytes(value):
    return bytes(str(value), 'utf-8')


def create_table(conn, table_name, column_families):
    try:
        conn.create_table(table_name, column_families)
        print(f'Tabla {table_name} creada en el namespace {args.namespace}')
    except Exception as e:
        print('Unable to create table')
        if isinstance(e, Hbase_thrift.AlreadyExists):
             print(f'Tabla {table_name} ya existe en el namespace {args.namespace}')
             while True:
                user_input = input('¿Desea borrarla y crearla de nuevo? (S/n): ')
                if user_input.lower() == 's':
                    conn.disable_table(table_name)
                    conn.delete_table(table_name)
                    conn.create_table(table_name, column_families)
                    print(f'Tabla {table_name} borrada y creada de nuevo en el namespace {args.namespace}')
                    break
                elif user_input.lower() == 'n':
                    break
                    exit()
                else:
                    print("Opción inválida. Intente de nuevo.")
        else:
             print('Unknown Exception:', e)


def load_data(conn, file_path, table_name:str):
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)

        # Ignorar la primera línea (cabeceras)
        next(csvreader)

        with conn.table(table_name).batch(batch_size=1000) as table:
            for row in csvreader:
                if len(row) != 7:
                    continue
                row_key = to_bytes(row[0])

                data = {
                    'airport:name': to_bytes(row[1]),
                    'airport:city': to_bytes(row[2]),
                    'airport:state': to_bytes(row[3]),
                    'airport:country': to_bytes(row[4]),
                    'airport:lat': to_bytes(row[5]),
                    'airport:long': to_bytes(row[6]),
                }

                table.put(row_key, data)


if __name__ == '__main__':
    conn = getConn('0.0.0.0', args.namespace)

    if conn.transport.is_open():
        conn.close()

    conn.open()
    column_families = {
        'airport': dict()
    }
    file_path = '/tmp/nosql/airData/airports.csv'

    create_table(conn, args.table, column_families)
    load_data(conn, file_path, args.table)

    conn.close()

