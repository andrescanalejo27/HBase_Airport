import re
import happybase
import argparse
from Exercise1 import getConn

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
    
def access_table(conn, table_name):
    try:
        table = conn.table(table_name)
        print(f'Conexion con la tabla {table_name} realizada')
        return table
    except Exception as e:
        print(e)

def get_valid_input(prompt, max_len):
      while True:
        user_input = input(prompt).strip()
        if not re.match("^[a-z]*$", user_input, re.IGNORECASE):
            print(f"Error! Only letters a-z allowed!")
        elif len(user_input) > max_len:
            print(f"Error! Only {max_len} characters allowed!")
        else:
            return user_input

def get_duration_avg_for_route(rows):
    # Obtener la duración de todos los vuelos de la ruta
    durations = []
    for key, data in rows:
        try:
            duration = int(data[b'datos:Duracion'].decode('utf-8'))
            durations.append(duration)
        except ValueError:
            pass

    if durations:
        # Calcular la duración promedio
        avg_duration = sum(durations) / len(durations)
        return avg_duration
    else:
        return None

def search_route_duration(conn, table_name):
    table = access_table(conn, table_name)

    # Get the origin and destination from the user
    origin = get_valid_input("Enter Origin: ", 3)
    dest = get_valid_input("Enter Destination: ", 3)

    # Filter the rows that correspond to the desired route
    row_prefix = f"{origin}-{dest}"
    print(row_prefix)
    print("Buscando")
    rows = table.scan(row_prefix=row_prefix.encode('utf-8'))
    print("Búsqueda finalizada")

    # Check if any rows match the prefix
    if not any(rows):
        print(f"No rows in the table match the pair {row_prefix}")
    else:
        # Calculate the average duration for the route
        avg_duration = get_duration_avg_for_route(rows)
        if avg_duration is not None:
            print(f"Average duration for the route {origin}-{dest}: {avg_duration}")
        else:
            print(f"No duration information for the route {origin}-{dest}")

if __name__ == '__main__':
    conn = getConn('0.0.0.0', args.namespace)

    if conn.transport.is_open():
        conn.close()

    conn.open()

    search_route_duration(conn, args.table)

    conn.close()
