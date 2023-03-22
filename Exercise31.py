import pandas as pd


def getConn():
    return happybase.Connection(host='localhost')
connection=getConn()

table = connection.table('mbd10_30:airport')
def get_duration_avg(table, origin, dest):
    # Filtrar las filas que corresponden a la ruta deseada
    row_prefix = origin + '-' + dest
    rows = table.scan(row_prefix=row_prefix)

    # Calcular la duración promedio
    total_duration = 0
    num_flights = 0
    for key, data in rows:
        duration = int(data[b'info:Duracion'].decode('utf-8'))
        total_duration += duration
        num_flights += 1

    if num_flights > 0:
        avg_duration = total_duration / num_flights
        return avg_duration
    else:
        return None

# Obtener una conexión a la tabla de HBase
connection = getConn()
table = connection.table(table_name)

# Obtener todos los pares origen-destino disponibles en la tabla
origin_dest_pairs = set()
for key, data in table.scan():
    origin_dest_pairs.add(key.split(b'-'))

# Calcular la duración promedio para cada ruta y almacenar los resultados en un DataFrame
results = []
for origin, dest in origin_dest_pairs:
    avg_duration = get_duration_avg(table, origin.decode('utf-8'), dest.decode('utf-8'))
    if avg_duration is not None:
        results.append({'origin': origin.decode('utf-8'), 'dest': dest.decode('utf-8'), 'avg_duration': avg_duration})

df = pd.DataFrame(results)