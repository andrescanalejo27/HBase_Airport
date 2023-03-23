
def getConn():
    return happybase.Connection(host='localhost')
connection=getConn()


def get_airline_delay_avg(table,origin, dest):

    # Concatenar origen y destino para obtener rowkey
    row_key = origin + '-' + dest

    # Obtener registros correspondientes al rowkey
    row = table.row(row_key)

    # Si no existe el rowkey, se devuelve None
    if not row:
        return None

    # Obtener retrasos en la hora de salida y llegada
    salidas = [int(value.decode()) for key, value in row.items() if key.startswith(b'info:SalRetraso')]
    llegadas = [int(value.decode()) for key, value in row.items() if key.startswith(b'info:LlegRetraso')]

    # Calcular la media de los retrasos en la hora de salida y llegada
    avg_salidas = statistics.mean(salidas) if len(salidas) > 0 else None
    avg_llegadas = statistics.mean(llegadas) if len(llegadas) > 0 else None

    return avg_salidas, avg_llegadas


# Obtener una conexión a la tabla de HBase
connection = getConn()
table_name = 'mbd10_30:origen-destino'
table = connection.table(table_name)

# Obtener todos los pares origen-destino disponibles en la tabla
origin_dest_pairs = set()
for key, data in table.scan():
    origin_dest_pairs.add(key.split(b'-'))

# Calcular la duración promedio para cada ruta y almacenar los resultados en un DataFrame
results = []
for origin, dest in origin_dest_pairs:
    avg_salidas , avg_llegadas = get_airline_delay_avg(table,origin.decode('utf-8'), dest.decode('utf-8'))
    if avg_salidas is not None :
        if avg_llegadas is not None:
            results.append({'origin': origin.decode('utf-8'), 'dest': dest.decode('utf-8'), 'avg_salidas': avg_salidas,'avg_llegadas': avg_llegadas })

df = pd.DataFrame(results)