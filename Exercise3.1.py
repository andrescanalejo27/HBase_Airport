import re
import happybase

def getConn():
    return happybase.Connection(host='localhost')
def get_duration_avg_for_route(rows, origin, dest):
    # Obtener la duración de todos los vuelos de la ruta
    durations = []
    print("Search")
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

# Obtener una conexión a la tabla de HBase
connection = getConn()
table_name = 'mbd10_30:origen-destino'
table = connection.table(table_name)
print(f'Conexion con la tabla {table_name} realizada en el namespace mbd10_30')
# Obtener la pareja de origen y destino dada por parámetro
origin = input("Enter Origin: ")
if not re.match("^[a-z]*$", origin, re.IGNORECASE):
    print ("Error! Only letters a-z allowed!")
    sys.exit()
elif len(origin) > 4:
    print ("Error! Only 15 characters allowed!")
    sys.exit()

print ("Your destiny is :", origin)
dest = input("Enter Destination: ")
if not re.match("^[a-z]*$", dest, re.IGNORECASE):
    print ("Error! Only letters a-z allowed!")
    sys.exit()
elif len(dest) > 4:
    print ("Error! Only 4 characters allowed!")
    sys.exit()

print ("Your destination is:", dest)
# origin = 'ABE'
# dest = 'DTW'

# Filtrar las filas que corresponden a la ruta deseada
row_prefix = origin+"-"+dest
print("Comienza la búsqueda")
rows = table.scan(row_prefix=row_prefix.encode('utf-8'))
print("Finaliza la búsqueda")
# Verificar si existen rows que matcheen el prefijo
if not any(rows):
    print(f"No hay rows en la tabla con el prefijo {row_prefix}")
else:
    # Calcular la duración promedio para la ruta dada
    avg_duration = get_duration_avg_for_route(rows, origin, dest)
    if avg_duration is not None:
        print(f"Duración promedio para la ruta {origin}-{dest}: {avg_duration}")
    else:
        print(f"No hay información de duración para la ruta {origin}-{dest}")
connection.close()