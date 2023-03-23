import re
import happybase

def getConn():
    return happybase.Connection(host='localhost', port=9090)
def get_duration_avg_for_route(rows, origin, dest):
   
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
table_name = 'mbd10_30:origen-destino'
table = connection.table(table_name)

# Obtener la pareja de origen y destino dada por parámetro
# origin = raw_input("Origen: ")
# if not re.match("^[a-z]*$", origin):
#     print ("Error! Only letters a-z allowed!")
#     sys.exit()
# elif len(origin) > 4:
#     print "Error! Only 15 characters allowed!"
#     sys.exit()

# print "Your input was:", input_str
# dest = raw_input("Destino: ")
# if not re.match("^[a-z]*$", dest):
#     print ("Error! Only letters a-z allowed!")
#     sys.exit()
# elif len(dest) > 4:
#     print "Error! Only 4 characters allowed!"
#     sys.exit()

# print "Your input was:", input_str
origin = 'MAD'
dest = 'BCN'

# Filtrar las filas que corresponden a la ruta deseada
row_prefix = f"{origin}-{dest}".encode('utf-8')
rows = table.scan(row_prefix=row_prefix)

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
