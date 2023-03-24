import re
import happybase

def getConn():
    return happybase.Connection(host='localhost', port=9090)
def get_duration_avg_for_route(rows, origin, dest):
    # Obtener la duración de todos los vuelos de la ruta
    aerolineas = []
    #print(list(rows))
    for key, data in rows:
        try:
            aero = data[b'datos:NombreAerolinea'].decode('utf-8')
            aerolineas.append(aero)
        except ValueError:
            pass
    aerolineas_uniq = list(set(aerolineas))
    aero_info=[]
    for aerolinea in aerolineas_uniq:
        llegadas=[]
        salidas=[]
        for key, data in rows:
            
            try:
                aero = data[b'datos:NombreAerolinea'].decode('utf-8')
                aerolineas.append(aero)
            except ValueError:
                pass
            if aero == aerolinea:
                try:
                    llegada=float(data[b'datos:LlegRetraso'].decode('utf-8'))
                    llegadas.append(llegada)
                except ValueError:
                    pass
                try:
                    salida=float(data[b'datos:SalRetraso'].decode('utf-8'))
                    salidas.append(salida)
                except ValueError:
                    pass
        # Calcular la duración promedio
        avg_llegadas = sum(llegadas) / len(llegadas)
        avg_salidas = sum(salidas) / len(llegadas)
        datos={
            "Aerolinea": aerolinea,
            "AVG_Llegadas_Retaso": avg_llegadas,
            "AVG_Llegadas_Retraso": avg_salidas
        }
        aero_info.append(datos)
    
    return aero_info
 

# Obtener una conexión a la tabla de HBase
connection = getConn()
table_name = 'mbd10_30:origen-destino'
table = connection.table(table_name)
print(f'Conexion con la tabla {table_name} realizada en el namespace mbd10_30')

origin = 'ABE'
dest = 'ATL'
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
    avg_duration = get_duration_avg_for_route(list(rows), origin, dest)
    if avg_duration is not None:
        print(f"Duración promedio para la ruta {origin}-{dest}: {avg_duration}")
    else:
        print(f"No hay información de duración para la ruta {origin}-{dest}")
