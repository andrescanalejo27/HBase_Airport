# Como extra sería interesante obtener el modelo de aeronave más usado por la aerolínea para dicha ruta.
import re
import happybase
import collections
import json

def getConn():
    return happybase.Connection(host='localhost')
def get_duration_avg_for_route(rows, origin, dest):
    # Obtener la duración de todos los vuelos de la ruta
    counter = collections.defaultdict(lambda: [0, collections.Counter()])

    for key, data in rows:
        try:
            aero = data[b'datos:NombreAerolinea'].decode('utf-8')
            tailnum = data[b'datos:TailNum'].decode('utf-8')
            counter[aero][0] += 1
            counter[aero][1][tailnum] += 1
        except ValueError:
            pass 

    result = []
    model_tailnum_dict = {}
    model_freq = {}
    table = connection.table('mbd10_30:planes')
    for aerolinea, (freq_aerolinea, tailnum_counter) in counter.items():
        for tailnum, freq_tailnum in tailnum_counter.items():
            new_search = table.scan(row_prefix=tailnum.encode('utf-8'))
            new_search=list(new_search)
            if new_search:
                modelo = new_search[0][1][b'datos:model'].decode('utf-8')
                if modelo in model_tailnum_dict:
                    model_tailnum_dict[modelo].append(tailnum)
                    # Si el modelo no existe en el diccionario, lo creamos y agregamos el tailnum
                else:
                    model_tailnum_dict[modelo] = [tailnum]

                if modelo in model_freq:
                    model_freq[modelo] += 1
                else:
                    model_freq[modelo] = 1
            else:
                modelo = None
            result.append({
                "Aerolínea": aerolinea,
                "Frecuencia de Aerolínea": freq_aerolinea,
                #"TailNum": tailnum,
                "Model": modelo,
                #"Frecuencia de TailNum": freq_tailnum
            })
    for r in result:
        modelo = r['Model']
        if modelo:
            freq = model_freq[modelo]
        else:
            freq = 0
        r['Frecuencia de Model'] = freq

    # Eliminar duplicados
    unique_results = []
    unique_tuples = set()
    for r in result:
        t = (r['Aerolínea'], r['Frecuencia de Aerolínea'], r['Model'], r['Frecuencia de Model'])
        if t not in unique_tuples:
            unique_tuples.add(t)
            unique_results.append(dict(zip(('Aerolínea', 'Frecuencia de Aerolínea', 'Model', 'Frecuencia de Model'), t)))

    result = sorted(unique_results, key=lambda x: (x["Frecuencia de Aerolínea"], x["Frecuencia de Model"]), reverse=True)
    # Inicializa el diccionario final
    final_dict = {}

    # Itera sobre la lista de diccionarios original
    for d in result:
        # Encuentra los valores de media de retraso para la aerolínea y TailNum actuales
        aerolinea = d['Aerolínea']
        # tailnum = d['TailNum']
        model=d['Model']
        llegadas_retraso = []
        salidas_retraso = []
        freq_mod=0
        for keys,r in rows:
            if r[b'datos:NombreAerolinea'].decode('utf-8') == aerolinea and (model is not None and model.strip()!='') and r[b'datos:TailNum'].decode('utf-8') in model_tailnum_dict[model]:
                lleg = r[b'datos:LlegRetraso'].decode('utf-8') if r[b'datos:LlegRetraso'].decode('utf-8') != 'NA' and r[b'datos:LlegRetraso'].decode('utf-8').isdigit() else 0
                sal = r[b'datos:LlegRetraso'].decode('utf-8') if r[b'datos:LlegRetraso'].decode('utf-8') != 'NA' and r[b'datos:LlegRetraso'].decode('utf-8').isdigit() else 0
                try:
                    llegadas_retraso.append(float(lleg))
                    salidas_retraso.append(float(sal))
                    freq_mod=1+freq_mod
                except ValueError:
                    pass
                
        avg_llegadas_retraso = sum(llegadas_retraso) / len(llegadas_retraso) if llegadas_retraso else None
        avg_salidas_retraso = sum(salidas_retraso) / len(salidas_retraso) if salidas_retraso else None
        if model is not None and model.strip()!='' and model !='null':
            result_dict = {
                'Aerolinea': aerolinea,
                'Numero de vuelos totales Aerolinea': d['Frecuencia de Aerolínea'],
                'Modelo': d['Model'],
                'Numero de vuelos on este modelo': freq_mod,
                'AVG_Llegadas_Retaso': avg_llegadas_retraso,
                'AVG_Salidas_Retraso': avg_salidas_retraso
            }
            final_dict[f"{aerolinea}_{model}"] = result_dict
    return final_dict
 

# Obtener una conexión a la tabla de HBase
connection = getConn()
table_name = 'mbd10_30:origen-destino'
table = connection.table(table_name)
print(f'Conexion con la tabla {table_name} realizada en el namespace mbd10_30')

origin = 'JFK'
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

rows = table.scan(row_prefix=row_prefix.encode('utf-8'))

# Verificar si existen rows que matcheen el prefijo
if not any(rows):
    print(f"No hay rows en la tabla con el prefijo {row_prefix}")
else:
    # Calcular la duración promedio para la ruta dada
    results = get_duration_avg_for_route(list(rows), origin, dest)
    if results is not None:
        print(f"Duración promedio para la ruta {origin}-{dest}:")
        print(json.dumps(results, indent=4))
    else:
        print(f"No hay información de duración para la ruta {origin}-{dest}")
connection.close()
