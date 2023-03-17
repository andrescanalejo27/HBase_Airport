#/opt/miniconda3/bin/python3.8

import happybase
import pandas as pd

def getConn():
    return happybase.Connection(host='0.0.0.0')

#Si al conectar da el error -> thriftpy2.transport.base.TTransportException: TTransportException(type=1, message="Could not connect to ('0.0.0.0', 9090)")
#ejecutar en una terminal: hbase thrift start -p 9090

#funcion auxiliar para convertir a bytes lo que queremos almacenar en HBase
def to_bytes(value):
  return bytes(str(value), 'utf-8')

tc = getConn().table('test:carriers')

tc.put(to_bytes('myKey'), {to_bytes('cf1:desc'): to_bytes('myValue')})

print(tc.row(to_bytes('AC')))

print(tc.row(to_bytes('myKey')))
