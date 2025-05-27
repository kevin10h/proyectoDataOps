import pika
import pandas as pd
import glob
import pathlib
import json
import time
import os

# Retry loop para conexión con RabbitMQ
connection = None
for i in range(20):
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        print("[extractor] ✅ Conectado a RabbitMQ.")
        break
    except pika.exceptions.AMQPConnectionError:
        print(f"[extractor] ❌ Intento {i+1}: RabbitMQ no disponible. Reintentando en 3s...")
        time.sleep(3)

if not connection:
    raise Exception("🛑 No se pudo conectar a RabbitMQ.")

channel = connection.channel()
channel.queue_declare(queue='etl_trigger')
channel.queue_declare(queue='etl_transform')

def extraccion():
    columnas = ['Producto', 'Categoría', 'Cantidad Producida', 'Unidad Medida',
                'Costo Unitario ($)', 'Costo Total ($)', 'Planta de Producción',
                'Método Transporte', 'Estado Logístico', 'Fecha de Producción']
    df = pd.DataFrame(columns=columnas)
    # La ruta se basa en el directorio data dentro de este servicio
    ruta_archivos = os.path.join(os.path.dirname(__file__), "data")
    # Extraer archivos CSV
    for archivo in glob.glob(f'{ruta_archivos}/*.csv'):
        df_tmp = pd.read_csv(archivo)
        df = pd.concat([df, df_tmp], ignore_index=True)
    # Extraer archivos JSON (suponiendo que cada línea es un registro)
    for archivo in glob.glob(f'{ruta_archivos}/*.json'):
        df_tmp = pd.read_json(archivo, lines=True)
        df = pd.concat([df, df_tmp], ignore_index=True)
    # Extraer archivos XML
    for archivo in glob.glob(f'{ruta_archivos}/*.xml'):
        df_tmp = pd.read_xml(archivo)
        df = pd.concat([df, df_tmp], ignore_index=True)
    # Extraer archivos XLSX
    for archivo in glob.glob(f'{ruta_archivos}/*.xlsx'):
        df_tmp = pd.read_excel(archivo)
        df = pd.concat([df, df_tmp], ignore_index=True)
    return df

def callback(ch, method, properties, body):
    print("[extractor] 🎯 Trigger recibido. Ejecutando extracción ETL...")
    df = extraccion()
    print(f"[extractor] 📥 Datos extraídos: {len(df)} registros.")
    # Convertir todo el DataFrame a JSON (lista de registros)
    data_json = df.to_json(orient="records")
    channel.basic_publish(
        exchange='',
        routing_key='etl_transform',
        body=data_json
    )
    print("[extractor] 🚀 Datos enviados a 'etl_transform'.")

channel.basic_consume(queue='etl_trigger', on_message_callback=callback, auto_ack=True)
print("[extractor] 🕒 Esperando trigger en 'etl_trigger'...")
channel.start_consuming()
