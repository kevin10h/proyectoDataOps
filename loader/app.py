import pika
import json
import time
import pandas as pd
from sqlalchemy import create_engine

# Retry loop para conexión con RabbitMQ
connection = None
for i in range(20):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        print("[loader] ✅ Conectado a RabbitMQ.")
        break
    except pika.exceptions.AMQPConnectionError:
        print(f"[loader] ❌ Intento {i+1}: RabbitMQ no disponible. Reintentando en 3s...")
        time.sleep(3)

if not connection:
    raise Exception("🛑 No se pudo conectar a RabbitMQ.")

channel = connection.channel()
channel.queue_declare(queue='etl_load')

def cargar_a_db(dfs, user="user", password="password", host="postgres", port="5432", database="dataops"):
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    for nombre_tabla, json_str in dfs.items():
        df = pd.read_json(json_str, orient="records")
        print(f"[loader] 📂 Cargando {nombre_tabla} en PostgreSQL...")
        df.to_sql(nombre_tabla, engine, if_exists="replace", index=False)
    print(f"[loader] ✅ Datos cargados en PostgreSQL ({database}).")

def callback(ch, method, properties, body):
    print("[loader] 📥 Mensaje recibido de 'etl_load'.")
    try:
        data = json.loads(body.decode())
        cargar_a_db(data)
    except Exception as e:
        print(f"[loader] ⚠️ Error al cargar datos: {str(e)}")

channel.basic_consume(queue='etl_load', on_message_callback=callback, auto_ack=True)
print("[loader] 🕒 Esperando mensajes en 'etl_load'...")
channel.start_consuming()
