import pika
import json
import time
import pandas as pd

# Retry loop para conexión con RabbitMQ
connection = None
for i in range(20):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        print("[transformer] ✅ Conectado a RabbitMQ.")
        break
    except pika.exceptions.AMQPConnectionError:
        print(f"[transformer] ❌ Intento {i+1}: RabbitMQ no disponible. Reintentando en 3s...")
        time.sleep(3)

if not connection:
    raise Exception("🛑 No se pudo conectar a RabbitMQ.")

channel = connection.channel()
channel.queue_declare(queue='etl_transform')
channel.queue_declare(queue='etl_load')

def transformacion(df_datos):
    df_datos = df_datos.dropna()
    df_datos['Cantidad Producida'] = pd.to_numeric(df_datos['Cantidad Producida'], errors='coerce')
    df_datos['Costo Unitario ($)'] = pd.to_numeric(df_datos['Costo Unitario ($)'], errors='coerce')
    df_datos['Costo Total ($)'] = pd.to_numeric(df_datos['Costo Total ($)'], errors='coerce')
    # Recalcular el costo total
    df_datos['Costo Total ($)'] = df_datos['Cantidad Producida'] * df_datos['Costo Unitario ($)']
    
    # Análisis 1: Costo total por planta de producción
    df_costo_planta = df_datos.groupby('Planta de Producción')[['Costo Total ($)']].sum().reset_index()
    # Análisis 2: Cantidad producida por categoría
    df_cantidad_categoria = df_datos.groupby(['Categoría', 'Unidad Medida'])[['Cantidad Producida']].sum().reset_index()
    # Análisis 3: Costo unitario promedio por método de transporte
    df_costo_transporte = df_datos.groupby('Método Transporte')[['Costo Unitario ($)']].mean().reset_index()
    # Análisis 4: Porcentaje de productos por estado logístico
    df_estado_logistico = df_datos['Estado Logístico'].value_counts(normalize=True).reset_index()
    df_estado_logistico.columns = ['Estado_Logistico', 'Porcentaje']
    # Análisis 5: Producción mensual
    df_datos['Fecha de Producción'] = pd.to_datetime(df_datos['Fecha de Producción'], errors='coerce')
    df_produccion_mensual = df_datos.groupby(df_datos['Fecha de Producción'].dt.to_period("M"))[['Cantidad Producida']].sum().reset_index()
    df_produccion_mensual['Fecha de Producción'] = df_produccion_mensual['Fecha de Producción'].astype(str)
    
    return {
        "produccion_general": df_datos,
        "costo_total_por_planta": df_costo_planta,
        "cantidad_por_categoria": df_cantidad_categoria,
        "costo_unitario_por_transporte": df_costo_transporte,
        "estado_logistico": df_estado_logistico,
        "produccion_mensual": df_produccion_mensual
    }

def transformacion_json(df):
    resultado = transformacion(df)
    # Convertir cada DataFrame a JSON con formato 'records'
    for key, df_val in resultado.items():
        resultado[key] = df_val.to_json(orient="records")
    return resultado

def callback(ch, method, properties, body):
    print("[transformer] 📩 Mensaje recibido del extractor.")
    try:
        data_json = body.decode()
        df = pd.read_json(data_json, orient="records")
        resultado_transformacion = transformacion_json(df)
        resultado_json = json.dumps(resultado_transformacion)
        channel.basic_publish(
            exchange='',
            routing_key='etl_load',
            body=resultado_json
        )
        print("[transformer] 🚀 Datos transformados enviados a 'etl_load'.")
    except Exception as e:
        print(f"[transformer] ⚠️ Error en transformación: {str(e)}")

channel.basic_consume(queue='etl_transform', on_message_callback=callback, auto_ack=True)
print('[transformer] 🕒 Esperando mensajes en "etl_transform"...')
channel.start_consuming()
# channel.close()