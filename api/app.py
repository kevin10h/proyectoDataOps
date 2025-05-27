from fastapi import FastAPI
import pika

app = FastAPI()

@app.get("/")
def home():
    """
    Endpoint base para verificar el estado de la API.
    """
    return {"mensaje": "✅ API DataOps en funcionamiento"}

@app.get("/run-etl")
def run_etl():
    """
    Endpoint para lanzar el proceso ETL completo.
    Envía un mensaje 'start' a la cola 'etl_trigger' de RabbitMQ,
    lo cual inicia la ejecución en el microservicio 'extractor'.
    """
    try:
        # Establecer conexión con RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='etl_trigger')

        # Enviar el trigger
        channel.basic_publish(
            exchange='',
            routing_key='etl_trigger',
            body='start'
        )
        connection.close()

        return {
            "status": "🚀 Trigger enviado correctamente",
            "mensaje": "El microservicio extractor debería empezar la ejecución ETL."
        }
    except Exception as e:
        return {
            "error": str(e),
            "mensaje": "❌ No se pudo enviar el trigger. Revisa la conexión a RabbitMQ."
        }
