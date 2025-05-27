# manual_trigger.py

##PARA ejecutarlo manualmente dentro del contenedor api : docker exec -it api python manual_trigger.py

import pika

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='etl_trigger')

    channel.basic_publish(
        exchange='',
        routing_key='etl_trigger',
        body='start'
    )

    connection.close()
    print("✅ Trigger enviado correctamente a la cola 'etl_trigger'.")

except Exception as e:
    print(f"❌ Error al enviar trigger: {str(e)}")
    print("❌ No se pudo enviar el trigger. Revisa la conexión a RabbitMQ.")
    