import json
import time
from kafka import KafkaProducer

def kafka_producer(X_test, y_test, topic='happiness_data_topic', sleep_time=1):
    """
    Envía los datos de prueba (X_test) al topic de Kafka. 
    """
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("[Producer] Enviando datos a Kafka...")

    producer.flush()
    print("[Producer] Envío completado.")
