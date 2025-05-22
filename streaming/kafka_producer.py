# kafka_producer.py

import json
import time
from kafka import KafkaProducer
import pandas as pd

def kafka_producer(X_test, topic='happiness_topic', sleep_time=1):
    """
    Envía los datos de prueba (X_test) fila por fila al topic de Kafka.
    """
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("[Producer] Enviando datos a Kafka...")
    for i, row in X_test.iterrows():
        message = row.to_dict()
        producer.send(topic, message)
        print(f"[Producer] Enviado: {message}")
        time.sleep(sleep_time)  # Simula flujo de datos

    producer.flush()
    print("[Producer] Envío completado.")
