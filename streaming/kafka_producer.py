import json
import time
from kafka import KafkaProducer

def kafka_producer(X_test, y_test, topic='happiness_data_topic', sleep_time=1):
    """
    Envía los datos de prueba (X_test) junto con los valores reales (y_test)
    fila por fila al topic de Kafka.
    """
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("[Producer] Enviando datos a Kafka...")
    for i, (idx, row) in enumerate(X_test.iterrows()):
        message = row.to_dict()
        message['y_real'] = float(y_test.iloc[i])  # Añade el valor real
        producer.send(topic, message)
        #print(f"[Producer] Enviado: {message}")
        time.sleep(sleep_time)

    producer.flush()
    print("[Producer] Envío completado.")
