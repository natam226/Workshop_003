# kafka_consumer.py

import json
import pandas as pd
import psycopg2
from kafka import KafkaConsumer
import joblib
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# Configuración base de datos PostgreSQL

def insert_prediction(cursor, conn, data):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            gdp_per_capita REAL,
            social_support REAL,
            life_expectancy REAL,
            freedom REAL,
            corruption REAL,
            region TEXT,
            y_real REAL,
            y_pred REAL
        )
    ''')
    conn.commit()

    cursor.execute('''
        INSERT INTO predictions (gdp_per_capita, social_support, life_expectancy, freedom, corruption, region, y_real, y_pred)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        data['gdp_per_capita'], data['social_support'], data['life_expectancy'],
        data['freedom'], data['corruption'], data['region'],
        data['y_real'], data['y_pred']
    ))
    conn.commit()


def kafka_consumer(y_test_path, model_path='happiness_model.pkl', topic='happiness_topic'):
    # Conectar a la base de datos
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Cargar valores reales y modelo
    y_test = pd.read_csv(y_test_path)
    model = joblib.load(model_path)

    # Configurar Kafka
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='happiness_group'
    )

    print("[Consumer] Esperando mensajes de Kafka...")
    for i, msg in enumerate(consumer):
        data = msg.value
        df_row = pd.DataFrame([data])

        # Predicción
        y_pred = model.predict(df_row)[0]
        y_real = y_test.iloc[i].values[0]

        # Combinar y guardar en BD
        prediction_data = {**data, 'y_real': y_real, 'y_pred': y_pred}
        insert_prediction(cursor, conn, prediction_data)
        print(f"[Consumer] Guardado en DB: {prediction_data}")

        if i >= len(y_test) - 1:
            break

    cursor.close()
    conn.close()
    print("[Consumer] Proceso terminado. Conexión cerrada.")


if __name__ == "__main__":
    kafka_consumer("../output/y_test.csv")
