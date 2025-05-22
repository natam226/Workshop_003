import json
import pandas as pd
import psycopg2
from kafka import KafkaConsumer
import joblib
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

def load_db_credentials(path='../credentials.json'):
    with open(path) as f:
        return json.load(f)

def insert_prediction(cursor, conn, data):
    data = {
        'gdp_per_capita': float(data['gdp_per_capita']),
        'social_support': float(data['social_support']),
        'life_expectancy': float(data['life_expectancy']),
        'freedom': float(data['freedom']),
        'corruption': float(data['corruption']),
        'region': str(data['region']),
        'y_real': float(data['y_real']),
        'y_pred': float(data['y_pred'])
    }

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            gdp_per_capita NUMERIC(10, 6),
            social_support NUMERIC(10, 6),
            life_expectancy NUMERIC(10, 6),
            freedom NUMERIC(10, 6),
            corruption NUMERIC(10, 6),
            region VARCHAR(100),
            y_real NUMERIC(10, 6),
            y_pred NUMERIC(10, 6)
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


def kafka_consumer(y_test_path, model_path='../models/trained_model.pkl', topic='happiness_data_topic', credentials_path='../credentials.json'):
    db_params = load_db_credentials(credentials_path)

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    model = joblib.load(model_path)

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

        y_real = data.pop('y_real')
        df_row = pd.DataFrame([data])

        y_pred = model.predict(df_row)[0]

        prediction_data = {**data, 'y_real': y_real, 'y_pred': y_pred}
        insert_prediction(cursor, conn, prediction_data)
        print(f"[Consumer] Guardado en DB: {prediction_data}")

    cursor.close()
    conn.close()
    print("[Consumer] Proceso terminado. Conexión cerrada.")


if __name__ == "__main__":
    kafka_consumer("../output/y_test.csv")
