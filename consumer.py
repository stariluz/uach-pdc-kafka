import json
from kafka import KafkaConsumer
from pymongo import MongoClient
import threading
import matplotlib.pyplot as plt
from datetime import datetime


# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'  # Cambia esto según la configuración de tu broker
WHEATER_TOPIC = 'openWeather'

MONGO_URI = 'mongodb://root:this_is_a_password@localhost:27017'
DATABASE_NAME = 'weather'
COLLECTION_NAME = 'current'

# Inicializa la conexión a MongoDB
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[DATABASE_NAME]
mongo_collection = mongo_db[COLLECTION_NAME]

# Lista para almacenar datos para la gráfica
timestamps = []
temperatures = []

def consume_wheater():
    """Consume los mensajes del tópico y los guarda en MongoDB."""
    # Inicializa el consumidor de Kafka
    wheather_consumer = KafkaConsumer(
        WHEATER_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',  # Leer desde el inicio si es la primera vez
        enable_auto_commit=True,
        group_id='weather-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Conectado al tópico '{WHEATER_TOPIC}' en {KAFKA_BROKER}. Esperando mensajes...")
    try:
        for message in wheather_consumer:
            data = message.value
            print(f"Mensaje recibido: {data}")

            # Guardar en MongoDB
            mongo_collection.insert_one(data)
            print(f"Datos guardados en MongoDB: {data}")
    except KeyboardInterrupt:
        print("\nDeteniendo el consumidor...")
    finally:
        wheather_consumer.close()
        mongo_client.close()

def fetch_and_update_weather_data():
    """Obtiene datos de MongoDB y actualiza las listas para la gráfica."""
    global timestamps, temperatures
    while True:
        try:
            # Consulta MongoDB
            all_data = list(mongo_collection.find().sort("timestamp", 1))
            if all_data:
                timestamps = [datetime.fromtimestamp(entry['timestamp']) for entry in all_data]
                temperatures = [entry['temperature'] for entry in all_data]
        except Exception as e:
            print(f"Error al obtener datos de MongoDB: {e}")

def plot_weather_data():
    """Grafica los datos en tiempo real."""
    plt.ion()  # Modo interactivo
    fig, ax = plt.subplots()
    ax.set_title('Temperatura en tiempo real')
    ax.set_xlabel('Tiempo')
    ax.set_ylabel('Temperatura (°C)')

    while True:
        try:
            if timestamps and temperatures:
                ax.clear()
                ax.plot(timestamps, temperatures, label='Temperatura')
                ax.legend()
                plt.gcf().autofmt_xdate()
                plt.pause(1)  # Pausa para permitir la actualización
        except KeyboardInterrupt:
            print("\nCerrando la gráfica...")
            break
        except Exception as e:
            print(f"Error al graficar: {e}")

def main():
    """Crea un hilo para ejecutar el consumidor."""
    consumer_weather_thread = threading.Thread(target=consume_wheater, daemon=True)
    consumer_weather_thread.start()

    # Hilo para obtener datos de MongoDB
    fetch_weather_thread = threading.Thread(target=fetch_and_update_weather_data, daemon=True)
    fetch_weather_thread.start()

    # Mantén el programa principal activo mientras el hilo está ejecutándose
    print("Hilo del consumidor iniciado. Presiona Ctrl+C para detener.")
    try:
        while consumer_weather_thread.is_alive():
            consumer_weather_thread.join(1)
    except KeyboardInterrupt:
        print("\nSaliendo del programa principal...")

if __name__ == '__main__':
    main()
