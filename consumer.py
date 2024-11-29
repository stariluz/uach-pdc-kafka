import json
from kafka import KafkaConsumer
from pymongo import MongoClient
import threading
import matplotlib.pyplot as plt
from datetime import datetime

# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'
MONGO_URI = 'mongodb://root:this_is_a_password@localhost:27017'

OPENWEATHER_TOPIC = 'openWeather'
NASA_WEATHER_TOPIC = 'nasa'

OPENWEATHER_DB_NAME = 'weather'
NASA_DB_NAME = 'mars_weather'
COLLECTION_NAME = 'current'

# Inicializa la conexión a MongoDB
mongo_client = MongoClient(MONGO_URI)
openweather_db = mongo_client[OPENWEATHER_DB_NAME]
nasa_db = mongo_client[NASA_DB_NAME]
openweather_collection = openweather_db[COLLECTION_NAME]
nasa_collection = nasa_db[COLLECTION_NAME]

# Listas para almacenar datos para la gráfica 
timestamps_openweather = []
temperatures_openweather = []
timestamps_nasa = []
temperatures_nasa = []

def consume_openweather():
    """Consume mensajes de OpenWeather y guárdalos en MongoDB."""
    openweather_consumer = KafkaConsumer(
        OPENWEATHER_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Conectado al tópico '{OPENWEATHER_TOPIC}'")
    try:
        for message in openweather_consumer:
            data = message.value
            print(f"Mensaje recibido de OpenWeather: {data}")
            openweather_collection.insert_one(data)
            print(f"Datos de OpenWeather guardados en MongoDB: {data}")
    except Exception as e:
        print(f"Error al consumir OpenWeather: {e}")
    finally:
        openweather_consumer.close()

def consume_nasa():
    """Consume mensajes de NASA y guárdalos en MongoDB."""
    nasa_consumer = KafkaConsumer(
        NASA_WEATHER_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Conectado al tópico '{NASA_WEATHER_TOPIC}'")
    try:
        for message in nasa_consumer:
            data = message.value
            print(f"Mensaje recibido de NASA: {data}")
            nasa_collection.insert_one(data)
            print(f"Datos de NASA guardados en MongoDB: {data}")
    except Exception as e:
        print(f"Error al consumir NASA: {e}")
    finally:
        nasa_consumer.close()


def fetch_and_update_weather_data():
    """Obtiene datos de MongoDB y actualiza las listas para la gráfica."""
    global timestamps_openweather, temperatures_openweather, timestamps_nasa, temperatures_nasa
    while True:
        try:
            # Consulta MongoDB para OpenWeather
            openweather_data = list(openweather_collection.find().sort("timestamp", 1))
            if openweather_data:
                timestamps_openweather = [datetime.fromtimestamp(entry['timestamp']) for entry in openweather_data]
                temperatures_openweather = [entry['temperature'] for entry in openweather_data]

            # Consulta MongoDB para NASA
            nasa_data = list(nasa_collection.find().sort("timestamp", 1))
            if nasa_data:
                timestamps_nasa = [datetime.fromtimestamp(entry['timestamp']) for entry in nasa_data]
                temperatures_nasa = [entry['temperature'] for entry in nasa_data]

        except Exception as e:
            print(f"Error al obtener datos de MongoDB: {e}")

def plot_weather_data():
    """Grafica los datos en tiempo real."""
    plt.ion()  # Modo interactivo
    fig, ax = plt.subplots()
    ax.set_title('Temperatura en tiempo real: OpenWeather vs NASA')
    ax.set_xlabel('Tiempo')
    ax.set_ylabel('Temperatura (°C)')

    while True:
        try:
            if timestamps_openweather and temperatures_openweather and timestamps_nasa and temperatures_nasa:
                ax.clear()

                # Graficar los datos de OpenWeather
                ax.plot(timestamps_openweather, temperatures_openweather, label='OpenWeather', color='blue')

                # Graficar los datos de NASA
                ax.plot(timestamps_nasa, temperatures_nasa, label='NASA (Marte)', color='red')

                ax.legend()
                plt.gcf().autofmt_xdate()
                plt.pause(1)  # Pausa para permitir la actualización

        except KeyboardInterrupt:
            print("\nCerrando la gráfica...")
            break
        except Exception as e:
            print(f"Error al graficar: {e}")

def main():
    """Crea hilos para ejecutar los consumidores y actualizar los datos."""
    openweather_thread = threading.Thread(target=consume_openweather, daemon=True)
    nasa_thread = threading.Thread(target=consume_nasa, daemon=True)

    openweather_thread.start()
    nasa_thread.start()

    # Hilo para obtener y actualizar datos de MongoDB
    fetch_weather_thread = threading.Thread(target=fetch_and_update_weather_data, daemon=True)
    fetch_weather_thread.start()

    # Graficar en el hilo principal
    plot_weather_data()


if __name__ == '__main__':
    main ()
