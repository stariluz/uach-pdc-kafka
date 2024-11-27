import time
import json
import requests
from kafka import KafkaProducer

# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'  # Cambia esto según la configuración de tu broker
WEATHER_TOPIC = 'openWeather'

# Configuración de OpenWeatherMap
API_KEY = 'f9c88108f60190a68fb9d3bc156cfeba' # API de stariluz, saca la tuya
LAT = 28.704218708714663
LON = -106.1394034006503
URL = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"

# Inicializa el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather_data():
    """Obtiene los datos del clima desde la API de OpenWeatherMap."""
    try:
        response = requests.get(URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error al obtener los datos del clima: {e}")
        return None

def send_to_kafka(data, topic):
    """Envía los datos al tópico de Kafka."""
    try:
        producer.send(topic, value=data)
        producer.flush()
        print(f"Datos enviados a Kafka: {data}")
    except Exception as e:
        print(f"Error al enviar datos a Kafka: {e}")

def main():
    """Función principal que envía datos a Kafka cada minuto."""
    print("Iniciando productor de Kafka para datos del clima...")
    while True:
        weather_data = get_weather_data()
        if weather_data:
            formatted_data = {
                'timestamp': time.time(),
                'location': {'lat': LAT, 'lon': LON},
                'temperature': weather_data['main']['temp'],
                'humidity': weather_data['main']['humidity'],
                'weather': weather_data['weather'][0]['description']
            }
            send_to_kafka(formatted_data, WEATHER_TOPIC)
        time.sleep(60)

if __name__ == '__main__':
    main()
