import time
import json
import requests
from kafka import KafkaProducer


# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'  # Cambia esto según la configuración de tu broker
WEATHER_TOPIC = 'openWeather'
NASA_WEATHER_TOPIC = 'nasa'

# Configuración de OpenWeatherMap
API_KEY = 'f9c88108f60190a68fb9d3bc156cfeba' # API de stariluz, saca la tuya
LAT = 28.704218708714663
LON = -106.1394034006503
URL = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"

# Configuración de API de la NASA
API_KEY_NASA = 'pwREnD4cT4z5l6iccBP2dokQocPxFvEFhLDMMYaD' # API de la API de la NASA
URL_NASA = f"https://api.nasa.gov/insight_weather/?api_key={API_KEY_NASA}&feedtype=json&ver=1.0"

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
        data=response.json()
        return  {
            'timestamp': time.time(),
            'location': {'lat': LAT, 'lon': LON},
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'weather': data['weather'][0]['description']
        }
    except requests.RequestException as e:
        print(f"Error al obtener los datos del clima: {e}")
        return None

def get_mars_weather_data():
    """Obtiene los datos del clima de Marte desde la API de NASA."""
    try:
        response = requests.get(URL_NASA)
        response.raise_for_status()
        data = response.json()

        sol_keys = data.get("sol_keys", [])
        
        if sol_keys:
            first_sol = sol_keys[0]
            mars_weather_data = data.get(first_sol, {})

            formatted_data = {
                'timestamp': time.time(),
                'mars_sol': first_sol,
                'temperature': mars_weather_data.get('AT', {}).get('av', None),
                'pressure': mars_weather_data.get('PRE', {}).get('av', None),
                'wind_speed': mars_weather_data.get('WD', {}).get('most_common', {}).get('sp', None),
                'season': data.get('season', '')
            }
            return formatted_data
        else:
            print("La API de la NASA no contempla datos para el sol especificado.")
            return None
    except requests.RequestException as e:
        print(f"Error al obtener los datos del clima de Marte: {e}")
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
            send_to_kafka(weather_data, WEATHER_TOPIC)
        
        mars_weather_data = get_mars_weather_data()
        if mars_weather_data:
            send_to_kafka(mars_weather_data, NASA_WEATHER_TOPIC)
        time.sleep(20)

if __name__ == '__main__':
    main()
