# Data pipeline Python-Kafka-Mongodb
´´´sh
docker-compose down
´´´
´´´sh
docker-compose up -d
´´´
´´´sh
docker exec -it kafka bash
´´´
´´´sh
kafka-topics --bootstrap-server localhost:9092 --create --topic openWeather
kafka-topics --bootstrap-server localhost:9092 --create --topic nasa
exit
´´´
Después de eso.
Ejecutar producer y consumer intentar hacer que jale.