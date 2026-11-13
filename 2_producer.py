from kafka import KafkaProducer
from configs import kafka_config
from datetime import datetime, timezone
import json
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "viper"
topic_name = f'{my_name}_building_sensors'

# ID датчика: випадковий, але постійний протягом одного запуску скрипту
sensor_id = random.randint(1000, 9999)
send_interval = 2  # секунд між вимірюваннями

print(f"Sensor {sensor_id} started. Sending data to '{topic_name}' (Ctrl+C to stop)...")

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),  # Час отримання даних
            "temperature": round(random.uniform(25, 45), 2),     # 25..45 °C
            "humidity": round(random.uniform(15, 85), 2),        # 15..85 %
        }
        try:
            # Ключ = ID датчика, щоб усі дані одного датчика потрапляли в одну партицію
            producer.send(topic_name, key=sensor_id, value=data)
            producer.flush()
            print(f"Sent: {data}")
        except Exception as e:
            print(f"An error occurred: {e}")
        time.sleep(send_interval)
except KeyboardInterrupt:
    print(f"\nSensor {sensor_id} stopped.")
finally:
    producer.close()  # Закриття producer
