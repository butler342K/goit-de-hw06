from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
from datetime import datetime, timezone
import json

# Назви топіків
my_name = "viper"
sensors_topic = f'{my_name}_building_sensors'
temperature_alerts_topic = f'{my_name}_temperature_alerts'
humidity_alerts_topic = f'{my_name}_humidity_alerts'

# Порогові значення
TEMPERATURE_MAX = 40   # °C
HUMIDITY_MAX = 80      # %
HUMIDITY_MIN = 20      # %

common_config = dict(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
)

# Kafka Consumer — читає дані датчиків
consumer = KafkaConsumer(
    **common_config,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',       # Зчитування повідомлень з початку
    enable_auto_commit=True,            # Автоматичне підтвердження зчитаних повідомлень
    group_id=f'{my_name}_alerts_processor'
)

# Kafka Producer — відправляє сповіщення
producer = KafkaProducer(
    **common_config,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def build_alert(data, message):
    return {
        "sensor_id": data["sensor_id"],
        "timestamp": data["timestamp"],                                  # час вимірювання
        "alert_timestamp": datetime.now(timezone.utc).isoformat(),      # час створення сповіщення
        "temperature": data["temperature"],
        "humidity": data["humidity"],
        "message": message,
    }


def send_alert(topic, alert):
    producer.send(topic, key=alert["sensor_id"], value=alert)
    print(f"  -> ALERT to '{topic}': {alert['message']}")


consumer.subscribe([sensors_topic])
print(f"Subscribed to '{sensors_topic}'. Watching for alerts (Ctrl+C to stop)...")

try:
    for msg in consumer:
        data = msg.value
        try:
            temperature = data["temperature"]
            humidity = data["humidity"]
        except (KeyError, TypeError):
            print(f"Skipping malformed message: {data}")
            continue

        print(f"Received from sensor {data.get('sensor_id')}: "
              f"t={temperature}°C, h={humidity}% (partition {msg.partition})")

        if temperature > TEMPERATURE_MAX:
            send_alert(temperature_alerts_topic, build_alert(
                data, f"Temperature {temperature}°C exceeds threshold {TEMPERATURE_MAX}°C"))

        if humidity > HUMIDITY_MAX:
            send_alert(humidity_alerts_topic, build_alert(
                data, f"Humidity {humidity}% exceeds upper threshold {HUMIDITY_MAX}%"))
        elif humidity < HUMIDITY_MIN:
            send_alert(humidity_alerts_topic, build_alert(
                data, f"Humidity {humidity}% is below lower threshold {HUMIDITY_MIN}%"))

        producer.flush()
except KeyboardInterrupt:
    print("\nStopped.")
finally:
    consumer.close()
    producer.close()
