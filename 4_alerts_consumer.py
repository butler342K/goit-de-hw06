from kafka import KafkaConsumer
from configs import kafka_config
import json

# Назви топіків
my_name = "viper"
temperature_alerts_topic = f'{my_name}_temperature_alerts'
humidity_alerts_topic = f'{my_name}_humidity_alerts'

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',       # Зчитування повідомлень з початку
    enable_auto_commit=True,            # Автоматичне підтвердження зчитаних повідомлень
    group_id=f'{my_name}_alerts_viewer'
)

# Підписка на обидва топіки сповіщень
consumer.subscribe([temperature_alerts_topic, humidity_alerts_topic])
print(f"Subscribed to '{temperature_alerts_topic}' and '{humidity_alerts_topic}' (Ctrl+C to stop)...")

labels = {
    temperature_alerts_topic: "TEMPERATURE",
    humidity_alerts_topic: "HUMIDITY",
}

try:
    for msg in consumer:
        alert = msg.value
        print(
            f"[{labels.get(msg.topic, msg.topic)} ALERT] "
            f"sensor={alert.get('sensor_id')} | "
            f"time={alert.get('timestamp')} | "
            f"t={alert.get('temperature')}°C, h={alert.get('humidity')}% | "
            f"{alert.get('message')}"
        )
except KeyboardInterrupt:
    print("\nStopped.")
finally:
    consumer.close()  # Закриття consumer
