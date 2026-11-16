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
    key_deserializer=lambda v: v.decode('utf-8') if v else None,
    auto_offset_reset='earliest',       # Зчитування повідомлень з початку
    enable_auto_commit=False,           # Без збереження оффсетів:
    group_id=None,                      # кожен запуск показує всі алерти з початку топіків
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
        window = alert.get('window')
        if not window:
            continue  # старі повідомлення з hw-05 (інший формат) пропускаємо
        print(
            f"[{labels.get(msg.topic, msg.topic)} ALERT {alert.get('code')}] "
            f"window={window.get('start')} .. {window.get('end')} | "
            f"t_avg={alert.get('t_avg', 0):.2f}°C, h_avg={alert.get('h_avg', 0):.2f}% | "
            f"{alert.get('message')} | created={alert.get('timestamp')}"
        )
except KeyboardInterrupt:
    print("\nStopped.")
finally:
    consumer.close()  # Закриття consumer
