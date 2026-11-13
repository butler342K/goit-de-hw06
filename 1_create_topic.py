from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення топіків
my_name = "viper"
num_partitions = 2
replication_factor = 1

topic_names = [
    f'{my_name}_building_sensors',     # дані з усіх датчиків
    f'{my_name}_temperature_alerts',   # сповіщення про перевищення температури
    f'{my_name}_humidity_alerts',      # сповіщення про вихід вологості за межі
]

# Створення топіків (кожен окремо, щоб вже існуючий не блокував інші)
for topic_name in topic_names:
    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"An error occurred while creating '{topic_name}': {e}")

# Перевіряємо список існуючих топіків (лише наші)
print("\nMy topics:")
for topic in sorted(admin_client.list_topics()):
    if my_name in topic:
        print(f"  {topic}")

# Закриття зв'язку з клієнтом
admin_client.close()
