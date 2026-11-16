import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, col, current_timestamp, date_format, from_json, struct,
                                   to_json, to_timestamp, when, window)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from configs import kafka_config

# Пакет для роботи з Kafka (версія має відповідати версії PySpark)
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

my_name = "viper"
sensors_topic = f'{my_name}_building_sensors'
humidity_alerts_topic = f'{my_name}_humidity_alerts'
temperature_alerts_topic = f'{my_name}_temperature_alerts'

kafka_options = {
    "kafka.bootstrap.servers": ",".join(kafka_config['bootstrap_servers']),
    "kafka.security.protocol": kafka_config['security_protocol'],
    "kafka.sasl.mechanism": kafka_config['sasl_mechanism'],
    "kafka.sasl.jaas.config": (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    ),
}

spark = (SparkSession.builder
         .appName("BuildingSensorsAlerts")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")  # як у timestamp від продюсера
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------------------------
# 1. Генерація потоку даних: зчитуємо дані датчиків з Kafka (їх пише 2_producer.py)
# ---------------------------------------------------------------------------
sensor_schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
])

sensors_df = (spark.readStream
              .format("kafka")
              .options(**kafka_options)
              .option("subscribe", sensors_topic)
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", "500")
              .option("failOnDataLoss", "false")
              .load()
              .select(from_json(col("value").cast("string"), sensor_schema).alias("data"))
              .select("data.*")
              .withColumn("timestamp", to_timestamp(col("timestamp")))
              .dropna(subset=["timestamp", "temperature", "humidity"]))


# ---------------------------------------------------------------------------
# 2. Агрегація даних: sliding window 1 хв, крок 30 с, watermark 10 с
# ---------------------------------------------------------------------------
avg_df = (sensors_df
          .withWatermark("timestamp", "10 seconds")
          .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
          .agg(avg("temperature").alias("t_avg"),
               avg("humidity").alias("h_avg")))


# ---------------------------------------------------------------------------
# 3. Параметри алертів: зчитуємо alerts_conditions.csv (-999 — межа не використовується)
# ---------------------------------------------------------------------------
NOT_USED = -999

conditions_schema = StructType([
    StructField("id", IntegerType()),
    StructField("humidity_min", DoubleType()),
    StructField("humidity_max", DoubleType()),
    StructField("temperature_min", DoubleType()),
    StructField("temperature_max", DoubleType()),
    StructField("code", IntegerType()),
    StructField("message", StringType()),
])
conditions_df = spark.read.csv(os.path.join(BASE_DIR, "alerts_conditions.csv"),
                               header=True, schema=conditions_schema)


# ---------------------------------------------------------------------------
# 4. Побудова визначення алертів: cross join + фільтрація
# ---------------------------------------------------------------------------
def in_range(value_col, min_col, max_col):
    """Значення в межах (min, max); межа зі значенням -999 ігнорується."""
    return (((col(min_col) == NOT_USED) | (col(value_col) > col(min_col))) &
            ((col(max_col) == NOT_USED) | (col(value_col) < col(max_col))))


alerts_df = (avg_df
             .crossJoin(conditions_df)
             .filter(in_range("h_avg", "humidity_min", "humidity_max") &
                     in_range("t_avg", "temperature_min", "temperature_max"))
             .select("window", "t_avg", "h_avg",
                     col("code").cast("string").alias("code"), "message")
             .withColumn("timestamp", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS")))


# ---------------------------------------------------------------------------
# 5. Запис даних у Kafka-топік: вологість (101, 102) -> humidity_alerts,
#    температура (103, 104) -> temperature_alerts
# ---------------------------------------------------------------------------
kafka_df = alerts_df.select(
    col("code").alias("key"),
    to_json(struct("window", "t_avg", "h_avg", "code", "message", "timestamp")).alias("value"),
    when(col("code").isin("101", "102"), humidity_alerts_topic)
    .otherwise(temperature_alerts_topic).alias("topic"),
)

# append: кожне вікно видається один раз — після того, як watermark його закриє
query = (kafka_df.writeStream
         .outputMode("append")
         .format("kafka")
         .options(**kafka_options)
         .option("checkpointLocation", os.path.join(BASE_DIR, "checkpoints", "alerts_to_kafka"))
         .trigger(processingTime="10 seconds")
         .start())

print(f"Writing alerts to '{humidity_alerts_topic}' and '{temperature_alerts_topic}' (Ctrl+C to stop)...")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopped.")
    query.stop()
    spark.stop()
