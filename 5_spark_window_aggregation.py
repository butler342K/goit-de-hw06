import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, round as spark_round, to_timestamp, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from configs import kafka_config

# Пакет для читання з Kafka (версія має відповідати версії PySpark)
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'
)

# Назва топіку, в який пише 2_producer.py
my_name = "viper"
sensors_topic = f'{my_name}_building_sensors'

# Параметри вікна
WINDOW_DURATION = "1 minute"
SLIDE_DURATION = "30 seconds"
WATERMARK_DELAY = "10 seconds"

# Файл з умовами алертів
ALERTS_CONDITIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts_conditions.csv")
NOT_USED = -999

spark = (SparkSession.builder
         .appName("BuildingSensorsWindowAggregation")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")  # як у timestamp від продюсера
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Схема JSON-повідомлень від датчиків
sensor_schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
])

jaas_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
)

# Зчитування потоку з Kafka
raw_df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", ",".join(kafka_config['bootstrap_servers']))
          .option("kafka.security.protocol", kafka_config['security_protocol'])
          .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
          .option("kafka.sasl.jaas.config", jaas_config)
          .option("subscribe", sensors_topic)
          .option("startingOffsets", "earliest")
          .option("maxOffsetsPerTrigger", "500")
          .option("failOnDataLoss", "false")
          .load())

# Розбір JSON та перетворення часу вимірювання в timestamp (event time)
sensors_df = (raw_df
              .select(from_json(col("value").cast("string"), sensor_schema).alias("data"))
              .select("data.*")
              .withColumn("timestamp", to_timestamp(col("timestamp")))
              .dropna(subset=["timestamp", "temperature", "humidity"]))

# Sliding window 1 хв з кроком 30 с, watermark 10 с
avg_df = (sensors_df
          .withWatermark("timestamp", WATERMARK_DELAY)
          .groupBy(window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION))
          .agg(spark_round(avg("temperature"), 2).alias("t_avg"),
               spark_round(avg("humidity"), 2).alias("h_avg"))
          .select(col("window.start").alias("window_start"),
                  col("window.end").alias("window_end"),
                  "t_avg", "h_avg"))

# Умови алертів (статичний DataFrame). -999 означає, що межа не використовується
conditions_schema = StructType([
    StructField("id", IntegerType()),
    StructField("humidity_min", DoubleType()),
    StructField("humidity_max", DoubleType()),
    StructField("temperature_min", DoubleType()),
    StructField("temperature_max", DoubleType()),
    StructField("code", IntegerType()),
    StructField("message", StringType()),
])
conditions_df = spark.read.csv(ALERTS_CONDITIONS_FILE, header=True, schema=conditions_schema)


def in_range(value_col, min_col, max_col):
    """Значення в межах (min, max); межа зі значенням -999 ігнорується."""
    return (((col(min_col) == NOT_USED) | (col(value_col) > col(min_col))) &
            ((col(max_col) == NOT_USED) | (col(value_col) < col(max_col))))


# Cross join середніх значень з усіма умовами + фільтрація тих, що спрацювали
alerts_df = (avg_df
             .crossJoin(conditions_df)
             .filter(in_range("h_avg", "humidity_min", "humidity_max") &
                     in_range("t_avg", "temperature_min", "temperature_max"))
             .select("window_start", "window_end", "t_avg", "h_avg", "code", "message"))

# Виведення алертів у консоль
query = (alerts_df.writeStream
         .outputMode("update")
         .format("console")
         .option("truncate", "false")
         .trigger(processingTime="10 seconds")
         .start())

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopped.")
    query.stop()
    spark.stop()
