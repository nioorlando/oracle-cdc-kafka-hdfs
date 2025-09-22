from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, date_format
from pyspark.sql.types import *

# ==== CONFIGURATION ====
KAFKA_BS  = "kafka-broker1:9093"               # ganti dengan alamat broker Kafka
TRUST_JKS = "/path/to/kafka-truststore.jks"    # truststore untuk TLS
TOPIC     = "ora_cdc_demo"                     # nama topik CDC
OUT_DIR   = "hdfs://namenode:8020/cdc/output"  # path HDFS output
CHK_DIR   = "hdfs://namenode:8020/checkpoints/cdc_kafka_to_hdfs"
CONSUMER_GROUP = "spark-consumer-cdc"          # nama consumer group
# ==============================================

spark = (SparkSession.builder
         .appName("cdc_consumer_kafka_to_hdfs_partitioned")
         .getOrCreate())

# Skema payload (contoh sederhana, sesuaikan dengan CDC sebenarnya)
payload_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True)
])

# Baca stream dari Kafka
df_kafka = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BS)
    .option("subscribe", TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "GSSAPI")
    .option("kafka.sasl.kerberos.service.name", "kafka")
    .option("kafka.ssl.truststore.location", TRUST_JKS)
    .option("kafka.ssl.truststore.password", "changeit")  # placeholder
    .option("startingOffsets", "earliest")   # mulai konsumsi dari awal
    .option("kafka.group.id", CONSUMER_GROUP)
    .load()
)

# Ambil raw value (string)
df_raw = df_kafka.selectExpr("CAST(value AS STRING) as raw_value", "timestamp")

# Parse JSON sesuai schema Debezium (schema + payload)
df_parsed = (df_raw
    .withColumn("json_data", from_json(col("raw_value"), 
                                       StructType([
                                           StructField("schema", StringType(), True),
                                           StructField("payload", payload_schema, True)
                                       ])))
    .select(
        col("json_data.payload.*"),
        col("raw_value"),   # simpan juga raw_value untuk debug
        col("timestamp")
    )
)

# Tambahkan kolom partisi (tanggal & jam)
df_partitioned = (df_parsed
    .withColumn("dt", to_date(col("timestamp")))
    .withColumn("hour", date_format(col("timestamp"), "HH"))
)

# Tulis stream ke HDFS dengan partisi
q = (df_partitioned.writeStream
     .format("parquet")  # bisa diganti "json" kalau mau file JSON
     .option("path", OUT_DIR)
     .option("checkpointLocation", CHK_DIR)
     .outputMode("append")
     .partitionBy("dt", "hour")
     .start())

q.awaitTermination()
