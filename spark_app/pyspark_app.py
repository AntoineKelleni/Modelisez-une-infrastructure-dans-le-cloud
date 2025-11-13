import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    lit,
    count,
)


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "client_tickets")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/data/outputs")

# Création de la SparkSession avec le package Kafka
spark = (
    SparkSession.builder.appName("ClientTicketsStreaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"[SPARK] Lecture du topic {TOPIC_NAME} sur {BOOTSTRAP_SERVERS}")
print(f"[SPARK] Export des résultats vers {OUTPUT_PATH}")

# Schéma du JSON reçu depuis Kafka
ticket_schema = StructType(
    [
        StructField("ticket_id", StringType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("created_at", StringType(), True),
        StructField("request", StringType(), True),
        StructField("request_type", StringType(), True),
        StructField("priority", StringType(), True),
    ]
)

# 1. Lecture du flux Kafka
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)

# 2. Récupération de la valeur et parsing du JSON
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

parsed_df = json_df.select(
    from_json(col("json_value"), ticket_schema).alias("data")
).select("data.*")

# 3. Conversion du champ created_at en timestamp
parsed_df = parsed_df.withColumn(
    "created_at_ts", to_timestamp("created_at")
)

# 4. Ajout d'une équipe de support en fonction du type de demande
enriched_df = parsed_df.withColumn(
    "support_team",
    when(col("request_type") == "incident", lit("Support N1"))
    .when(col("request_type") == "question", lit("Helpdesk"))
    .when(col("request_type") == "demande_evolution", lit("Equipe Produit"))
    .otherwise(lit("Support N2")),
)

# 5. Agrégation : nombre de tickets par type et priorité (pour la console uniquement)
agg_df = enriched_df.groupBy("request_type", "priority").agg(
    count("*").alias("nb_tickets")
)

# 6a. Écriture en streaming vers la console en mode COMPLETE
console_query = (
    agg_df.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", "/tmp/checkpoints_tickets_console")
    .start()
)

# 6b. Écriture des tickets ENRICHIS en parquet en mode APPEND
file_query = (
    enriched_df.writeStream.outputMode("append")
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", "/tmp/checkpoints_tickets_files")
    .start()
)

# Attente de la fin des streams
spark.streams.awaitAnyTermination()
