import sys
from time import sleep

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def start_streaming(spark):
    try:
        print("Starting stream", file=sys.stderr)
        stream_df = (spark.readStream.format("socket")
                     .option("host", "localhost")
                     .option("port", 9999)
                     .load())

        print("Stream started, defining schema", file=sys.stderr)
        schema = StructType([
            StructField("review_id", StringType()),
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("stars", FloatType()),
            StructField("date", StringType()),
            StructField("text", StringType())
        ])

        print("Selecting data from stream", file=sys.stderr)
        stream_def = stream_df.select(from_json(col('value'), schema).alias("data")).select("data.*")

        stream_def.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()

        print("Starting query", file=sys.stderr)
        query = stream_def.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()

        query.awaitTermination()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)

if __name__ == "__main__":
    print("Creating Spark session", file=sys.stderr)
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    print("Spark session created", file=sys.stderr)

    start_streaming(spark_conn)
