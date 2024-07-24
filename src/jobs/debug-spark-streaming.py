from time import sleep
import openai
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize OpenAI API
openai.api_key = os.getenv('OPENAI_API_KEY')

def sentiment_analysis(comment) -> str:
    if comment:
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": comment}
                ]
            )
            sleep(20)
            print(response)
            sleep(20)
            return response.choices[0].message['content']
        except Exception as e:
            return str(e)
    return "Empty"

def start_streaming(spark):
    topic = 'customers_review'
    while True:
        try:
            print("Starting stream...")
            stream_df = (spark.readStream.format("socket")
                         .option("host", "0.0.0.0")
                         .option("port", 9999)
                         .load())

            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select("data.*")
            print("Stream schema applied. Waiting for data...")

            sentiment_analysis_udf = udf(sentiment_analysis, StringType())

            stream_df = stream_df.withColumn('feedback',
                                             when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                             .otherwise(None))
            
            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

            query = (kafka_df.writeStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", os.getenv('KAFKA_BOOTSTRAP_SERVERS'))
                     .option("checkpointLocation", "/tmp/checkpoint")
                     .option("topic", topic)
                     .start()
                     .awaitTermination())
        except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10 seconds')
            sleep(10)

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark_conn)
