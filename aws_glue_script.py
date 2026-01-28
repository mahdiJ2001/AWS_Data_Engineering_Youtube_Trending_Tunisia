import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, BooleanType
)
from awsglue.context import GlueContext

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Explicit schema for raw CSV
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("title", StringType(), True),
    StructField("localized_title", StringType(), True),
    StructField("channel_title", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_name", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("tag_count", LongType(), True),
    StructField("thumbnail", StringType(), True),
    StructField("default_language", StringType(), True),
    StructField("audio_language", StringType(), True),
    StructField("is_live", BooleanType(), True),
    StructField("duration", LongType(), True),
    StructField("duration_raw", StringType(), True),
    StructField("definition", StringType(), True),
    StructField("caption_available", BooleanType(), True),
    StructField("licensed_content", BooleanType(), True),
    StructField("embeddable", BooleanType(), True),
    StructField("made_for_kids", BooleanType(), True),
    StructField("views", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("comments", LongType(), True),
    StructField("ingestion_date", StringType(), True)
])

# Read raw CSV
df = spark.read.option("header", "true").schema(schema).csv(
    "s3://youtube-datalake-mj-2026/raw/ingestion_date=2026-01-27/"
)

# Type conversions
df = df.withColumn("fetched_at", to_timestamp(col("fetched_at")))
df = df.withColumn("published_at", to_timestamp(col("published_at")))

# Keep only curated columns
columns_to_keep = [
    "video_id", "channel_id", "fetched_at", "published_at",
    "title", "category_id", "category_name",
    "country", "default_language", "views", "likes", "comments",
    "ingestion_date"
]
df = df.select(columns_to_keep)

# Write to curated Parquet partitioned by country + ingestion_date
df.write.mode("overwrite").partitionBy("country", "ingestion_date").parquet(
    "s3://youtube-datalake-mj-2026/curated/videos/"
)

print("Curated ETL job completed successfully!")