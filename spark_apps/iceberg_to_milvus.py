# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer

print("--- KHỞI TẠO TẦNG TÍNH TOÁN SPARK AI PIPELINE (SHADED CLEAN CATALOG) ---")

spark = SparkSession.builder \
  .appName("IcebergToMilvus_AI_Pipeline") \
  .config("spark.driver.memory", "3g") \
  .config("spark.executor.memory", "2g") \
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.local.type", "hadoop") \
  .config("spark.sql.catalog.local.warehouse", "s3a://amazon-lakehouse/warehouse") \
  .config("spark.sql.defaultCatalog", "local") \
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
  .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
  .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
  .config("spark.sql.catalog.local.s3.path-style-access", "true") \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config("spark.sql.caseSensitive", "true") \
  .getOrCreate()

print("--- ĐANG NẠP MÔ HÌNH AI OFFLINE TỪ LƯU TRỮ LOCAL ---")
model = SentenceTransformer('/home/iceberg/data/all-MiniLM-L6-v2')

print("--- ĐỌC DỮ LIỆU TỪ TẦNG SILVER ICEBERG ---")
df_silver = spark.table("local.amazon.all_beauty_silver")

df_subset = df_silver

print("--- TIẾN HÀNH TÍNH TOÁN SONG SONG VECTOR EMBEDDINGS (384 DIM) ---")
def compute_embedding(text_content):
    if text_content is None:
        return [0.0] * 384
    return model.encode(text_content).tolist()

embedding_udf = udf(compute_embedding, ArrayType(FloatType()))

df_vectorized = df_subset.withColumn("vector", embedding_udf(col("text_for_ai"))) \
                         .withColumn("id", monotonically_increasing_id())

# Chỉ chọn ra những cột cần thiết mang vào tầng gold
df_final_gold = df_vectorized.select(
    col("id"), 
    col("item_id"), 
    col("title"), 
    col("brand"), 
    col("price").cast("float"), 
    col("image_url"), 
    col("vector")
).na.fill({
    "item_id": "Unknown",
    "title": "Sản phẩm không có tên",
    "brand": "N/A",
    "price": 0.0,
    "image_url": ""
})

print("--- GHI ĐỒNG THỜI VÀO CƠ SỞ DỮ LIỆU VECTOR MILVUS ---")
df_final_gold.write.format("milvus") \
  .option("milvus.host", "milvus-standalone") \
  .option("milvus.port", "19530") \
  .option("milvus.user", "root") \
  .option("milvus.password", "Milvus") \
  .option("milvus.collection.name", "amazon_beauty_gold") \
  .option("milvus.collection.vectorField", "vector") \
  .option("milvus.collection.vectorDim", "384") \
  .option("milvus.collection.primaryKeyField", "id") \
  .mode("append") \
  .save()

print("--- TRẠNG THÁI: THÀNH CÔNG ---")
spark.stop()