# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer

print("--- KHỞI TẠO TẦNG TÍNH TOÁN SPARK AI PIPELINE (SHADED CLEAN CATALOG) ---")

# Khởi tạo Spark Session sử dụng cấu hình Hadoop Catalog sạch, đồng nhất với Phase 1
spark = SparkSession.builder \
  .appName("IcebergToMilvus_AI_Pipeline") \
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

# Nạp mô hình Sentence Transformers từ thư mục lưu trữ Offline an toàn trên máy thật
print("--- ĐANG NẠP MÔ HÌNH AI OFFLINE TỪ LƯU TRỮ LOCAL ---")
model = SentenceTransformer('/home/iceberg/data/all-MiniLM-L6-v2')

print("--- ĐỌC DỮ LIỆU TỪ BẢNG ICEBERG HADOOP ---")
df_reviews = spark.table("local.amazon.reviews").limit(5000)

print("--- TIẾN HÀNH TÍNH TOÁN SONG SONG VECTOR EMBEDDINGS (384 DIM) ---")
def compute_embedding(text_content):
    if text_content is None:
        return [0.0] * 384
    return model.encode(text_content).tolist()

embedding_udf = udf(compute_embedding, ArrayType(FloatType()))
df_vectorized = df_reviews.withColumn("vector", embedding_udf(col("text")))

print("--- GHI ĐỒNG THỜI VÀO CƠ SỞ DỮ LIỆU VECTOR MILVUS ---")
df_vectorized.write.format("milvus") \
  .option("milvus.host", "milvus-standalone") \
  .option("milvus.port", "19530") \
  .option("milvus.user", "root") \
  .option("milvus.password", "Milvus") \
  .option("milvus.collection.name", "amazon_reviews_ai") \
  .option("milvus.collection.vectorField", "vector") \
  .option("milvus.collection.vectorDim", "384") \
  .option("milvus.collection.primaryKeyField", "user_id") \
  .mode("append") \
  .save()

print("--- TRẠNG THÁI: THÀNH CÔNG RỰC RỠ! HÃY MỞ ATTU ĐỂ KIỂM TRA ---")
spark.stop()