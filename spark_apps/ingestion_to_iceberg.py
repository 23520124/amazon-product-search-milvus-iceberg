from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Khởi tạo SparkSession dùng Hadoop Catalog (Lưu trực tiếp lên MinIO)
spark = SparkSession.builder \
  .appName("AmazonProductETL") \
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.local.type", "hadoop") \
  .config("spark.sql.catalog.local.warehouse", "s3a://amazon-lakehouse/warehouse") \
  .config("spark.sql.defaultCatalog", "local") \
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
  .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
  .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config("spark.sql.caseSensitive", "true") \
  .getOrCreate()

# 2. Đường dẫn dữ liệu (Sửa đuôi.jsonl.gz như bạn đã ls thấy)
reviews_path = "/home/iceberg/data/raw/All_Beauty.jsonl.gz"
meta_path = "/home/iceberg/data/raw/meta_All_Beauty.jsonl.gz"

print("Đang đọc dữ liệu Amazon...")
df_reviews = spark.read.json(reviews_path)
df_meta = spark.read.json(meta_path)

# 3. Làm sạch dữ liệu
cleaned_reviews = df_reviews.select(
    col("user_id"), 
    col("parent_asin").alias("item_id"), 
    col("rating"), 
    col("text"), # Bản 2023 dùng 'text'
    col("timestamp")
).filter("text IS NOT NULL")

cleaned_meta = df_meta.select(
    col("parent_asin").alias("item_id"), 
    col("title"), 
    col("store").alias("brand"), # Lấy store đổi tên thành brand cho dễ hiểu
    col("price")
).dropDuplicates(["item_id"])

# 4. Ghi vào Lakehouse
print("Đang ghi vào Iceberg Lakehouse (Hadoop Catalog)...")
spark.sql("CREATE DATABASE IF NOT EXISTS local.amazon")
cleaned_reviews.writeTo("local.amazon.reviews").createOrReplace()
cleaned_meta.writeTo("local.amazon.metadata").createOrReplace()

print("Chúc mừng! Dữ liệu đã nạp thành công vào Iceberg.")
spark.stop()