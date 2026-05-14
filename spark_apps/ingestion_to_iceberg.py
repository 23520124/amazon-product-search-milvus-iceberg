from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, element_at 

# KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("AmazonBeautyETL") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.sql.shuffle.partitions", "8") \
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

# ĐƯỜNG DẪN DỮ LIỆU ALL_BEAUTY
reviews_path = "file:///home/iceberg/data/raw/All_Beauty.jsonl.gz"
meta_path = "file:///home/iceberg/data/raw/meta_All_Beauty.jsonl.gz"

print("Đang đọc dữ liệu Amazon Beauty...")
df_reviews = spark.read.json(reviews_path)
df_meta = spark.read.json(meta_path)

# LÀM SẠCH VÀ TRÍCH XUẤT THÔNG MINH
print("Đang làm sạch và trích xuất siêu dữ liệu...")

# Bảng Review: Chỉ lấy những dòng có text
clean_reviews = df_reviews.select(
    col("user_id"), 
    col("parent_asin").alias("item_id"), 
    col("rating"), 
    col("text"), 
    col("timestamp")
).filter("text IS NOT NULL AND text != ''")

# Bảng Meta: Lấy thông tin cơ bản và bóc tách các trường quan trọng từ struct 'details'
clean_meta = df_meta.select(
    col("parent_asin").alias("item_id"), 
    col("title"), 
    col("store").alias("brand"), 
    col("price"),
    col("details.`Skin Type`").alias("skin_type"),
    col("details.`Item Form`").alias("item_form"),
    col("details.`Scent`").alias("scent"),
    element_at(col("images"), 1).getItem("large").alias("image_url")
).dropDuplicates(["item_id"])

# Xử lý các giá trị null bằng các chuỗi có ý nghĩa để AI dễ hiểu
clean_meta = clean_meta.fillna({
    "skin_type": "Mọi loại da", 
    "item_form": "Chưa xác định", 
    "scent": "Không mùi",
    "image_url": "https://via.placeholder.com/300x300.png?text=No+Image"
})

# JOIN TẠO BẢNG SILVER VÀ BƠM "KIẾN THỨC" CHO AI
print("Đang gộp dữ liệu Review và Metadata...")
silver_df = clean_reviews.join(clean_meta, on="item_id", how="left")

# Gộp thành 1 cột "text_for_ai" 
silver_df = silver_df.withColumn(
    "text_for_ai", 
    concat_ws(". ", 
        col("title"), 
        concat_ws(": ", lit("Phù hợp cho da"), col("skin_type")),
        concat_ws(": ", lit("Dạng"), col("item_form")),
        col("text") 
    )
)

# GHI VÀO ICEBERG LAKEHOUSE
print("Đang ghi bảng All Beauty vào Iceberg...")
spark.sql("CREATE DATABASE IF NOT EXISTS local.amazon")

silver_df.writeTo("local.amazon.all_beauty_silver") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

print("✅ đã hoàn tất và sẵn sàng cho Milvus.")
spark.stop()