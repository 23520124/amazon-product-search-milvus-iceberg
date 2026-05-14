from pymilvus import connections, Collection

print("Đang kết nối tới Milvus...")
connections.connect("default", host="milvus-standalone", port="19530")

col = Collection("amazon_beauty_gold")

print("Đang ép Milvus xả toàn bộ dữ liệu tạm thời xuống ổ cứng (MinIO)...")
col.flush()

print("✅ Đã xả xong")