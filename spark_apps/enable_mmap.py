from pymilvus import connections, Collection

print("Đang kết nối tới Milvus...")
connections.connect("default", host="milvus-standalone", port="19530")

col = Collection("amazon_beauty_gold")

print("Release RAM để cấu hình lại...")
col.release()

print("Đang kích hoạt Mmap (Memory-Mapping)...")
col.set_properties({"mmap.enabled": True})

print("Đang Load lại Collection với Mmap...")
col.load()

print("✅ THÀNH CÔNG!")