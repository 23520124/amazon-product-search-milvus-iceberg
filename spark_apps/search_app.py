# -*- coding: utf-8 -*-
import time
from pymilvus import MilvusClient
from sentence_transformers import SentenceTransformer

# 1. Kết nối tới Milvus Server
client = MilvusClient(uri="http://milvus-standalone:19530", token="root:Milvus")

client.load_collection(collection_name="amazon_reviews_ai")

# 2. Nạp mô hình AI Offline
model = SentenceTransformer('/home/iceberg/data/all-MiniLM-L6-v2')

def semantic_search(query_text, top_k=5):
    print(f"🔍 Truy vấn: '{query_text}'")
    
    # Bước 1: Sinh vector cho câu truy vấn
    start_embed = time.time()
    query_vector = model.encode([query_text])[0].tolist()
    embed_time = (time.time() - start_embed) * 1000

    # Bước 2: Thiết lập tham số tìm kiếm (Phải khớp COSINE với lúc tạo Index)
    search_params = {
        "metric_type": "COSINE", 
        "params": {"ef": 64}  # Tham số mở rộng vùng tìm kiếm (có thể tinh chỉnh sau)
    }
    
    start_search = time.time()
    # Thực hiện tìm kiếm qua Milvus V2 API
    results = client.search(
        collection_name="amazon_reviews_ai",
        data=[query_vector],
        limit=top_k,
        output_fields=["item_id", "rating", "text"], # Lấy 3 trường như yêu cầu
        search_params=search_params
    )
    search_time = (time.time() - start_search) * 1000

    # Bước 3: Xử lý và in kết quả
    print(f"⏱️ Vectorize: {embed_time:.2f} ms | Vector Search: {search_time:.2f} ms")
    print("-" * 65)
    
    # results[0] vì chúng ta chỉ gửi 1 query_vector
    if not results or not results[0]:
        print("Không tìm thấy kết quả phù hợp.")
        return

    for idx, hit in enumerate(results[0]):
        # Bóc tách Dictionary theo chuẩn V2
        distance = hit.get("distance", 0.0)
        entity = hit.get("entity", {})
        
        item_id = entity.get("item_id", "N/A")
        rating = entity.get("rating", "N/A")
        text = entity.get("text", "N/A")
        
        # Cắt chuỗi để terminal không bị trôi quá nhanh
        short_text = text[:150] + "..." if len(text) > 150 else text
        
        print(f"Top {idx + 1} | Độ tương đồng (Cosine Score): {distance:.4f}")
        print(f"📦 Item ID: {item_id} | ⭐ Rating: {rating}")
        print(f"💬 Review: {short_text}")
        print("-" * 65)

if __name__ == "__main__":
    
    # 1. Các câu truy vấn thử nghiệm cho tập All_Beauty
    test_queries = [
        "good quality perfume with a great scent", 
        "hair care product with nice smell",
        "bad quality, too small"
    ]
    
    # 2. Thực thi tìm kiếm
    for q in test_queries:
        semantic_search(query_text=q, top_k=3)
        print("\n")