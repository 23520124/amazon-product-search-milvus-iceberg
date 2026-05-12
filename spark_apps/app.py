import time
import streamlit as st
from pymilvus import MilvusClient
from sentence_transformers import SentenceTransformer

st.title("🛍️ Amazon Smart Search")

# 1. Khởi tạo Backend (Dùng Cache để AI model không bị load lại mỗi lần bạn gõ phím)
@st.cache_resource
def load_backend():
    # Kết nối Milvus
    client = MilvusClient(uri="http://milvus-standalone:19530", token="root:Milvus")
    
    # Bắt buộc nạp dữ liệu lên RAM để search không bị văng lỗi
    client.load_collection("amazon_reviews_ai")
    
    # Load Model Offline
    model = SentenceTransformer('/home/iceberg/data/all-MiniLM-L6-v2')
    
    return client, model

try:
    client, model = load_backend()
except Exception as e:
    st.error(f"❌ Lỗi khởi tạo hệ thống: {e}")
    st.stop()

# 2. Tạo thanh tìm kiếm UI
query = st.text_input("Bạn muốn tìm sản phẩm gì hôm nay?", placeholder="VD: good quality perfume with great scent...")

# 3. Nút bấm thực thi
if st.button("Tìm kiếm"):
    if query:
        with st.spinner('AI đang tính toán vector và quét dữ liệu...'):
            start_time = time.time()
            
            # Biến văn bản truy vấn thành vector
            query_vector = model.encode(query).tolist()
            
            # Thực hiện ANN Search trên Milvus
            raw_results = client.search(
                collection_name="amazon_reviews_ai",
                data=[query_vector],
                limit=3,
                output_fields=["item_id", "text", "rating"] 
            )
            
            search_time = (time.time() - start_time) * 1000
            st.success(f"✅ Hoàn tất trong {search_time:.2f} ms!")
            
            # Trích xuất mảng kết quả của câu hỏi đầu tiên
            results = raw_results[0]
            
            # 4. Hiển thị kết quả ra thẻ (Card) trực quan
            for hit in results:
                entity = hit['entity']
                distance = hit['distance']
                
                st.subheader(f"📦 Mã SP: {entity.get('item_id')}")
                st.write(f"⭐ **Đánh giá:** {entity.get('rating')} sao")
                st.write(f"📈 **Độ tương đồng:** {distance:.4f}")
                st.info(f"💬 **Review:** {entity.get('text')}")
                st.divider() # Dòng kẻ ngang phân cách