import time
import streamlit as st
from pymilvus import MilvusClient
from sentence_transformers import SentenceTransformer

st.set_page_config(page_title="Luxe Beauty AI Search", layout="wide")
st.title("🧴 Luxe Beauty: AI Smart Search")
st.markdown("---")

# 1. Khởi tạo Backend 
@st.cache_resource
def load_backend():
    # Kết nối Milvus 
    client = MilvusClient(uri="http://milvus-standalone:19530", token="root:Milvus")
    
    # Nạp dữ liệu lên RAM 
    client.load_collection("amazon_beauty_gold")
    
    # Load Model Offline
    model = SentenceTransformer('/home/iceberg/data/all-MiniLM-L6-v2')
    
    return client, model

try:
    client, model = load_backend()
except Exception as e:
    st.error(f"❌ Lỗi khởi tạo hệ thống: {e}")
    st.stop()

# 2. Thanh tìm kiếm UI và Sidebar thông tin
with st.sidebar:
    st.header("Thông tin hệ thống")
    st.info("Dữ liệu: All Beauty (~430k dòng)")
    st.write("Model: `all-MiniLM-L6-v2`")
    st.write("Database: Iceberg + Milvus")

query = st.text_input("✨ Bạn đang cần tìm loại mỹ phẩm nào?", placeholder="VD: moisturizing cream for sensitive skin, charcoal cleanser...")
button_search = st.button("Tìm kiếm")

if query:
    with st.spinner('AI đang phân tích nhu cầu của bạn...'):
        if button_search:
            start_time = time.time()
            
            query_vector = model.encode(query).tolist()
            
            # Lấy dư ra 20 kết quả để dự phòng lọc trùng
            raw_results = client.search(
                collection_name="amazon_beauty_gold",
                data=[query_vector],
                limit=20, 
                output_fields=["item_id", "title", "brand", "price", "image_url"] 
            )
            
            search_time = (time.time() - start_time) * 1000
            st.write(f"⏱️ Tìm thấy trong {search_time:.2f} ms")

            results = raw_results[0]
            
            if not results:
                st.warning("Rất tiếc, AI không tìm thấy sản phẩm phù hợp.")
            else:
                # LỌC TRÙNG (DEDUPLICATION)
                seen_titles = set()
                unique_results = []
                
                for hit in results:
                    title = hit['entity'].get('title', '')
                    # Nếu tên sản phẩm này chưa từng xuất hiện
                    if title not in seen_titles:
                        seen_titles.add(title)
                        unique_results.append(hit)
                    # Đủ 6 món độc bản thì dừng lại
                    if len(unique_results) == 6:
                        break

                # HIỂN THỊ GIAO DIỆN
                cols = st.columns(2)
                
                for idx, hit in enumerate(unique_results): # Duyệt qua danh sách đã lọc
                    entity = hit['entity']
                    dist = hit['distance']
                    
                    with cols[idx % 2]:
                        with st.container(border=True):
                            col_img, col_info = st.columns([1, 2])
                            
                            with col_img:
                                img_url = entity.get('image_url')
                                if img_url and img_url != "":
                                    st.image(img_url, use_container_width=True)
                                else:
                                    st.image("https://via.placeholder.com/150", caption="No Image")
                            
                            with col_info:
                                st.markdown(f"**{entity.get('title')}**")
                                st.write(f"🏷️ **Thương hiệu:** {entity.get('brand')}")
                                price = entity.get('price')
                                if price and price > 0:
                                    st.write(f"💰 **Giá:** ${price:.2f}")
                                else:
                                    st.write("💰 **Giá:** Đang cập nhật")
                                
                                # Chuyển đổi Distance thành Điểm Match % cho người dùng dễ hiểu
                                st.success(f"🎯 **Độ tương đồng:** {dist:.2f}/1")
                                
                                # st.button("Xem chi tiết", key=f"btn_{idx}")