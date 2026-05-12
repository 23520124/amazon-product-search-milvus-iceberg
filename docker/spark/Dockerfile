FROM tabulario/spark-iceberg:latest

# Chuyển sang quyền root để cài đặt thư viện
USER root

# Cài đặt các thư viện AI và Milvus cần thiết
# Chúng ta cài đặt trực tiếp mà không cần đổi WORKDIR
RUN pip install --no-cache-dir \
    huggingface-hub==0.25.2 \
    sentence-transformers==2.2.2 \
    pymilvus==2.6.11 \
    pandas \
    streamlit

