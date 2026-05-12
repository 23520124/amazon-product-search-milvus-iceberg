from huggingface_hub import snapshot_download

print("Đang tự động tải trọn gói mô hình và cấu trúc thư mục...")
snapshot_download(
    repo_id="sentence-transformers/all-MiniLM-L6-v2",
    local_dir=r"C:\Users\LENOVO\amazon-product-search-milvus-iceberg\data\all-MiniLM-L6-v2",
    local_dir_use_symlinks=False
)
print("Thành công! Toàn bộ file cấu hình và thư mục con đã được tải về.")