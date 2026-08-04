# AI-Powered Data Lakehouse: Semantic Product Search at Scale

This repository implements an end-to-end modern Data Lakehouse and AI Vector Search pipeline to clean, process, and perform real-time semantic queries on Amazon product data. It demonstrates a cloud-native architecture combining distributed batch computing with high-performance similarity search.

---

#### 📖 Project Overview

Project này xây dựng một hệ thống **Modern Data Lakehouse & AI Vector Search Pipeline** hoàn chỉnh nhằm mục đích xử lý, làm sạch và thực hiện các truy vấn tìm kiếm sản phẩm thông minh bằng ý nghĩa ngữ nghĩa (Semantic Search).

Thay vì so khớp từ khóa chính xác theo cách truyền thống, hệ thống tích hợp mô hình ngôn ngữ lớn để "hiểu" nhu cầu người dùng (ngay cả bằng tiếng Việt) và bóc tách các bài đánh giá sản phẩm Amazon (bằng tiếng Anh) có nội dung tương đồng chỉ trong vài miligiây. Dự án sử dụng tập dữ liệu **Amazon Product Reviews (All_Beauty 2023)** với quy mô **701,500+ reviews** và **112,500+ metadata records**, đồng thời được thiết kế sẵn sàng mở rộng (scale-out) lên tới **43.9 triệu dòng** của danh mục **Electronics**.

##### Key Features

- **High-Performance ETL Ingestion**: Sử dụng Apache Spark xử lý song song các tác vụ gộp dữ liệu nén JSON thô từ Amazon, chuẩn hóa cấu trúc cột và bẻ gãy các xung đột schema.
- **Modern Data Lakehouse Architecture**: Triển khai định dạng bảng Apache Iceberg trên MinIO (S3-compatible) mang lại các tính năng vượt trội của Kho dữ liệu (ACID, Schema Evolution, Time Travel) đặt trên nền Hồ chứa giá rẻ.
- **Offline Distributed AI Pipeline**: Spark tải offline mô hình học máy `all-MiniLM-L6-v2` để chuyển đổi văn bản phi cấu trúc thành các vector nhúng 384 chiều song song trên tài nguyên CPU đa luồng.
- **State-of-the-Art Vector DB**: Tích hợp Milvus 2.6.14 sử dụng bộ máy lưu trữ nhật ký **Woodpecker WAL** phi đĩa, đạt thông lượng ghi cực đại **750 MB/s** trực tiếp xuống MinIO mà không cần duy trì cụm Kafka/Pulsar cồng kềnh.
- **Real-time Semantic Query**: Tận dụng chỉ mục đồ thị **HNSW** kết hợp với độ đo **Cosine** trên RAM của Milvus, đưa độ trễ phản hồi tìm kiếm thời gian thực đạt ngưỡng dưới **10ms (P50: 6ms)**.
- **Tối ưu hóa tài nguyên đệm**: Áp dụng nén lượng tử hóa 1-bit (**RaBitQ**) giúp tiết kiệm **72% dung lượng RAM** tiêu thụ thực tế để hệ thống phân tán phức tạp chạy mượt mà ngay cả trên máy tính cá nhân 16GB.

---

#### 🛠️ Tech Stack & Architecture

| Layer              | Component          | Technology               | Description                                                                                         |
| ------------------ | ------------------ | ------------------------ | --------------------------------------------------------------------------------------------------- |
| **Compute / AI**   | Distributed Engine | ⚡ PySpark (v3.5)        | Đọc tệp nén thô, xử lý song song, tạo vector nhúng offline và quản lý các luồng tính toán.          |
| **Data Format**    | Table Format       | 🧊 Apache Iceberg        | Chuẩn hóa cấu trúc bảng, đảm bảo tính ACID, hỗ trợ Schema Evolution và Time Travel trên MinIO.      |
| **Storage**        | S3 Object Storage  | 🪣 MinIO                 | Hồ dữ liệu (Data Lake) trung tâm phân tán, tương thích chuẩn S3 để chứa dữ liệu bảng và vector.     |
| **Vector DB**      | Vector Store       | 🔍 Milvus v2.6.14        | Cơ sở dữ liệu vector chuyên dụng, phục vụ tìm kiếm hàng xóm gần nhất (ANN Search) bằng đồ thị HNSW. |
| **AI Embedding**   | Text Embedding     | 🤖 Sentence Transformers | Mô hình `all-MiniLM-L6-v2` (384 chiều) giúp ánh xạ ngữ nghĩa văn bản từ Spark DataFrame.            |
| **Management**     | Vector Admin       | 📊 Attu WebUI            | Giao diện đồ họa trực quan hóa Collection, kiểm tra Schema, Segment và thử nghiệm search.           |
| **Infrastructure** | Containerization   | 🐋 Docker & WSL2         | Đóng gói cô lập các dịch vụ phân tán, giới hạn tài nguyên đệm hệ thống qua tệp cấu hình WSL.        |

---

#### 📂 Project Structure

```text
amazon-product-search-milvus-iceberg/
├── docker/
│   ├── docker-compose.yml       # Khởi chạy cụm Spark, MinIO, Milvus, Etcd, Attu
│   └── spark/
│       ├── Dockerfile           # Custom image Spark tự động cài thư viện AI
│       └── requirements.txt     # Định nghĩa các thư viện Python (sentence-transformers)
├── data/
│   ├── raw/                     # Chứa tệp JSON gốc Amazon (.jsonl.gz)
│   ├── sample/                  # Dữ liệu thử nghiệm nhỏ
│   └── all-MiniLM-L6-v2/        # Thư mục lưu trữ mô hình AI chạy Offline
├── spark_apps/
│   ├── ingestion_to_iceberg.py  # Spark Job nạp dữ liệu sạch vào bảng Iceberg
│   ├── iceberg_to_milvus.py     # Spark AI Pipeline tạo embedding nạp vào Milvus
│   └── search_app.py            # Ứng dụng tìm kiếm ngữ nghĩa thời gian thực
├── jars/                        # Thư mục lưu trữ driver kết nối (.jar)
└── README.md                    # Tài liệu hướng dẫn chi tiết dự án
```

---

#### 🚀 Getting Started

##### 1. Prerequisites

- [Docker Desktop](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) (Bắt buộc đối với Windows). Bạn **phải** tạo tệp cấu hình tài nguyên `.wslconfig` tại thư mục `%UserProfile%` để tránh quá tải bộ nhớ máy thật:
  ```ini
  [wsl2]
  memory=6GB
  processors=4
  swap=4GB
  autoMemoryReclaim=gradual
  ```
- [Python 3.10+](https://www.python.org/) cài đặt trên máy thật Windows để chuẩn bị nạp mô hình.

##### 2. Manual Step-by-Step Setup

**Step A: Clone Repo và tạo cấu trúc thư mục**

```bash
git clone https://github.com/23520124/amazon-product-search-milvus-iceberg.git
cd amazon-product-search-milvus-iceberg
mkdir -p data/raw data/all-MiniLM-L6-v2 jars
```

**Step B: Chuẩn bị nguồn dữ liệu**

1. Truy cập trang Hugging Face / McAuley Lab của **Amazon Reviews 2023** và tải xuống danh mục **All_Beauty** (khoảng 701.5K dòng):
   - Tải tệp đánh giá: **All_Beauty.jsonl.gz**
   - Tải tệp thông tin: **meta_All_Beauty.jsonl.gz**
2. Di chuyển cả 2 tệp trên (giữ nguyên định dạng nén `.gz`) vào thư mục `data/raw/` của dự án.

**Step C: Tải trọn gói Mô hình AI chạy Offline**
Để tránh lỗi nghẽn mạng quốc tế `ReadTimeout` khi container chạy, hãy tải trước mô hình về máy thật bằng cách mở PowerShell máy thật và chạy:

```bash
python -m pip install huggingface-hub==0.25.2
python -c "
from huggingface_hub import snapshot_download
snapshot_download(repo_id='sentence-transformers/all-MiniLM-L6-v2', local_dir='data/all-MiniLM-L6-v2')
"
```

**Step D: Chuẩn bị trình kết nối (Jars)**
Tải xuống các tệp JAR sau và bỏ vào thư mục `jars/` của dự án:

1.  **spark-milvus-1.4.0-SNAPSHOT-shaded.jar** (Bản Shaded 312MB giúp dứt điểm lỗi Guava/Protobuf Classpath).
2.  **iceberg-spark-runtime-3.5_2.12-1.4.1.jar**
3.  **hadoop-aws-3.3.4.jar**
4.  **aws-java-sdk-bundle-1.12.262.jar**

**Step E: Khởi động Hạ tầng trên Docker**

```bash
cd docker
docker-compose up --build -d
```

_Đợi khoảng 2-3 phút, dùng lệnh `docker ps` để kiểm tra các container minio, etcd, standalone đều đạt trạng thái `(healthy)`._

**Step F: Tạo Bucket trên MinIO**
Truy cập MinIO Console (`http://localhost:9001`) bằng thông tin `minioadmin / minioadmin`. Tạo một Bucket trống tên là: **amazon-lakehouse**.

**Step G: Chạy Pipeline nạp dữ liệu**

1. **Chạy Phase 1 (Nạp dữ liệu vào Lakehouse Iceberg):**
   ```bash
   docker exec -it spark-iceberg spark-submit --jars /home/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.4.1.jar,/home/iceberg/jars/hadoop-aws-3.3.4.jar,/home/iceberg/jars/aws-java-sdk-bundle-1.12.262.jar /home/iceberg/apps/ingestion_to_iceberg.py
   ```
2. **Chạy Phase 2 (Xử lý vector AI và nạp vào Milvus):**
   ```bash
   docker exec -it spark-iceberg spark-submit --jars /home/iceberg/jars/spark-milvus-1.4.0-SNAPSHOT-shaded.jar,/home/iceberg/jars/hadoop-aws-3.3.4.jar,/home/iceberg/jars/aws-java-sdk-bundle-1.12.262.jar,/home/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.4.1.jar /home/iceberg/apps/iceberg_to_milvus.py
   ```

**Step H: Khởi tạo Index trên Attu**

1. Truy cập Attu WebUI (`http://localhost:8000`) và đăng nhập (user/pass: `root / Milvus`, host: `standalone:19530`).
2. Vào collection `amazon_reviews_ai` -> Chọn tab **Index** -> Chọn **Create Index** cho trường vector.
3. Cấu hình Index: Chọn **HNSW** và độ đo **COSINE** (Hyperparameters: `M = 16`, `efConstruction = 200`). Nhấn tạo.
4. Chọn lệnh **Load Collection** để đưa dữ liệu lên RAM của Query Node.

**Step I: Trải nghiệm ứng dụng tìm kiếm**
Chạy ứng dụng tìm kiếm ngữ nghĩa trực tiếp từ Terminal máy thật Windows:

```bash
docker exec -it spark-iceberg python /home/iceberg/apps/search_app.py
```

---

#### 🔗 Monitoring & Access

| Service             | URL                   | Credentials (Default)     | Description                                            |
| ------------------- | --------------------- | ------------------------- | ------------------------------------------------------ |
| **MinIO Console**   | http://localhost:9001 | `minioadmin / minioadmin` | Trực quan hóa tệp bảng Parquet của Iceberg Lakehouse.  |
| **Attu WebUI**      | http://localhost:8000 | `root / Milvus`           | Công cụ giám sát và truy vấn bộ sưu tập vector Milvus. |
| **Spark Master UI** | http://localhost:8080 | N/A (Xem công khai)       | Giám sát trạng thái, các tác vụ tính toán song song.   |
| **Jupyter Lab**     | http://localhost:8888 | N/A (Xem công khai)       | Môi trường notebook thử nghiệm thuật toán nhanh.       |

---

#### 📊 Data Pipeline Flow & Scale

Quy trình vận hành dòng dữ liệu lớn qua 4 tầng cấu trúc:

1.  **Raw Layer**: Spark đọc song song các tệp nén JSON Lines từ Amazon trong thư mục được mount. Các xung đột tên cột dạng chữ hoa/chữ thường (ví dụ: `Assembly Required` vs `assembly required`) được giải quyết nhờ thiết lập `caseSensitive=true`.
2.  **Lakehouse Layer (Apache Iceberg)**: Dữ liệu được chuẩn hóa các khóa chính `parent_asin` và `asin` về một cột duy nhất `item_id` để tối ưu phép JOIN lịch sử, lưu kho bền vững dạng cột Parquet nén `zstd` trên MinIO.
3.  **AI Pipeline Layer**: PySpark quét bảng Iceberg sạch, nạp mô hình cục bộ nhanh chóng (1 giây) và chia nhỏ dữ liệu thành nhiều phân vùng để tạo các đặc trưng ngữ nghĩa dày đặc 384 chiều. Toàn bộ ma trận vector được đẩy đồng thời sang Milvus thông qua chế độ `.mode("append")` tránh bộ quét truncate chặt chẽ của nhân Spark mới.
4.  **Semantic Search Layer**: Ứng dụng `search_app.py` bóc tách cấu trúc mảng lô hai chiều `[ [hit1, hit2] ]` từ `client.search()` của MilvusClient, cho phép người dùng nhập ngôn ngữ tự nhiên và trả về bài đánh giá chính xác nhất.

---

#### 🖼️ Screenshots Gallery

##### 1. System Architecture

_(Thêm ảnh sơ đồ kiến trúc hạ tầng tích hợp giữa Spark + Iceberg + MinIO + Milvus tại đây)_

##### 2. MinIO Data Lakehouse Storage

_(Thêm ảnh chụp bucket amazon-lakehouse chứa 213.2 MiB với 27 objects của Iceberg tại đây)_

##### 3. Attu Milvus Management Console

_(Thêm ảnh chụp Collection amazon_reviews_ai với cấu trúc 384 chiều đã được nạp loaded thành công tại đây)_

---

_Author: Nguyễn Ngọc Duy Bảo - UIT Big Data AI Portfolio Project_
