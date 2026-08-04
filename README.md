<div align="center">
  <h1>AI-Powered Data Lakehouse & Semantic Search 🌊🔍</h1>
  <p><em>An End-to-End Spark AI Pipeline & Modern Data Lakehouse on MinIO and Milvus</em></p>

[![Apache Spark](https://img.shields.io/badge/PySpark-Data_Processing-FDEE21?logo=apachespark&logoColor=black)]()
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Table%20Format-blue?logo=apacheiceberg&logoColor=white)]()
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Object%20Storage-C92847?logo=minio&logoColor=white)]()
[![Milvus](https://img.shields.io/badge/Milvus-Vector%20Database-00A2E8?logo=milvus&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-Infrastructure-2CA5E0?logo=docker&logoColor=white)]()
[![Hugging Face](https://img.shields.io/badge/Hugging%20Face-AI%20Transformers-FFD21E?logo=huggingface&logoColor=black)]()

</div>

---

#### 📖 Project Overview

This project implements an AI-powered **Modern Data Lakehouse & AI Vector Search Pipeline** to ingest, process, clean, and run real-time semantic queries on e-commerce datasets.

Rather than relying on traditional exact keyword matching, the system integrates a local Large Language Model (LLM) representation to "understand" user queries (even in Vietnamese) and retrieve English Amazon product reviews with highly relevant contextual meanings within milliseconds. The project leverages the **Amazon Product Reviews (All_Beauty 2023)** dataset, containing over **701,500+ reviews** and **112,500+ product metadata records**, with an architecture built to scale-out seamlessly to the heavy **Electronics** category with over **43.9 Million records**.

##### Key Features

- **High-Performance ETL Ingestion**: Utilizes Apache Spark (PySpark) to run parallelized batch processes that ingest, clean, and resolve schema conflicts in raw compressed Amazon JSON Lines datasets.
- **Modern Data Lakehouse Architecture**: Deploys the Apache Iceberg table format on top of MinIO (S3-compatible object storage) to establish a structured, transactional data lake offering ACID guarantees, Schema Evolution, and Time Travel.
- **Offline Distributed AI Pipeline**: PySpark orchestrates distributed, multi-threaded CPU processing to load the `all-MiniLM-L6-v2` transformer model offline and convert unstructured text columns into 384-dimensional dense vector embeddings.
- **Cloud-Native Vector DB**: Integrates Milvus 2.6.14 standalone using the modern **Woodpecker WAL** (Write-Ahead Log) engine to stream write logs directly to MinIO, achieving a peak throughput of **750 MB/s** without the operational complexity of dedicated Kafka/Pulsar clusters.
- **Real-time Semantic Query**: Leverages the hierarchical small-world graph (**HNSW**) index coupled with **Cosine** similarity on Milvus memory to deliver real-time vector search responses with sub-**10ms latency (P50: 6ms, P99: 35ms)**.
- **Resource Footprint Optimization**: Utilizes 1-bit quantization via **RaBitQ** to reduce vector index RAM usage by **72%** while maintaining over **95% recall accuracy**, enabling the entire distributed platform to run smoothly on a standard 16GB RAM local workstation.

---

#### 🛠️ Tech Stack & Architecture

| Layer              | Component          | Technology               | Description                                                                                                                |
| ------------------ | ------------------ | ------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| **Compute / AI**   | Distributed Engine | ⚡ PySpark (v3.5)        | Reads raw compressed files, processes batch jobs, generates offline embeddings, and orchestrates distributed computations. |
| **Data Format**    | Table Format       | 🧊 Apache Iceberg        | Standardizes tables, provides ACID transactions, and enables Schema Evolution and Time Travel on MinIO.                    |
| **Storage**        | S3 Object Storage  | 🪣 MinIO                 | Serves as the central S3-compatible Data Lake to persist relational Iceberg tables and raw vector segment files.           |
| **Vector DB**      | Vector Store       | 🔍 Milvus v2.6.14        | Stores dense embeddings and performs high-speed Approximate Nearest Neighbor (ANN) search via HNSW graphs.                 |
| **AI Embedding**   | Text Embedding     | 🤖 Sentence Transformers | Implements the `all-MiniLM-L6-v2` model (384-dim) to capture semantic meanings from review texts.                          |
| **Management**     | Vector Admin       | 📊 Attu WebUI            | Offers a graphical management console to inspect Collections, Schemas, Segments, and test live queries.                    |
| **Infrastructure** | Containerization   | 🐋 Docker & WSL2         | Containerizes services in an isolated environment, managing memory constraints via WSL configuration.                      |

---

#### 📂 Project Structure

```text
amazon-product-search-milvus-iceberg/
├── docker/
│   ├── docker-compose.yml       # Spins up Spark, MinIO, Milvus standalone, Etcd, and Attu
│   └── spark/
│       ├── Dockerfile           # Custom Spark image pre-installed with AI requirements
│       └── requirements.txt     # Python package definitions (sentence-transformers, pymilvus)
├── data/
│   ├── raw/                     # Raw Amazon JSON Lines dataset files (.jsonl.gz)
│   ├── sample/                  # Small sample files for rapid debugging
│   └── all-MiniLM-L6-v2/        # Offline weights and configuration of the embedding model
├── spark_apps/
│   ├── ingestion_to_iceberg.py  # Spark ETL job to ingest clean records into Iceberg tables
│   ├── iceberg_to_milvus.py     # Spark AI pipeline generating embeddings and inserting to Milvus
│   └── search_app.py            # Live terminal-based semantic product search application
├── jars/                        # Holds system connection and connector driver tethers (.jar)
└── README.md                    # Detailed documentation and guidelines
```

---

#### 🚀 Getting Started

##### 1. Prerequisites

- [Docker Desktop](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) (Required if running on Windows). You **must** limit the WSL virtual machine's RAM footprint to prevent resource exhaustion. Create a `.wslconfig` file in your `%UserProfile%` directory:
  ```ini
  [wsl2]
  memory=6GB
  processors=4
  swap=4GB
  autoMemoryReclaim=gradual
  ```
- [Python 3.10+](https://www.python.org/) installed locally on your Windows host machine to pre-download the AI model offline.

##### 2. Manual Step-by-Step Setup

**Step A: Clone the Repository & Initialize Folders**

```bash
git clone https://github.com/23520124/amazon-product-search-milvus-iceberg.git
cd amazon-product-search-milvus-iceberg
mkdir -p data/raw data/all-MiniLM-L6-v2 jars
```

**Step B: Prepare the Dataset**

1. Navigate to McAuley Lab's **Amazon Reviews 2023** page and download the following files from the **All_Beauty** category (~701.5K records):
   - Reviews data: **All_Beauty.jsonl.gz**
   - Metadata: **meta_All_Beauty.jsonl.gz**
2. Move both downloaded compressed `.gz` files directly into the `data/raw/` directory. Do not extract them; Apache Spark will decompress them on-the-fly.

**Step C: Download the Embedding Model for Offline Use**
To prevent international network latency or `ReadTimeout` exceptions inside the Spark container, run this script on your **host machine** to cache the model parameters locally:

```bash
python -m pip install huggingface-hub==0.25.2
python -c "
from huggingface_hub import snapshot_download
snapshot_download(repo_id='sentence-transformers/all-MiniLM-L6-v2', local_dir='data/all-MiniLM-L6-v2')
"
```

**Step D: Gather System Connections Drivers (JARs)**
Download and copy the following JAR dependencies into your local `jars/` directory:

1.  **spark-milvus-1.4.0-SNAPSHOT-shaded.jar** (A crucial 312MB shaded uber-JAR enclosing isolated Protobuf and Guava classpath layers to resolve JVM conflicts).
2.  **iceberg-spark-runtime-3.5_2.12-1.4.1.jar**
3.  **hadoop-aws-3.3.4.jar**
4.  **aws-java-sdk-bundle-1.12.262.jar**

**Step E: Spin up Infrastructure Containers**

```bash
cd docker
docker-compose up --build -d
```

_Wait 2–3 minutes and execute `docker ps` to verify all containers (minio, etcd, Milvus standalone) display a `(healthy)` status._

**Step F: Initialize Storage Buckets**
Log into the MinIO Console (`http://localhost:9001`) with the credentials `minioadmin / minioadmin`. Create a new bucket named **amazon-lakehouse**.

**Step G: Run the ETL and AI Pipeline**

1. **Phase 1: Ingest Raw Logs to Iceberg Lakehouse:**
   ```bash
   docker exec -it spark-iceberg spark-submit --jars /home/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.4.1.jar,/home/iceberg/jars/hadoop-aws-3.3.4.jar,/home/iceberg/jars/aws-java-sdk-bundle-1.12.262.jar /home/iceberg/apps/ingestion_to_iceberg.py
   ```
2. **Phase 2: Extract Relational Data, Generate AI Vectors, and Insert to Milvus:**
   ```bash
   docker exec -it spark-iceberg spark-submit --jars /home/iceberg/jars/spark-milvus-1.4.0-SNAPSHOT-shaded.jar,/home/iceberg/jars/hadoop-aws-3.3.4.jar,/home/iceberg/jars/aws-java-sdk-bundle-1.12.262.jar,/home/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.4.1.jar /home/iceberg/apps/iceberg_to_milvus.py
   ```

**Step H: Build Vector Index on Attu WebUI**

1. Open the Attu Console (`http://localhost:8000`) and connect using: uri: `standalone:19530`, username: `root`, password: `Milvus`.
2. Locate the `amazon_reviews_ai` collection -> Navigate to the **Index** tab -> Click **Create Index** on the `vector` field.
3. Configure the index as: Index Type: **HNSW**, Metric Type: **COSINE**, with Hyperparameters `M = 16` and `efConstruction = 200`. Create the index.
4. Select **Load Collection** to load the index and vector records onto the RAM of Milvus's query node.

**Step I: Run Semantic Queries**
Initiate semantic searches directly from your terminal using natural language inputs:

```bash
docker exec -it spark-iceberg python /home/iceberg/apps/search_app.py
```

---

#### 🔗 Monitoring & Access

| Service             | URL                   | Credentials (Default)     | Description                                                                                  |
| ------------------- | --------------------- | ------------------------- | -------------------------------------------------------------------------------------------- |
| **MinIO Console**   | http://localhost:9001 | `minioadmin / minioadmin` | GUI to monitor compressed Parquet transactions and Iceberg metadata manifests.               |
| **Attu WebUI**      | http://localhost:8000 | `root / Milvus`           | Visual administration dashboard to configure Milvus collections, index states, and segments. |
| **Spark Master UI** | http://localhost:8080 | N/A (Public)              | Web dashboard displaying task states, metrics, and parallelized worker statuses.             |
| **Jupyter Lab**     | http://localhost:8888 | N/A (Public)              | Web-based notebook workspace ideal for prototyping ad-hoc scripts.                           |

---

#### 📊 Data Pipeline Flow & Scale

The end-to-end data pipeline is structured as follows:

1.  **Raw Layer**: Spark processes compressed JSON Lines datasets in parallel. Unstructured column variations and case-sensitivity duplicates (e.g., `Assembly Required` vs `assembly required`) are resolved by forcing case-sensitive processing (`caseSensitive=true`).
2.  **Lakehouse Layer (Apache Iceberg)**: Data tables are modeled by normalizing inconsistent keys (`parent_asin` and `asin`) into a cohesive primary key called `item_id`. Relational records are compressed under high-performance `zstd` Parquet formats in MinIO.
3.  **AI Pipeline Layer**: PySpark queries the cleaned Iceberg tables, loads the offline model in 1 second, and partitions the records to calculate 384-dimensional dense vectors. Records are inserted into Milvus under `.mode("append")` to bypass Spark 3.5.6+'s strict internal schema truncation checks.
4.  **Semantic Search Layer**: The `search_app.py` wrapper intercepts the multidimensional batch search results array (`[ [hit1, hit2] ]`) returned from MilvusClient, extracts exact dictionary hits, maps query intent, and displays contextually similar results under 10ms.

---

#### 🖼️ Screenshots Gallery

##### 1. System Architecture

_image_

##### 2. MinIO Data Lakehouse Storage

_image_

##### 3. Attu Milvus Management Console

_image_

---

_Author: Nguyễn Ngọc Duy Bảo - UIT Big Data AI Portfolio Project_
