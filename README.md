![CI](https://github.com/Ronnagon-Phukahuta/hadoop-data-pipeline/actions/workflows/ci.yml/badge.svg)

# Finance ITSC Dashboard

ระบบ Data Lake และ Dashboard สำหรับวิเคราะห์งบประมาณ ITSC มหาวิทยาลัยเชียงใหม่

## Architecture

```
flowchart TD
    subgraph Input
        A[📊 Excel / CSV]
        B[🤖 GPT\nColumn Fixer]
    end

    subgraph HDFS["HDFS Data Lake"]
        C[📁 Raw Zone\n/datalake/raw]
        D[📁 Staging Zone\n/datalake/staging]
        E[📁 Curated Zone\n/datalake/curated]
    end

    subgraph ETL["ETL Layer (PySpark)"]
        F[⚡ Spark Job\nfinance_itsc_pipeline.py]
        G{Data Quality\nChecks}
        H[✅ .done marker]
        I[❌ .failed marker]
        J[📧 Email Alert]
    end

    subgraph Orchestration
        K[🌀 Airflow DAG\nevery 5 min]
    end

    subgraph Serving["Serving Layer (Hive)"]
        L[(🐝 Hive\nWide Table)]
        M[(🐝 Hive\nLong Table)]
    end

    subgraph Dashboard["Dashboard (Streamlit)"]
        N[📈 Charts\nPlotly]
        O[💬 NLP Query\nThai → HiveQL]
        P[🔐 Auth]
    end

    subgraph Infra
        Q[🔒 Nginx\nHTTPS Proxy]
        R[🐳 Docker Compose]
    end

    A --> B --> C
    K --> F
    C --> F
    F --> G
    G -->|Pass| H
    G -->|Fail| I --> J
    H --> L
    L --> M
    L --> N
    L --> O
    O -->|GPT| O
    N --> Q
    O --> Q
    P --> Q
    R -.->|runs| HDFS
    R -.->|runs| ETL
    R -.->|runs| Dashboard
    R -.->|runs| Orchestration
```

**Stack**
- **Data Lake**: Hadoop HDFS + Hive Metastore
- **ETL**: Apache Spark (PySpark)
- **Orchestration**: Apache Airflow
- **Dashboard**: Streamlit + Plotly
- **NLP**: OpenAI GPT → HiveQL
- **Proxy**: Nginx (HTTPS)

## Project Structure

```
HADOOP_NEW/
├── airflow/
│   ├── dags/               # Airflow DAGs
│   └── Dockerfile.airflow
├── dashboard/
│   ├── components/         # Streamlit UI components
│   ├── services/           # Hive + GPT integration
│   ├── utils/              # History, helpers
│   ├── app.py              # Entry point
│   ├── auth.py             # Authentication
│   └── config.py           # Table schema, category mapping
├── jobs/
│   └── finance_itsc_pipeline.py  # Spark ETL + Data Quality
├── tests/                  # Unit tests (pytest)
├── certs/                  # SSL certificates (ไม่ commit)
├── data/                   # Raw data files (ไม่ commit)
├── docker-compose.yaml
├── nginx.conf
└── .env                    # ไม่ commit — ดู .env.example
```

## Prerequisites

- Docker + Docker Compose
- OpenAI API Key
- Gmail App Password (สำหรับ email alerts)

## Setup

**1. Clone และตั้งค่า environment**
```bash
git clone <repo-url>
cd HADOOP_NEW
cp .env.example .env
# แก้ไข .env ใส่ค่าจริง
```

**2. สร้าง SSL Certificate**
```bash
# Windows (Git Bash)
bash generate_cert.sh

# Linux/Mac
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout certs/server.key \
    -out certs/server.crt \
    -subj "/C=TH/ST=ChiangMai/O=ITSC-CMU/CN=localhost"
```

**3. สร้าง config.py จาก example**
```bash
cp dashboard/config.py.example dashboard/config.py
# แก้ไข config.py ตามต้องการ
```

**4. รัน Docker Compose**
```bash
docker compose up -d
```

**5. ตั้งค่า Airflow**
```bash
# เข้า Airflow UI: http://localhost:8088
# Admin → Variables → เพิ่ม:
#   Key: alert_email
#   Value: your-email@gmail.com
```

**6. Upload ข้อมูลเข้า HDFS**
```bash
# สร้าง directory structure
docker exec namenode hdfs dfs -mkdir -p /datalake/raw/finance-itsc/year=2024

# Upload CSV
docker exec -i namenode hdfs dfs -put /data/finance_itsc_2024.csv \
    /datalake/raw/finance-itsc/year=2024/
```

## Services

| Service | URL | หมายเหตุ |
|---------|-----|---------|
| Dashboard | https://localhost | หน้าหลัก |
| Airflow | http://localhost:8088 | Pipeline management |
| Spark Master | http://localhost:8080 | หรือ https://localhost/spark/ |
| HDFS NameNode | http://localhost:9870 | |
| Hive Server | localhost:10000 | JDBC |

## ETL Pipeline

Pipeline รันอัตโนมัติทุก 5 นาที ผ่าน Airflow DAG `finance_etl_pipeline`

**Flow:**
```
1. ตรวจ HDFS หาไฟล์ใหม่ (ไม่มี .done marker)
2. Data Quality checks (schema, null, date format, total amount)
3. ถ้าผ่าน → load เข้า Hive Wide table → สร้าง .done
4. ถ้าไม่ผ่าน → สร้าง .failed → ส่ง email alert
5. แปลง Wide → Long format
```

**Marker files:**
- `filename.csv.done` — processed สำเร็จ
- `filename.csv.failed` — Data Quality failed (ต้องแก้ไขก่อน retry)

## Data Quality Checks

| Check | ระดับ | รายละเอียด |
|-------|-------|-----------|
| Schema | Fatal | Column ครบ 32 อัน |
| Null Values | Fatal | date, details ห้าม null |
| Date Format | Fatal | ต้องมี all-year-budget, total spent, remaining |
| Total Amount | Warning | total_amount ≈ sum ทุก column (±1%) |
| Remaining | Warning | remaining ต้องลดหลั่นทุกเดือน |

## Running Tests

```bash
pytest tests/ -v
```

## Troubleshooting

**Spark ใช้ Python ผิด version**
```bash
# ตรวจสอบ PYSPARK_PYTHON ใน docker-compose.yaml
- PYSPARK_PYTHON=python3
- PYSPARK_DRIVER_PYTHON=python3
```

**Hive reserved keyword error**
```
Pipeline จะ auto-fix `date` → `\`date\`` อัตโนมัติ
```

**HDFS ไม่ขึ้น**
```bash
docker compose restart namenode datanode
```

**Dashboard ไม่อัพเดทหลังแก้โค้ด**
```bash
docker compose restart streamlit-dashboard
```