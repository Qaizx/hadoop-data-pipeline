# Hadoop Data Pipeline Project

A comprehensive data processing pipeline using Hadoop, Hive, Spark, and Apache Airflow for financial data analysis.

## Project Overview

This project implements a complete big data processing pipeline for financial data analysis. It includes:

- **Apache Hadoop/HDFS** for distributed storage
- **Apache Hive** for data warehousing and SQL queries  
- **Apache Spark** for large-scale data processing
- **Apache Airflow** for workflow orchestration
- **Docker Compose** for containerized deployment

## Architecture Components

### Core Services
- **Hadoop NameNode & DataNode**: Distributed file system
- **Hive Metastore**: Schema management for data warehouse
- **Apache Airflow**: Workflow scheduling and monitoring
- **PostgreSQL**: Metadata storage for Hive and Airflow

### Data Processing
- **Finance Pipeline**: Processes financial data from 2023-2025
- **HDFS API**: Python interface for Hadoop file operations
- **Hive Integration**: SQL queries on distributed datasets

## Project Structure

```
├── airflow/                 # Airflow configuration and DAGs
│   ├── dags/               # Workflow definitions
│   └── Dockerfile.airflow  # Airflow container setup
├── data/                   # Sample financial datasets
├── jobs/                   # Data processing jobs
├── docker-compose.yaml     # Multi-container orchestration
├── Dockerfile.spark        # Spark container setup
├── hive-site.xml          # Hive configuration
└── *.py                   # Python APIs and tests
```

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Git

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/hadoop-pipeline.git
   cd hadoop-pipeline
   ```

2. **Start the services**
   ```bash
   docker-compose up -d
   ```

3. **Access the services**
   - Airflow UI: http://localhost:8080
   - Hadoop NameNode: http://localhost:9870
   - Hive Metastore: Available on port 9083

### Running Data Pipeline

The main data pipeline is orchestrated through Airflow:

1. Access Airflow UI at http://localhost:8080
2. Enable the `hadoop_dag` DAG
3. Monitor pipeline execution and logs

## File Descriptions

- `hive_hdfs_api.py`: Python interface for HDFS operations
- `hive_test.py`: Hive integration tests
- `jobs/finance_pipeline.py`: Main data processing pipeline
- `hadoop-hive.env`: Environment variables for services
- `docker-compose.yaml`: Service orchestration configuration

## Data Flow

1. **Ingestion**: Raw financial data loaded into HDFS
2. **Processing**: Spark jobs transform and analyze data
3. **Storage**: Processed data stored in Hive tables
4. **Orchestration**: Airflow manages the entire workflow
5. **Monitoring**: Real-time pipeline monitoring via Airflow UI

## Development

### Local Development Setup

1. **Create Python virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt  # Create this file with your dependencies
   ```

3. **Run tests**
   ```bash
   python hive_test.py
   python hive_hdfs_api_test.py
   ```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions and support, please open an issue on GitHub.