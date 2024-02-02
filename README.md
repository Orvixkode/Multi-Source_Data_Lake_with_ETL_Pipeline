# Multi-Source Data Lake with ETL Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.1-009688.svg)](https://fastapi.tiangolo.com)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.7.3-017CEE.svg)](https://airflow.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive, production-ready data lake system that ingests data from multiple heterogeneous sources, performs ETL transformations, and provides unified query interfaces.

## 🚀 Key Features

- **Scalable Data Ingestion**: Process 5M+ records daily from multiple sources
- **Multi-Database Support**: MongoDB, PostgreSQL, InfluxDB integration
- **Industrial IoT Protocols**: OPC UA and MQTT support
- **Automated ETL Workflows**: Apache Airflow-based orchestration
- **Data Quality Validation**: Built-in validation and monitoring
- **RESTful API**: FastAPI endpoints for data queries
- **Real-time Monitoring**: Grafana dashboards and Prometheus metrics
- **Docker Support**: Containerized deployment

## 📊 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
│  APIs │ Files │ PostgreSQL │ MongoDB │ InfluxDB │ OPC UA │ MQTT │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  ETL Pipeline (Airflow)                      │
│  ┌──────────┐   ┌────────────┐   ┌──────────┐              │
│  │ Extract  │──▶│ Transform  │──▶│  Load    │              │
│  └──────────┘   └────────────┘   └──────────┘              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Data Lake Storage                         │
│  PostgreSQL (Structured) │ MongoDB (Semi) │ InfluxDB (Time)  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Query & Analytics Layer (FastAPI)               │
│  REST API │ Data Validation │ Metrics │ Monitoring           │
└─────────────────────────────────────────────────────────────┘
```

## 🛠️ Tech Stack

- **Backend**: Python 3.11+, FastAPI, .NET Core
- **Frontend**: React, TypeScript
- **Databases**: PostgreSQL, MongoDB, InfluxDB, Redis
- **Orchestration**: Apache Airflow
- **Data Processing**: Pandas, Polars, NumPy
- **Protocols**: OPC UA, MQTT, HTTP/REST
- **Monitoring**: Grafana, Prometheus
- **Containerization**: Docker, Docker Compose
- **Testing**: Pytest, Great Expectations

## 📁 Project Structure

```
Multi-Source_Data_Lake_with_ETL_Pipeline/
├── src/
│   ├── api/                    # FastAPI application
│   ├── connectors/             # Database & source connectors
│   ├── etl/                    # ETL components
│   │   ├── extractors/         # Data extraction logic
│   │   ├── transformers/       # Data transformation
│   │   └── loaders/            # Data loading
│   ├── validators/             # Data quality checks
│   └── utils/                  # Shared utilities
├── airflow/
│   ├── dags/                   # Airflow DAG definitions
│   ├── plugins/                # Custom Airflow plugins
│   └── config/                 # Airflow configuration
├── frontend/                   # React dashboard
├── docker/                     # Docker configurations
├── scripts/                    # Utility scripts
├── tests/                      # Test suites
├── config/                     # Application configs
└── data/                       # Data storage
    ├── raw/                    # Raw ingested data
    ├── staging/                # Intermediate data
    └── processed/              # Final processed data
```

## 🚦 Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Node.js 18+ (for frontend)
- PostgreSQL 15+
- MongoDB 7+
- InfluxDB 2.x

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd Multi-Source_Data_Lake_with_ETL_Pipeline
```

2. **Set up Python virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. **Start Docker services**
```bash
docker-compose up -d
```

5. **Initialize Airflow**
```bash
./scripts/init_airflow.sh
```

6. **Run the API server**
```bash
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

7. **Access the services**
- API Documentation: http://localhost:8000/docs
- Airflow UI: http://localhost:8080
- Grafana: http://localhost:3000
- Frontend: http://localhost:3001

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the root directory:

```env
# Database Connections
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=datalake
POSTGRES_USER=admin
POSTGRES_PASSWORD=your_password

MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=datalake
MONGO_USER=admin
MONGO_PASSWORD=your_password

INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=your_token
INFLUX_ORG=your_org
INFLUX_BUCKET=datalake

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Airflow
AIRFLOW_HOME=/path/to/airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin

# API
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Monitoring
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
```

## 📝 Usage Examples

### Starting ETL Pipeline

```python
from src.etl.pipeline import DataPipeline
from src.connectors.postgres_connector import PostgresConnector

# Initialize pipeline
pipeline = DataPipeline()

# Add source
source = PostgresConnector(config)
pipeline.add_source(source)

# Run ETL
pipeline.run()
```

### Querying Data via API

```bash
# Get records from PostgreSQL
curl http://localhost:8000/api/v1/query/postgres?table=users&limit=100

# Get time-series data from InfluxDB
curl http://localhost:8000/api/v1/query/influx?measurement=sensor_data&start=-1h

# Get documents from MongoDB
curl http://localhost:8000/api/v1/query/mongo?collection=events&filter={"status":"active"}
```

### Creating Airflow DAG

```python
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1)
)

# Add tasks...
```

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test suite
pytest tests/unit/
pytest tests/integration/
```

## 📊 Monitoring & Metrics

The system provides comprehensive monitoring:

- **Prometheus Metrics**: `/metrics` endpoint
- **Grafana Dashboards**: Pre-configured dashboards for data flow monitoring
- **Airflow Monitoring**: DAG execution metrics and logs
- **API Metrics**: Request rates, latency, error rates

## 🔒 Security

- Environment-based configuration
- Database connection pooling
- API rate limiting
- Input validation and sanitization
- Secrets management via environment variables
- Role-based access control (RBAC) in Airflow

## 🚀 Deployment

### Docker Deployment

```bash
docker-compose -f docker-compose.prod.yml up -d
```

### Kubernetes Deployment

```bash
kubectl apply -f k8s/
```

## 📈 Performance

- Processes **5M+ records daily**
- Supports **10+ concurrent projects**
- Sub-second API response times
- Horizontal scaling capability
- Automated data partitioning

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👥 Team

- Data Engineering Team
- Backend Development Team
- DevOps Team

## 📞 Support

For issues and questions:
- Create an issue in the repository
- Contact: data-team@example.com

## 🗺️ Roadmap

- [ ] Stream processing with Apache Kafka
- [ ] Machine Learning pipeline integration
- [ ] Advanced data cataloging
- [ ] GraphQL API support
- [ ] Real-time alerting system
- [ ] Data lineage tracking
- [ ] Multi-cloud support (AWS, Azure, GCP)

## 📚 Documentation

Detailed documentation is available in the `/docs` directory:
- [Architecture Guide](docs/architecture.md)
- [API Reference](docs/api.md)
- [ETL Guide](docs/etl.md)
- [Deployment Guide](docs/deployment.md)

---

**Built with ❤️ by the Data Engineering Team**
