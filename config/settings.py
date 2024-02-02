"""
Configuration Settings for Multi-Source Data Lake System
"""
from pydantic import BaseSettings, Field
from typing import List, Optional
import os
from pathlib import Path

class Settings(BaseSettings):
    """Application settings"""
    
    # Application Configuration
    app_name: str = Field(default="Multi-Source Data Lake", env="APP_NAME")
    app_env: str = Field(default="development", env="ENVIRONMENT")
    app_debug: bool = Field(default=True, env="API_DEBUG")
    
    # API Configuration
    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8000, env="API_PORT")
    api_workers: int = Field(default=4, env="API_WORKERS")
    api_reload: bool = Field(default=True, env="API_RELOAD")
    
    # CORS Configuration
    cors_origins: str = Field(default="*", env="CORS_ORIGINS")
    
    @property
    def cors_origins_list(self) -> List[str]:
        """Convert CORS origins string to list"""
        if self.cors_origins == "*":
            return ["*"]
        return [origin.strip() for origin in self.cors_origins.split(",")]
    
    # Database Configuration - PostgreSQL
    postgres_host: str = Field(default="localhost", env="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, env="POSTGRES_PORT")
    postgres_db: str = Field(default="datalake", env="POSTGRES_DB")
    postgres_user: str = Field(default="admin", env="POSTGRES_USER")
    postgres_password: str = Field(default="changeme123", env="POSTGRES_PASSWORD")
    postgres_max_connections: int = Field(default=20, env="POSTGRES_MAX_CONNECTIONS")
    
    @property
    def postgres_url(self) -> str:
        """PostgreSQL connection URL"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    @property
    def postgres_async_url(self) -> str:
        """PostgreSQL async connection URL"""
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    # Database Configuration - MongoDB
    mongo_host: str = Field(default="localhost", env="MONGO_HOST")
    mongo_port: int = Field(default=27017, env="MONGO_PORT")
    mongo_db: str = Field(default="datalake", env="MONGO_DB")
    mongo_user: str = Field(default="admin", env="MONGO_USER")
    mongo_password: str = Field(default="changeme123", env="MONGO_PASSWORD")
    mongo_max_pool_size: int = Field(default=100, env="MONGO_MAX_POOL_SIZE")
    
    @property
    def mongo_url(self) -> str:
        """MongoDB connection URL"""
        if self.mongo_user and self.mongo_password:
            return f"mongodb://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}/{self.mongo_db}"
        return f"mongodb://{self.mongo_host}:{self.mongo_port}/{self.mongo_db}"
    
    # Database Configuration - InfluxDB
    influx_url: str = Field(default="http://localhost:8086", env="INFLUX_URL")
    influx_token: str = Field(default="my-super-secret-token", env="INFLUX_TOKEN")
    influx_org: str = Field(default="data-engineering", env="INFLUX_ORG")
    influx_bucket: str = Field(default="datalake", env="INFLUX_BUCKET")
    
    # Database Configuration - Redis
    redis_host: str = Field(default="localhost", env="REDIS_HOST")
    redis_port: int = Field(default=6379, env="REDIS_PORT")
    redis_password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    redis_db: int = Field(default=0, env="REDIS_DB")
    
    @property
    def redis_url(self) -> str:
        """Redis connection URL"""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    # Airflow Configuration
    airflow_home: str = Field(default="./airflow", env="AIRFLOW_HOME")
    airflow_admin_user: str = Field(default="admin", env="AIRFLOW_ADMIN_USER")
    airflow_admin_password: str = Field(default="admin", env="AIRFLOW_ADMIN_PASSWORD")
    airflow_admin_email: str = Field(default="admin@example.com", env="AIRFLOW_ADMIN_EMAIL")
    airflow_executor: str = Field(default="LocalExecutor", env="AIRFLOW__CORE__EXECUTOR")
    airflow_sql_alchemy_conn: str = Field(default="", env="AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    airflow_fernet_key: str = Field(default="", env="AIRFLOW__CORE__FERNET_KEY")
    
    # OPC UA Configuration
    opcua_endpoint: str = Field(default="opc.tcp://localhost:4840", env="OPCUA_ENDPOINT")
    opcua_security_policy: str = Field(default="None", env="OPCUA_SECURITY_POLICY")
    
    # MQTT Configuration
    mqtt_broker: str = Field(default="localhost", env="MQTT_BROKER")
    mqtt_port: int = Field(default=1883, env="MQTT_PORT")
    mqtt_username: Optional[str] = Field(default=None, env="MQTT_USERNAME")
    mqtt_password: Optional[str] = Field(default=None, env="MQTT_PASSWORD")
    mqtt_topics: str = Field(default="data/sensors/#", env="MQTT_TOPIC")
    
    @property
    def mqtt_topics_list(self) -> List[str]:
        """Convert MQTT topics string to list"""
        return [topic.strip() for topic in self.mqtt_topics.split(",")]
    
    # Data Paths
    data_base_path: str = Field(default="./data", env="DATA_BASE_PATH")
    
    @property
    def data_raw_path(self) -> str:
        """Raw data storage path"""
        return os.path.join(self.data_base_path, "raw")
    
    @property
    def data_staging_path(self) -> str:
        """Staging data storage path"""
        return os.path.join(self.data_base_path, "staging")
    
    @property
    def data_processed_path(self) -> str:
        """Processed data storage path"""
        return os.path.join(self.data_base_path, "processed")
    
    @property
    def data_archive_path(self) -> str:
        """Archive data storage path"""
        return os.path.join(self.data_base_path, "archive")
    
    # ETL Configuration
    etl_batch_size: int = Field(default=1000, env="ETL_BATCH_SIZE")
    etl_parallel_workers: int = Field(default=4, env="ETL_PARALLEL_WORKERS")
    etl_retry_attempts: int = Field(default=3, env="ETL_RETRY_ATTEMPTS")
    etl_retry_delay: int = Field(default=60, env="ETL_RETRY_DELAY")  # seconds
    
    # Logging Configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: Optional[str] = Field(default="logs/datalake.log", env="LOG_FILE")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )
    
    # Security Configuration
    secret_key: str = Field(default="your-secret-key-change-in-production", env="SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    access_token_expire_minutes: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES")
    
    # Monitoring Configuration
    prometheus_port: int = Field(default=9090, env="PROMETHEUS_PORT")
    grafana_port: int = Field(default=3000, env="GRAFANA_PORT")
    grafana_admin_user: str = Field(default="admin", env="GRAFANA_ADMIN_USER")
    grafana_admin_password: str = Field(default="admin", env="GRAFANA_ADMIN_PASSWORD")
    
    # Data Quality Configuration
    data_quality_max_null_percentage: float = Field(default=0.1, env="DATA_QUALITY_MAX_NULL_PERCENTAGE")
    data_quality_max_duplicate_percentage: float = Field(default=0.05, env="DATA_QUALITY_MAX_DUPLICATE_PERCENTAGE")
    data_quality_min_records: int = Field(default=1, env="DATA_QUALITY_MIN_RECORDS")
    
    # Performance Configuration
    max_concurrent_tasks: int = Field(default=10, env="MAX_CONCURRENT_TASKS")
    request_timeout: int = Field(default=300, env="REQUEST_TIMEOUT")  # seconds
    connection_pool_size: int = Field(default=20, env="CONNECTION_POOL_SIZE")
    
    # File Processing Configuration
    max_file_size_mb: int = Field(default=100, env="MAX_FILE_SIZE_MB")
    supported_file_types: str = Field(default="json,csv,parquet,xlsx", env="SUPPORTED_FILE_TYPES")
    
    @property
    def supported_file_types_list(self) -> List[str]:
        """Convert supported file types string to list"""
        return [ftype.strip() for ftype in self.supported_file_types.split(",")]
    
    # Email Configuration (for notifications)
    email_smtp_server: Optional[str] = Field(default=None, env="EMAIL_SMTP_SERVER")
    email_smtp_port: int = Field(default=587, env="EMAIL_SMTP_PORT")
    email_username: Optional[str] = Field(default=None, env="EMAIL_USERNAME")
    email_password: Optional[str] = Field(default=None, env="EMAIL_PASSWORD")
    email_from_address: Optional[str] = Field(default=None, env="EMAIL_FROM_ADDRESS")
    
    # Backup Configuration
    backup_enabled: bool = Field(default=False, env="BACKUP_ENABLED")
    backup_schedule: str = Field(default="0 2 * * *", env="BACKUP_SCHEDULE")  # Daily at 2 AM
    backup_retention_days: int = Field(default=30, env="BACKUP_RETENTION_DAYS")
    
    # Feature Flags
    enable_data_profiling: bool = Field(default=True, env="ENABLE_DATA_PROFILING")
    enable_real_time_processing: bool = Field(default=False, env="ENABLE_REAL_TIME_PROCESSING")
    enable_data_lineage: bool = Field(default=True, env="ENABLE_DATA_LINEAGE")
    enable_auto_scaling: bool = Field(default=False, env="ENABLE_AUTO_SCALING")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            self.data_raw_path,
            self.data_staging_path,
            self.data_processed_path,
            self.data_archive_path,
            "logs",
            self.airflow_home
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def get_database_config(self, db_type: str) -> dict:
        """Get database configuration for specified type"""
        if db_type.lower() == "postgresql":
            return {
                "host": self.postgres_host,
                "port": self.postgres_port,
                "database": self.postgres_db,
                "user": self.postgres_user,
                "password": self.postgres_password,
                "url": self.postgres_url,
                "async_url": self.postgres_async_url
            }
        elif db_type.lower() == "mongodb":
            return {
                "host": self.mongo_host,
                "port": self.mongo_port,
                "database": self.mongo_db,
                "user": self.mongo_user,
                "password": self.mongo_password,
                "url": self.mongo_url
            }
        elif db_type.lower() == "influxdb":
            return {
                "url": self.influx_url,
                "token": self.influx_token,
                "org": self.influx_org,
                "bucket": self.influx_bucket
            }
        elif db_type.lower() == "redis":
            return {
                "host": self.redis_host,
                "port": self.redis_port,
                "password": self.redis_password,
                "db": self.redis_db,
                "url": self.redis_url
            }
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def get_etl_config(self) -> dict:
        """Get ETL configuration"""
        return {
            "batch_size": self.etl_batch_size,
            "parallel_workers": self.etl_parallel_workers,
            "retry_attempts": self.etl_retry_attempts,
            "retry_delay": self.etl_retry_delay
        }
    
    def get_data_quality_config(self) -> dict:
        """Get data quality configuration"""
        return {
            "max_null_percentage": self.data_quality_max_null_percentage,
            "max_duplicate_percentage": self.data_quality_max_duplicate_percentage,
            "min_records": self.data_quality_min_records
        }
    
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.app_env.lower() == "production"
    
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.app_env.lower() == "development"

# Create global settings instance
settings = Settings()
