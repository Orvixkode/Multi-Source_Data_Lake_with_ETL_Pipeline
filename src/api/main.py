"""
Multi-Source Data Lake API - Main Application
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from contextlib import asynccontextmanager
import logging
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from datetime import datetime

from config.settings import settings
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.mongo_connector import MongoConnector
from src.connectors.influx_connector import InfluxConnector
from src.etl.extractors.base_extractors import PostgreSQLExtractor, MongoExtractor, InfluxExtractor, FileExtractor, APIExtractor
from src.etl.transformers.data_transformers import DataCleaningTransformer, DataValidationTransformer, TransformationPipeline
from src.etl.loaders.base_loaders import PostgreSQLLoader, MongoLoader, InfluxLoader, LoaderManager

# Configure logging
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting Multi-Source Data Lake API")
    logger.info(f"Environment: {settings.app_env}")
    logger.info(f"Debug mode: {settings.app_debug}")
    yield
    logger.info("Shutting down Multi-Source Data Lake API")

# Create FastAPI app
app = FastAPI(
    title="Multi-Source Data Lake API",
    description="A comprehensive data lake system with ETL pipeline capabilities",
    version="1.0.0",
    lifespan=lifespan,
    debug=settings.app_debug
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Multi-Source Data Lake API",
        "version": "1.0.0",
        "status": "running",
        "environment": settings.app_env
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": "2025-11-24T00:00:00Z",
        "version": "1.0.0"
    }

@app.get("/api/v1/status")
async def api_status():
    """API status endpoint with detailed information"""
    return {
        "api": {
            "status": "running",
            "version": "1.0.0",
            "environment": settings.app_env,
            "debug": settings.app_debug
        },
        "databases": {
            "postgres": {
                "host": settings.postgres_host,
                "port": settings.postgres_port,
                "database": settings.postgres_db
            },
            "mongodb": {
                "host": settings.mongo_host,
                "port": settings.mongo_port,
                "database": settings.mongo_db
            },
            "influxdb": {
                "url": settings.influx_url,
                "org": settings.influx_org,
                "bucket": settings.influx_bucket
            },
            "redis": {
                "host": settings.redis_host,
                "port": settings.redis_port
            }
        },
        "protocols": {
            "opcua": {
                "endpoint": settings.opcua_endpoint
            },
            "mqtt": {
                "broker": settings.mqtt_broker,
                "port": settings.mqtt_port,
                "topics": settings.mqtt_topics_list
            }
        }
    }

@app.get("/api/v1/config")
async def get_configuration():
    """Get current configuration (non-sensitive data only)"""
    return {
        "app_name": settings.app_name,
        "environment": settings.app_env,
        "data_paths": {
            "raw": settings.data_raw_path,
            "staging": settings.data_staging_path,
            "processed": settings.data_processed_path
        },
        "etl_config": {
            "batch_size": settings.etl_batch_size,
            "parallel_workers": settings.etl_parallel_workers,
            "retry_attempts": settings.etl_retry_attempts
        }
    }

# Pydantic models for API requests
class QueryRequest(BaseModel):
    table: Optional[str] = None
    collection: Optional[str] = None
    measurement: Optional[str] = None
    filter: Optional[Dict[str, Any]] = None
    limit: Optional[int] = 100
    
class ETLJobRequest(BaseModel):
    source_type: str
    source_config: Dict[str, Any]
    target_type: str
    target_config: Dict[str, Any]
    transformations: Optional[List[str]] = []

class DataUploadRequest(BaseModel):
    data: List[Dict[str, Any]]
    target_type: str
    target_config: Dict[str, Any]

# Database query endpoints
@app.get("/api/v1/query/postgres")
async def query_postgres(table: str, limit: int = 100, where: Optional[str] = None):
    """Query PostgreSQL database"""
    try:
        connector = PostgresConnector()
        await connector.connect()
        
        query = f"SELECT * FROM {table}"
        if where:
            query += f" WHERE {where}"
        query += f" LIMIT {limit}"
        
        results = await connector.execute_query(query)
        await connector.disconnect()
        
        return {
            "status": "success",
            "data": results,
            "count": len(results),
            "table": table
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PostgreSQL query failed: {str(e)}")

@app.get("/api/v1/query/mongodb")
async def query_mongodb(collection: str, limit: int = 100, filter: Optional[str] = None):
    """Query MongoDB collection"""
    try:
        import json
        connector = MongoConnector()
        await connector.connect()
        
        filter_dict = json.loads(filter) if filter else {}
        results = await connector.find(collection, filter_dict, limit)
        await connector.disconnect()
        
        return {
            "status": "success",
            "data": results,
            "count": len(results),
            "collection": collection
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MongoDB query failed: {str(e)}")

@app.get("/api/v1/query/influxdb")
async def query_influxdb(measurement: str, start: str = "-1h", stop: str = "now()", fields: Optional[str] = None):
    """Query InfluxDB time-series data"""
    try:
        connector = InfluxConnector()
        await connector.connect()
        
        field_list = fields.split(',') if fields else None
        df = await connector.query_range(measurement, start, stop, field_list)
        
        # Convert DataFrame to records
        results = df.to_dict('records') if not df.empty else []
        await connector.disconnect()
        
        return {
            "status": "success",
            "data": results,
            "count": len(results),
            "measurement": measurement,
            "time_range": f"{start} to {stop}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"InfluxDB query failed: {str(e)}")

# ETL Pipeline endpoints
@app.post("/api/v1/etl/run")
async def run_etl_job(job_request: ETLJobRequest):
    """Run an ETL job"""
    try:
        # Initialize extractor
        if job_request.source_type == "postgres":
            extractor = PostgreSQLExtractor()
        elif job_request.source_type == "mongodb":
            extractor = MongoExtractor()
        elif job_request.source_type == "influxdb":
            extractor = InfluxExtractor()
        elif job_request.source_type == "file":
            extractor = FileExtractor()
        elif job_request.source_type == "api":
            extractor = APIExtractor()
        else:
            raise ValueError(f"Unsupported source type: {job_request.source_type}")
        
        # Initialize transformers
        transformers = []
        if "cleaning" in job_request.transformations:
            transformers.append(DataCleaningTransformer())
        if "validation" in job_request.transformations:
            transformers.append(DataValidationTransformer())
        
        pipeline = TransformationPipeline(transformers)
        
        # Initialize loader
        loader_manager = LoaderManager()
        
        # Extract data
        extracted_data = []
        async for record in extractor.extract(**job_request.source_config):
            # Transform data
            transformed_record = pipeline.transform(record)
            extracted_data.append(transformed_record)
        
        # Load data
        routing_config = {
            "target": {
                "type": job_request.target_type,
                "params": job_request.target_config
            }
        }
        
        load_results = await loader_manager.route_and_load(extracted_data, routing_config)
        
        return {
            "status": "success",
            "job_id": f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "extracted_count": len(extracted_data),
            "transformation_stats": pipeline.get_stats(),
            "load_results": load_results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ETL job failed: {str(e)}")

@app.post("/api/v1/data/upload")
async def upload_data(upload_request: DataUploadRequest):
    """Upload data directly to a target system"""
    try:
        loader_manager = LoaderManager()
        
        routing_config = {
            "upload_target": {
                "type": upload_request.target_type,
                "params": upload_request.target_config
            }
        }
        
        results = await loader_manager.route_and_load(upload_request.data, routing_config)
        
        return {
            "status": "success",
            "uploaded_count": len(upload_request.data),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data upload failed: {str(e)}")

# Database health and metadata endpoints
@app.get("/api/v1/databases/health")
async def check_database_health():
    """Check health of all database connections"""
    health_status = {}
    
    try:
        # PostgreSQL health
        pg_connector = PostgresConnector()
        pg_health = await pg_connector.health_check()
        health_status["postgresql"] = pg_health
    except Exception as e:
        health_status["postgresql"] = {"status": "error", "error": str(e)}
    
    try:
        # MongoDB health
        mongo_connector = MongoConnector()
        mongo_health = await mongo_connector.health_check()
        health_status["mongodb"] = mongo_health
    except Exception as e:
        health_status["mongodb"] = {"status": "error", "error": str(e)}
    
    try:
        # InfluxDB health
        influx_connector = InfluxConnector()
        influx_health = await influx_connector.health_check()
        health_status["influxdb"] = influx_health
    except Exception as e:
        health_status["influxdb"] = {"status": "error", "error": str(e)}
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "overall_status": "healthy" if all(db.get("status") == "healthy" for db in health_status.values()) else "degraded",
        "databases": health_status
    }

@app.get("/api/v1/metadata/tables")
async def get_postgres_tables():
    """Get list of PostgreSQL tables"""
    try:
        connector = PostgresConnector()
        await connector.connect()
        tables = await connector.get_tables()
        await connector.disconnect()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get tables: {str(e)}")

@app.get("/api/v1/metadata/collections")
async def get_mongo_collections():
    """Get list of MongoDB collections"""
    try:
        connector = MongoConnector()
        await connector.connect()
        collections = await connector.get_collections()
        await connector.disconnect()
        return {"collections": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get collections: {str(e)}")

@app.get("/api/v1/metadata/measurements")
async def get_influx_measurements():
    """Get list of InfluxDB measurements"""
    try:
        connector = InfluxConnector()
        await connector.connect()
        measurements = await connector.get_measurements()
        await connector.disconnect()
        return {"measurements": measurements}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get measurements: {str(e)}")

@app.get("/api/v1/stats/etl")
async def get_etl_statistics():
    """Get ETL pipeline statistics"""
    try:
        loader_manager = LoaderManager()
        stats = loader_manager.get_statistics()
        return {
            "status": "success",
            "statistics": stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get ETL stats: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.app_debug,
        workers=1 if settings.app_debug else settings.api_workers
    )