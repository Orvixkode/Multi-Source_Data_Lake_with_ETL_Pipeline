"""
Data Extractors for Multi-Source Data Lake ETL Pipeline
"""
import logging
import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Iterator
import pandas as pd
from datetime import datetime, timedelta
import json
import csv
import requests
from pathlib import Path

from config.settings import settings
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.mongo_connector import MongoConnector
from src.connectors.influx_connector import InfluxConnector

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Abstract base class for data extractors"""
    
    def __init__(self, name: str):
        self.name = name
        self.extracted_count = 0
        self.last_extraction = None
    
    @abstractmethod
    async def extract(self, **kwargs) -> Iterator[Dict[str, Any]]:
        """Extract data and yield records"""
        pass
    
    @abstractmethod
    async def validate_source(self) -> bool:
        """Validate if source is accessible"""
        pass

class PostgreSQLExtractor(BaseExtractor):
    """Extract data from PostgreSQL databases"""
    
    def __init__(self):
        super().__init__("PostgreSQL Extractor")
        self.connector = PostgresConnector()
    
    async def extract(self, table_name: str, batch_size: int = None, 
                     where_clause: str = None, **kwargs) -> Iterator[Dict[str, Any]]:
        """Extract data from PostgreSQL table"""
        try:
            await self.connector.connect()
            
            batch_size = batch_size or settings.etl_batch_size
            offset = 0
            
            while True:
                # Build query
                query = f"SELECT * FROM {table_name}"
                if where_clause:
                    query += f" WHERE {where_clause}"
                query += f" LIMIT {batch_size} OFFSET {offset}"
                
                results = await self.connector.execute_query(query)
                
                if not results:
                    break
                
                for record in results:
                    self.extracted_count += 1
                    yield record
                
                offset += batch_size
                
                if len(results) < batch_size:
                    break
            
            self.last_extraction = datetime.utcnow()
            logger.info(f"PostgreSQL extraction completed: {self.extracted_count} records")
            
        except Exception as e:
            logger.error(f"PostgreSQL extraction failed: {e}")
            raise
        finally:
            await self.connector.disconnect()
    
    async def validate_source(self) -> bool:
        """Validate PostgreSQL connection"""
        try:
            await self.connector.connect()
            health = await self.connector.health_check()
            await self.connector.disconnect()
            return health['status'] == 'healthy'
        except:
            return False

class MongoExtractor(BaseExtractor):
    """Extract data from MongoDB collections"""
    
    def __init__(self):
        super().__init__("MongoDB Extractor")
        self.connector = MongoConnector()
    
    async def extract(self, collection_name: str, filter_dict: Dict = None,
                     batch_size: int = None, **kwargs) -> Iterator[Dict[str, Any]]:
        """Extract data from MongoDB collection"""
        try:
            await self.connector.connect()
            
            batch_size = batch_size or settings.etl_batch_size
            skip = 0
            
            while True:
                documents = await self.connector.find(
                    collection_name, 
                    filter_dict, 
                    limit=batch_size
                )
                
                if not documents:
                    break
                
                for document in documents:
                    self.extracted_count += 1
                    yield document
                
                skip += batch_size
                
                if len(documents) < batch_size:
                    break
            
            self.last_extraction = datetime.utcnow()
            logger.info(f"MongoDB extraction completed: {self.extracted_count} records")
            
        except Exception as e:
            logger.error(f"MongoDB extraction failed: {e}")
            raise
        finally:
            await self.connector.disconnect()
    
    async def validate_source(self) -> bool:
        """Validate MongoDB connection"""
        try:
            await self.connector.connect()
            health = await self.connector.health_check()
            await self.connector.disconnect()
            return health['status'] == 'healthy'
        except:
            return False

class InfluxExtractor(BaseExtractor):
    """Extract time-series data from InfluxDB"""
    
    def __init__(self):
        super().__init__("InfluxDB Extractor")
        self.connector = InfluxConnector()
    
    async def extract(self, measurement: str, start: str = "-1h", 
                     stop: str = "now()", **kwargs) -> Iterator[Dict[str, Any]]:
        """Extract data from InfluxDB measurement"""
        try:
            await self.connector.connect()
            
            df = await self.connector.query_range(measurement, start, stop)
            
            for _, row in df.iterrows():
                record = row.to_dict()
                self.extracted_count += 1
                yield record
            
            self.last_extraction = datetime.utcnow()
            logger.info(f"InfluxDB extraction completed: {self.extracted_count} records")
            
        except Exception as e:
            logger.error(f"InfluxDB extraction failed: {e}")
            raise
        finally:
            await self.connector.disconnect()
    
    async def validate_source(self) -> bool:
        """Validate InfluxDB connection"""
        try:
            await self.connector.connect()
            health = await self.connector.health_check()
            await self.connector.disconnect()
            return health['status'] == 'healthy'
        except:
            return False

class FileExtractor(BaseExtractor):
    """Extract data from files (CSV, JSON, Parquet)"""
    
    def __init__(self):
        super().__init__("File Extractor")
    
    async def extract(self, file_path: str, file_type: str = None, 
                     **kwargs) -> Iterator[Dict[str, Any]]:
        """Extract data from files"""
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            file_type = file_type or file_path.suffix.lower()
            
            if file_type in ['.csv']:
                df = pd.read_csv(file_path)
            elif file_type in ['.json', '.jsonl']:
                if file_type == '.jsonl':
                    df = pd.read_json(file_path, lines=True)
                else:
                    df = pd.read_json(file_path)
            elif file_type in ['.parquet']:
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            for _, row in df.iterrows():
                record = row.to_dict()
                # Convert NaN to None for JSON serialization
                record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
                self.extracted_count += 1
                yield record
            
            self.last_extraction = datetime.utcnow()
            logger.info(f"File extraction completed: {self.extracted_count} records from {file_path}")
            
        except Exception as e:
            logger.error(f"File extraction failed: {e}")
            raise
    
    async def validate_source(self, file_path: str) -> bool:
        """Validate file accessibility"""
        try:
            return Path(file_path).exists()
        except:
            return False

class APIExtractor(BaseExtractor):
    """Extract data from REST APIs"""
    
    def __init__(self):
        super().__init__("API Extractor")
    
    async def extract(self, url: str, headers: Dict = None, 
                     params: Dict = None, **kwargs) -> Iterator[Dict[str, Any]]:
        """Extract data from REST API"""
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Try to find data in common response patterns
                records = (data.get('data') or 
                          data.get('results') or 
                          data.get('items') or 
                          [data])
            else:
                records = [data]
            
            for record in records:
                self.extracted_count += 1
                yield record
            
            self.last_extraction = datetime.utcnow()
            logger.info(f"API extraction completed: {self.extracted_count} records from {url}")
            
        except Exception as e:
            logger.error(f"API extraction failed: {e}")
            raise
    
    async def validate_source(self, url: str, headers: Dict = None) -> bool:
        """Validate API accessibility"""
        try:
            response = requests.head(url, headers=headers, timeout=10)
            return response.status_code < 400
        except:
            return False