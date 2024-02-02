"""
Data Loaders for Multi-Source Data Lake ETL Pipeline
"""
import logging
import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime
import json
from pathlib import Path

from config.settings import settings
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.mongo_connector import MongoConnector
from src.connectors.influx_connector import InfluxConnector

logger = logging.getLogger(__name__)

class BaseLoader(ABC):
    """Abstract base class for data loaders"""
    
    def __init__(self, name: str):
        self.name = name
        self.loaded_count = 0
        self.failed_count = 0
        self.last_load = None
    
    @abstractmethod
    async def load(self, data: List[Dict[str, Any]], **kwargs) -> bool:
        """Load data to destination"""
        pass
    
    @abstractmethod
    async def validate_destination(self) -> bool:
        """Validate if destination is accessible"""
        pass

class PostgreSQLLoader(BaseLoader):
    """Load data into PostgreSQL database"""
    
    def __init__(self):
        super().__init__("PostgreSQL Loader")
        self.connector = PostgresConnector()
    
    async def load(self, data: List[Dict[str, Any]], table_name: str, 
                  if_exists: str = 'append', **kwargs) -> bool:
        """Load data into PostgreSQL table"""
        try:
            if not data:
                return True
            
            await self.connector.connect()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Insert data
            await self.connector.insert_dataframe(df, table_name, if_exists)
            
            self.loaded_count += len(data)
            self.last_load = datetime.utcnow()
            
            logger.info(f"Loaded {len(data)} records to PostgreSQL table {table_name}")
            return True
            
        except Exception as e:
            self.failed_count += len(data)
            logger.error(f"Failed to load data to PostgreSQL: {e}")
            return False
        finally:
            await self.connector.disconnect()
    
    async def load_batch(self, data_batches: List[List[Dict[str, Any]]], 
                        table_name: str, **kwargs) -> Dict[str, Any]:
        """Load multiple batches"""
        total_loaded = 0
        total_failed = 0
        
        await self.connector.connect()
        
        try:
            for batch in data_batches:
                try:
                    df = pd.DataFrame(batch)
                    await self.connector.insert_dataframe(df, table_name)
                    total_loaded += len(batch)
                except Exception as e:
                    total_failed += len(batch)
                    logger.error(f"Batch load failed: {e}")
        finally:
            await self.connector.disconnect()
        
        return {
            'total_loaded': total_loaded,
            'total_failed': total_failed,
            'batches_processed': len(data_batches)
        }
    
    async def validate_destination(self, table_name: str = None) -> bool:
        """Validate PostgreSQL connection and table"""
        try:
            await self.connector.connect()
            health = await self.connector.health_check()
            
            if table_name:
                tables = await self.connector.get_tables()
                table_exists = table_name in tables
                await self.connector.disconnect()
                return health['status'] == 'healthy' and table_exists
            
            await self.connector.disconnect()
            return health['status'] == 'healthy'
        except:
            return False

class MongoLoader(BaseLoader):
    """Load data into MongoDB collections"""
    
    def __init__(self):
        super().__init__("MongoDB Loader")
        self.connector = MongoConnector()
    
    async def load(self, data: List[Dict[str, Any]], collection_name: str, 
                  **kwargs) -> bool:
        """Load data into MongoDB collection"""
        try:
            if not data:
                return True
            
            await self.connector.connect()
            
            # Insert documents
            result = await self.connector.insert_many(collection_name, data)
            
            self.loaded_count += len(data)
            self.last_load = datetime.utcnow()
            
            logger.info(f"Loaded {len(data)} documents to MongoDB collection {collection_name}")
            return True
            
        except Exception as e:
            self.failed_count += len(data)
            logger.error(f"Failed to load data to MongoDB: {e}")
            return False
        finally:
            await self.connector.disconnect()
    
    async def load_with_upsert(self, data: List[Dict[str, Any]], 
                              collection_name: str, key_field: str = '_id', 
                              **kwargs) -> Dict[str, Any]:
        """Load data with upsert functionality"""
        inserted_count = 0
        updated_count = 0
        
        await self.connector.connect()
        
        try:
            for record in data:
                if key_field in record:
                    # Try to update existing record
                    filter_dict = {key_field: record[key_field]}
                    existing = await self.connector.find_one(collection_name, filter_dict)
                    
                    if existing:
                        await self.connector.update_one(collection_name, filter_dict, record)
                        updated_count += 1
                    else:
                        await self.connector.insert_one(collection_name, record)
                        inserted_count += 1
                else:
                    await self.connector.insert_one(collection_name, record)
                    inserted_count += 1
        finally:
            await self.connector.disconnect()
        
        return {
            'inserted_count': inserted_count,
            'updated_count': updated_count,
            'total_processed': len(data)
        }
    
    async def validate_destination(self, collection_name: str = None) -> bool:
        """Validate MongoDB connection and collection"""
        try:
            await self.connector.connect()
            health = await self.connector.health_check()
            
            if collection_name:
                collections = await self.connector.get_collections()
                collection_exists = collection_name in collections
                await self.connector.disconnect()
                return health['status'] == 'healthy' and collection_exists
            
            await self.connector.disconnect()
            return health['status'] == 'healthy'
        except:
            return False

class InfluxLoader(BaseLoader):
    """Load time-series data into InfluxDB"""
    
    def __init__(self):
        super().__init__("InfluxDB Loader")
        self.connector = InfluxConnector()
    
    async def load(self, data: List[Dict[str, Any]], measurement: str,
                  time_field: str = 'time', tag_fields: List[str] = None,
                  field_fields: List[str] = None, **kwargs) -> bool:
        """Load data into InfluxDB measurement"""
        try:
            if not data:
                return True
            
            await self.connector.connect()
            
            tag_fields = tag_fields or []
            field_fields = field_fields or []
            
            # Prepare points for InfluxDB
            points = []
            for record in data:
                point_data = {
                    'measurement': measurement,
                    'tags': {field: str(record.get(field, '')) for field in tag_fields if field in record},
                    'fields': {field: record[field] for field in field_fields if field in record and record[field] is not None},
                }
                
                # Add timestamp if available
                if time_field in record:
                    point_data['timestamp'] = record[time_field]
                
                # If no specific field_fields provided, use all numeric fields as fields
                if not field_fields:
                    point_data['fields'] = {
                        k: v for k, v in record.items() 
                        if k not in tag_fields + [time_field] and isinstance(v, (int, float))
                    }
                
                points.append(point_data)
            
            # Write points
            await self.connector.write_points(points)
            
            self.loaded_count += len(data)
            self.last_load = datetime.utcnow()
            
            logger.info(f"Loaded {len(data)} points to InfluxDB measurement {measurement}")
            return True
            
        except Exception as e:
            self.failed_count += len(data)
            logger.error(f"Failed to load data to InfluxDB: {e}")
            return False
        finally:
            await self.connector.disconnect()
    
    async def validate_destination(self, measurement: str = None) -> bool:
        """Validate InfluxDB connection and measurement"""
        try:
            await self.connector.connect()
            health = await self.connector.health_check()
            
            if measurement:
                measurements = await self.connector.get_measurements()
                measurement_exists = measurement in measurements
                await self.connector.disconnect()
                return health['status'] == 'healthy' and measurement_exists
            
            await self.connector.disconnect()
            return health['status'] == 'healthy'
        except:
            return False

class FileLoader(BaseLoader):
    """Load data to files (CSV, JSON, Parquet)"""
    
    def __init__(self):
        super().__init__("File Loader")
    
    async def load(self, data: List[Dict[str, Any]], file_path: str,
                  file_format: str = 'json', **kwargs) -> bool:
        """Load data to file"""
        try:
            if not data:
                return True
            
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            if file_format.lower() == 'json':
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            
            elif file_format.lower() == 'csv':
                df = pd.DataFrame(data)
                df.to_csv(file_path, index=False)
            
            elif file_format.lower() == 'parquet':
                df = pd.DataFrame(data)
                df.to_parquet(file_path, index=False)
            
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            self.loaded_count += len(data)
            self.last_load = datetime.utcnow()
            
            logger.info(f"Loaded {len(data)} records to file {file_path}")
            return True
            
        except Exception as e:
            self.failed_count += len(data)
            logger.error(f"Failed to load data to file: {e}")
            return False
    
    async def validate_destination(self, file_path: str) -> bool:
        """Validate file path is writable"""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            return file_path.parent.exists() and file_path.parent.is_dir()
        except:
            return False

class MultiTargetLoader(BaseLoader):
    """Load data to multiple destinations simultaneously"""
    
    def __init__(self, loaders: List[BaseLoader]):
        super().__init__("Multi-Target Loader")
        self.loaders = loaders
    
    async def load(self, data: List[Dict[str, Any]], targets: List[Dict[str, Any]], 
                  **kwargs) -> Dict[str, Any]:
        """Load data to multiple targets"""
        results = {}
        
        # Execute loads in parallel
        tasks = []
        for i, (loader, target) in enumerate(zip(self.loaders, targets)):
            task = asyncio.create_task(
                loader.load(data, **target),
                name=f"{loader.name}_{i}"
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        completed_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful_loads = 0
        for i, (loader, result) in enumerate(zip(self.loaders, completed_results)):
            if isinstance(result, Exception):
                results[loader.name] = {
                    'success': False,
                    'error': str(result)
                }
            else:
                results[loader.name] = {
                    'success': result,
                    'loaded_count': loader.loaded_count
                }
                if result:
                    successful_loads += 1
        
        self.loaded_count = sum(loader.loaded_count for loader in self.loaders)
        self.last_load = datetime.utcnow()
        
        return {
            'total_targets': len(self.loaders),
            'successful_loads': successful_loads,
            'results': results
        }
    
    async def validate_destination(self) -> bool:
        """Validate all destinations"""
        validation_tasks = [loader.validate_destination() for loader in self.loaders]
        results = await asyncio.gather(*validation_tasks, return_exceptions=True)
        
        return all(result is True for result in results if not isinstance(result, Exception))

class LoaderManager:
    """Manage multiple loaders and route data appropriately"""
    
    def __init__(self):
        self.loaders = {
            'postgres': PostgreSQLLoader(),
            'mongodb': MongoLoader(),
            'influxdb': InfluxLoader(),
            'file': FileLoader()
        }
        self.total_loaded = 0
        self.load_history = []
    
    async def route_and_load(self, data: List[Dict[str, Any]], 
                           routing_config: Dict[str, Any]) -> Dict[str, Any]:
        """Route data to appropriate loaders based on configuration"""
        results = {}
        
        for destination, config in routing_config.items():
            loader_type = config.get('type')
            
            if loader_type not in self.loaders:
                results[destination] = {
                    'success': False,
                    'error': f'Unknown loader type: {loader_type}'
                }
                continue
            
            loader = self.loaders[loader_type]
            
            try:
                success = await loader.load(data, **config.get('params', {}))
                results[destination] = {
                    'success': success,
                    'loaded_count': len(data) if success else 0
                }
                
                if success:
                    self.total_loaded += len(data)
                
            except Exception as e:
                results[destination] = {
                    'success': False,
                    'error': str(e)
                }
        
        # Record load history
        self.load_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'record_count': len(data),
            'destinations': list(routing_config.keys()),
            'results': results
        })
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get loading statistics"""
        return {
            'total_loaded': self.total_loaded,
            'loader_stats': {
                name: {
                    'loaded_count': loader.loaded_count,
                    'failed_count': loader.failed_count,
                    'last_load': loader.last_load.isoformat() if loader.last_load else None
                }
                for name, loader in self.loaders.items()
            },
            'recent_loads': self.load_history[-10:]  # Last 10 loads
        }