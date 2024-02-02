"""
MongoDB Database Connector for Multi-Source Data Lake
"""
import logging
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import asyncio
from datetime import datetime

from config.settings import settings

logger = logging.getLogger(__name__)

class MongoConnector:
    """MongoDB database connector with async support"""
    
    def __init__(self):
        self.async_client = None
        self.sync_client = None
        self.async_db = None
        self.sync_db = None
        self.connected = False
    
    async def connect(self):
        """Establish database connections"""
        try:
            # Async client
            self.async_client = AsyncIOMotorClient(
                settings.mongo_url,
                maxPoolSize=settings.mongo_max_pool_size,
                serverSelectionTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # Sync client for pandas operations
            self.sync_client = MongoClient(
                settings.mongo_url,
                maxPoolSize=settings.mongo_max_pool_size,
                serverSelectionTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            self.async_db = self.async_client[settings.mongo_db]
            self.sync_db = self.sync_client[settings.mongo_db]
            
            # Test connection
            await self.async_client.admin.command('ping')
            
            self.connected = True
            logger.info(f"Connected to MongoDB: {settings.mongo_host}:{settings.mongo_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def disconnect(self):
        """Close database connections"""
        if self.async_client:
            self.async_client.close()
        if self.sync_client:
            self.sync_client.close()
        self.connected = False
        logger.info("Disconnected from MongoDB")
    
    async def insert_one(self, collection_name: str, document: Dict[str, Any]) -> str:
        """Insert single document"""
        try:
            collection = self.async_db[collection_name]
            document['created_at'] = datetime.utcnow()
            result = await collection.insert_one(document)
            logger.info(f"Inserted document into {collection_name}: {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Failed to insert document into {collection_name}: {e}")
            raise
    
    async def insert_many(self, collection_name: str, documents: List[Dict[str, Any]]) -> List[str]:
        """Insert multiple documents"""
        try:
            collection = self.async_db[collection_name]
            # Add timestamps
            for doc in documents:
                doc['created_at'] = datetime.utcnow()
            
            result = await collection.insert_many(documents)
            logger.info(f"Inserted {len(documents)} documents into {collection_name}")
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            logger.error(f"Failed to insert documents into {collection_name}: {e}")
            raise
    
    async def find(self, collection_name: str, filter_dict: Dict = None, 
                   limit: Optional[int] = None, sort: Optional[List] = None) -> List[Dict]:
        """Find documents in collection"""
        try:
            collection = self.async_db[collection_name]
            cursor = collection.find(filter_dict or {})
            
            if sort:
                cursor = cursor.sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            
            documents = await cursor.to_list(length=limit)
            # Convert ObjectId to string for JSON serialization
            for doc in documents:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            return documents
        except Exception as e:
            logger.error(f"Failed to find documents in {collection_name}: {e}")
            raise
    
    async def find_one(self, collection_name: str, filter_dict: Dict) -> Optional[Dict]:
        """Find single document"""
        try:
            collection = self.async_db[collection_name]
            document = await collection.find_one(filter_dict)
            
            if document and '_id' in document:
                document['_id'] = str(document['_id'])
            
            return document
        except Exception as e:
            logger.error(f"Failed to find document in {collection_name}: {e}")
            raise
    
    async def update_one(self, collection_name: str, filter_dict: Dict, 
                        update_dict: Dict) -> Dict[str, Any]:
        """Update single document"""
        try:
            collection = self.async_db[collection_name]
            update_dict['updated_at'] = datetime.utcnow()
            
            result = await collection.update_one(
                filter_dict, 
                {"$set": update_dict}
            )
            
            return {
                "matched_count": result.matched_count,
                "modified_count": result.modified_count
            }
        except Exception as e:
            logger.error(f"Failed to update document in {collection_name}: {e}")
            raise
    
    async def delete_many(self, collection_name: str, filter_dict: Dict) -> int:
        """Delete multiple documents"""
        try:
            collection = self.async_db[collection_name]
            result = await collection.delete_many(filter_dict)
            logger.info(f"Deleted {result.deleted_count} documents from {collection_name}")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Failed to delete documents from {collection_name}: {e}")
            raise
    
    def collection_to_dataframe(self, collection_name: str, 
                               filter_dict: Dict = None, 
                               limit: Optional[int] = None) -> pd.DataFrame:
        """Convert MongoDB collection to pandas DataFrame"""
        try:
            collection = self.sync_db[collection_name]
            cursor = collection.find(filter_dict or {})
            
            if limit:
                cursor = cursor.limit(limit)
            
            documents = list(cursor)
            
            if not documents:
                return pd.DataFrame()
            
            # Convert ObjectId to string
            for doc in documents:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            return pd.DataFrame(documents)
        except Exception as e:
            logger.error(f"Failed to convert {collection_name} to DataFrame: {e}")
            raise
    
    async def dataframe_to_collection(self, df: pd.DataFrame, collection_name: str):
        """Insert DataFrame data into MongoDB collection"""
        try:
            documents = df.to_dict('records')
            # Add timestamps
            for doc in documents:
                doc['created_at'] = datetime.utcnow()
            
            collection = self.async_db[collection_name]
            result = await collection.insert_many(documents)
            logger.info(f"Inserted {len(documents)} documents into {collection_name}")
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            logger.error(f"Failed to insert DataFrame into {collection_name}: {e}")
            raise
    
    async def get_collections(self) -> List[str]:
        """Get list of all collections"""
        try:
            collections = await self.async_db.list_collection_names()
            return collections
        except Exception as e:
            logger.error(f"Failed to get collections: {e}")
            raise
    
    async def create_index(self, collection_name: str, index_spec: List, **kwargs):
        """Create index on collection"""
        try:
            collection = self.async_db[collection_name]
            result = await collection.create_index(index_spec, **kwargs)
            logger.info(f"Created index on {collection_name}: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to create index on {collection_name}: {e}")
            raise
    
    async def aggregate(self, collection_name: str, pipeline: List[Dict]) -> List[Dict]:
        """Execute aggregation pipeline"""
        try:
            collection = self.async_db[collection_name]
            cursor = collection.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
            # Convert ObjectId to string
            for result in results:
                if '_id' in result:
                    result['_id'] = str(result['_id'])
            
            return results
        except Exception as e:
            logger.error(f"Failed to execute aggregation on {collection_name}: {e}")
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """Check database health"""
        try:
            info = await self.async_client.server_info()
            return {
                "status": "healthy",
                "connected": self.connected,
                "server_info": {
                    "version": info.get("version"),
                    "uptime": info.get("uptime")
                }
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e)
            }