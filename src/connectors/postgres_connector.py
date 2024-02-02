"""
PostgreSQL Database Connector for Multi-Source Data Lake
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager

from config.settings import settings

logger = logging.getLogger(__name__)

class PostgresConnector:
    """PostgreSQL database connector with async support"""
    
    def __init__(self):
        self.sync_engine = None
        self.async_engine = None
        self.async_session_factory = None
        self.connected = False
    
    async def connect(self):
        """Establish database connections"""
        try:
            # Sync engine for pandas operations
            self.sync_engine = create_engine(
                settings.postgres_url,
                pool_size=settings.postgres_max_connections,
                max_overflow=20,
                pool_pre_ping=True
            )
            
            # Async engine for async operations
            self.async_engine = create_async_engine(
                settings.postgres_async_url,
                pool_size=settings.postgres_max_connections,
                max_overflow=20,
                pool_pre_ping=True
            )
            
            self.async_session_factory = sessionmaker(
                bind=self.async_engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Test connection
            async with self.async_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            
            self.connected = True
            logger.info(f"Connected to PostgreSQL: {settings.postgres_host}:{settings.postgres_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    async def disconnect(self):
        """Close database connections"""
        if self.async_engine:
            await self.async_engine.dispose()
        if self.sync_engine:
            self.sync_engine.dispose()
        self.connected = False
        logger.info("Disconnected from PostgreSQL")
    
    @asynccontextmanager
    async def get_session(self):
        """Get async database session"""
        if not self.connected:
            await self.connect()
        
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute SQL query and return results"""
        try:
            async with self.async_engine.connect() as conn:
                result = await conn.execute(text(query), params or {})
                columns = result.keys()
                rows = result.fetchall()
                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def read_table_to_dataframe(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Read table data into pandas DataFrame"""
        try:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
            
            return pd.read_sql(query, self.sync_engine)
        except Exception as e:
            logger.error(f"Failed to read table {table_name}: {e}")
            raise
    
    async def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """Insert DataFrame data into PostgreSQL table"""
        try:
            df.to_sql(
                table_name, 
                self.sync_engine, 
                if_exists=if_exists, 
                index=False,
                method='multi',
                chunksize=settings.etl_batch_size
            )
            logger.info(f"Inserted {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {e}")
            raise
    
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = :table_name
        ORDER BY ordinal_position
        """
        return await self.execute_query(query, {"table_name": table_name})
    
    async def get_tables(self) -> List[str]:
        """Get list of all tables in database"""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        """
        results = await self.execute_query(query)
        return [row['table_name'] for row in results]
    
    async def health_check(self) -> Dict[str, Any]:
        """Check database health"""
        try:
            result = await self.execute_query("SELECT version(), current_database(), current_user")
            return {
                "status": "healthy",
                "connected": self.connected,
                "database_info": result[0] if result else None
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e)
            }