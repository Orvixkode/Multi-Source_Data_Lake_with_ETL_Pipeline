"""
InfluxDB Connector for Time-Series Data in Multi-Source Data Lake
"""
import logging
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import asyncio
from datetime import datetime, timezone

from config.settings import settings

logger = logging.getLogger(__name__)

class InfluxConnector:
    """InfluxDB connector for time-series data"""
    
    def __init__(self):
        self.client = None
        self.write_api = None
        self.query_api = None
        self.connected = False
    
    async def connect(self):
        """Establish InfluxDB connection"""
        try:
            self.client = InfluxDBClient(
                url=settings.influx_url,
                token=settings.influx_token,
                org=settings.influx_org
            )
            
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()
            
            # Test connection
            ping_result = self.client.ping()
            if ping_result:
                self.connected = True
                logger.info(f"Connected to InfluxDB: {settings.influx_url}")
            else:
                raise Exception("InfluxDB ping failed")
                
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            raise
    
    async def disconnect(self):
        """Close InfluxDB connection"""
        if self.client:
            self.client.close()
        self.connected = False
        logger.info("Disconnected from InfluxDB")
    
    async def write_point(self, measurement: str, tags: Dict[str, str], 
                         fields: Dict[str, Union[float, int, str]], 
                         timestamp: Optional[datetime] = None):
        """Write single data point"""
        try:
            point = Point(measurement)
            
            # Add tags
            for tag_key, tag_value in tags.items():
                point = point.tag(tag_key, tag_value)
            
            # Add fields
            for field_key, field_value in fields.items():
                point = point.field(field_key, field_value)
            
            # Set timestamp
            if timestamp:
                point = point.time(timestamp)
            
            self.write_api.write(bucket=settings.influx_bucket, record=point)
            logger.debug(f"Written point to {measurement}")
            
        except Exception as e:
            logger.error(f"Failed to write point to {measurement}: {e}")
            raise
    
    async def write_points(self, points: List[Dict[str, Any]]):
        """Write multiple data points"""
        try:
            influx_points = []
            
            for point_data in points:
                point = Point(point_data['measurement'])
                
                # Add tags
                if 'tags' in point_data:
                    for tag_key, tag_value in point_data['tags'].items():
                        point = point.tag(tag_key, tag_value)
                
                # Add fields
                if 'fields' in point_data:
                    for field_key, field_value in point_data['fields'].items():
                        point = point.field(field_key, field_value)
                
                # Set timestamp
                if 'timestamp' in point_data:
                    point = point.time(point_data['timestamp'])
                
                influx_points.append(point)
            
            self.write_api.write(bucket=settings.influx_bucket, record=influx_points)
            logger.info(f"Written {len(points)} points to InfluxDB")
            
        except Exception as e:
            logger.error(f"Failed to write points to InfluxDB: {e}")
            raise
    
    async def query_data(self, flux_query: str) -> List[Dict[str, Any]]:
        """Execute Flux query and return results"""
        try:
            tables = self.query_api.query(flux_query)
            results = []
            
            for table in tables:
                for record in table.records:
                    result = {
                        'measurement': record.get_measurement(),
                        'field': record.get_field(),
                        'value': record.get_value(),
                        'time': record.get_time(),
                    }
                    # Add tags
                    for key, value in record.values.items():
                        if key.startswith('_'):
                            continue
                        if key not in ['result', 'table', 'measurement', 'field', 'value', 'time']:
                            result[key] = value
                    
                    results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    async def query_range(self, measurement: str, start: str = "-1h", 
                         stop: str = "now()", fields: Optional[List[str]] = None,
                         tags: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """Query time range and return as DataFrame"""
        try:
            # Build Flux query
            query = f'''
                from(bucket: "{settings.influx_bucket}")
                |> range(start: {start}, stop: {stop})
                |> filter(fn: (r) => r._measurement == "{measurement}")
            '''
            
            # Add field filters
            if fields:
                field_filter = ' or '.join([f'r._field == "{field}"' for field in fields])
                query += f'|> filter(fn: (r) => {field_filter})'
            
            # Add tag filters
            if tags:
                for tag_key, tag_value in tags.items():
                    query += f'|> filter(fn: (r) => r.{tag_key} == "{tag_value}")'
            
            # Execute query
            tables = self.query_api.query(query)
            
            # Convert to DataFrame
            data = []
            for table in tables:
                for record in table.records:
                    row = {
                        'time': record.get_time(),
                        'measurement': record.get_measurement(),
                        'field': record.get_field(),
                        'value': record.get_value()
                    }
                    # Add tags as columns
                    for key, value in record.values.items():
                        if key.startswith('_') or key in ['result', 'table']:
                            continue
                        if key not in ['measurement', 'field', 'value', 'time']:
                            row[key] = value
                    data.append(row)
            
            return pd.DataFrame(data)
            
        except Exception as e:
            logger.error(f"Failed to query range for {measurement}: {e}")
            raise
    
    async def dataframe_to_influx(self, df: pd.DataFrame, measurement: str,
                                 time_column: str = 'time', 
                                 tag_columns: Optional[List[str]] = None,
                                 field_columns: Optional[List[str]] = None):
        """Insert DataFrame data into InfluxDB"""
        try:
            tag_columns = tag_columns or []
            field_columns = field_columns or [col for col in df.columns 
                                            if col not in [time_column] + tag_columns]
            
            points = []
            
            for _, row in df.iterrows():
                point = Point(measurement)
                
                # Add timestamp
                if time_column in row:
                    point = point.time(row[time_column])
                
                # Add tags
                for tag_col in tag_columns:
                    if tag_col in row and pd.notna(row[tag_col]):
                        point = point.tag(tag_col, str(row[tag_col]))
                
                # Add fields
                for field_col in field_columns:
                    if field_col in row and pd.notna(row[field_col]):
                        point = point.field(field_col, row[field_col])
                
                points.append(point)
            
            self.write_api.write(bucket=settings.influx_bucket, record=points)
            logger.info(f"Inserted {len(points)} points from DataFrame to {measurement}")
            
        except Exception as e:
            logger.error(f"Failed to insert DataFrame into {measurement}: {e}")
            raise
    
    async def get_measurements(self) -> List[str]:
        """Get list of all measurements"""
        try:
            query = f'''
                import "influxdata/influxdb/schema"
                schema.measurements(bucket: "{settings.influx_bucket}")
            '''
            
            tables = self.query_api.query(query)
            measurements = []
            
            for table in tables:
                for record in table.records:
                    measurements.append(record.get_value())
            
            return measurements
            
        except Exception as e:
            logger.error(f"Failed to get measurements: {e}")
            raise
    
    async def get_fields(self, measurement: str) -> List[str]:
        """Get list of fields for a measurement"""
        try:
            query = f'''
                import "influxdata/influxdb/schema"
                schema.measurementFieldKeys(
                    bucket: "{settings.influx_bucket}",
                    measurement: "{measurement}"
                )
            '''
            
            tables = self.query_api.query(query)
            fields = []
            
            for table in tables:
                for record in table.records:
                    fields.append(record.get_value())
            
            return fields
            
        except Exception as e:
            logger.error(f"Failed to get fields for {measurement}: {e}")
            raise
    
    async def delete_measurement(self, measurement: str, start: str, stop: str):
        """Delete data from measurement in time range"""
        try:
            delete_api = self.client.delete_api()
            
            delete_api.delete(
                start=start,
                stop=stop,
                predicate=f'_measurement="{measurement}"',
                bucket=settings.influx_bucket,
                org=settings.influx_org
            )
            
            logger.info(f"Deleted data from {measurement} between {start} and {stop}")
            
        except Exception as e:
            logger.error(f"Failed to delete data from {measurement}: {e}")
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """Check InfluxDB health"""
        try:
            health = self.client.health()
            return {
                "status": "healthy" if health.status == "pass" else "unhealthy",
                "connected": self.connected,
                "version": health.version if hasattr(health, 'version') else None,
                "message": health.message if hasattr(health, 'message') else None
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e)
            }