"""
Utility Functions for Multi-Source Data Lake System
"""
import logging
import json
import yaml
import csv
from typing import Dict, List, Any, Optional, Union, Iterator
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import hashlib
import uuid
from pathlib import Path
import asyncio
from functools import wraps
import time

logger = logging.getLogger(__name__)

class DataTypeUtils:
    """Utilities for data type detection and conversion"""
    
    @staticmethod
    def detect_data_type(value: Any) -> str:
        """Detect the data type of a value"""
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            # Try to detect specific string formats
            if DataTypeUtils.is_email(value):
                return 'email'
            elif DataTypeUtils.is_phone(value):
                return 'phone'
            elif DataTypeUtils.is_date(value):
                return 'date'
            elif DataTypeUtils.is_url(value):
                return 'url'
            else:
                return 'string'
        elif isinstance(value, (list, tuple)):
            return 'array'
        elif isinstance(value, dict):
            return 'object'
        else:
            return 'unknown'
    
    @staticmethod
    def is_email(value: str) -> bool:
        """Check if string is an email"""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, value))
    
    @staticmethod
    def is_phone(value: str) -> bool:
        """Check if string is a phone number"""
        import re
        # Remove all non-digit characters and check length
        digits = re.sub(r'\D', '', value)
        return len(digits) >= 10 and len(digits) <= 15
    
    @staticmethod
    def is_date(value: str) -> bool:
        """Check if string is a date"""
        date_formats = [
            '%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%f'
        ]
        
        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False
    
    @staticmethod
    def is_url(value: str) -> bool:
        """Check if string is a URL"""
        import re
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return bool(url_pattern.match(value))

class FileUtils:
    """File handling utilities"""
    
    @staticmethod
    def read_file(file_path: Union[str, Path], file_type: str = None) -> List[Dict[str, Any]]:
        """Read data from various file formats"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_type = file_type or file_path.suffix.lower()
        
        if file_type in ['.json']:
            with open(file_path, 'r') as f:
                data = json.load(f)
                return data if isinstance(data, list) else [data]
        
        elif file_type in ['.jsonl', '.ndjson']:
            data = []
            with open(file_path, 'r') as f:
                for line in f:
                    data.append(json.loads(line.strip()))
            return data
        
        elif file_type in ['.csv']:
            df = pd.read_csv(file_path)
            return df.to_dict('records')
        
        elif file_type in ['.parquet']:
            df = pd.read_parquet(file_path)
            return df.to_dict('records')
        
        elif file_type in ['.yaml', '.yml']:
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
                return data if isinstance(data, list) else [data]
        
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    
    @staticmethod
    def write_file(data: List[Dict[str, Any]], file_path: Union[str, Path], 
                   file_type: str = None) -> None:
        """Write data to various file formats"""
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_type = file_type or file_path.suffix.lower()
        
        if file_type in ['.json']:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        
        elif file_type in ['.jsonl', '.ndjson']:
            with open(file_path, 'w') as f:
                for record in data:
                    f.write(json.dumps(record, default=str) + '\n')
        
        elif file_type in ['.csv']:
            df = pd.DataFrame(data)
            df.to_csv(file_path, index=False)
        
        elif file_type in ['.parquet']:
            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)
        
        elif file_type in ['.yaml', '.yml']:
            with open(file_path, 'w') as f:
                yaml.dump(data, f, default_flow_style=False)
        
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

class HashUtils:
    """Hashing and deduplication utilities"""
    
    @staticmethod
    def generate_record_hash(record: Dict[str, Any], fields: List[str] = None) -> str:
        """Generate hash for a record"""
        if fields:
            hash_data = {k: record.get(k) for k in fields if k in record}
        else:
            hash_data = record
        
        # Sort keys for consistent hashing
        sorted_data = json.dumps(hash_data, sort_keys=True, default=str)
        return hashlib.md5(sorted_data.encode()).hexdigest()
    
    @staticmethod
    def find_duplicates(data: List[Dict[str, Any]], key_fields: List[str] = None) -> Dict[str, List[int]]:
        """Find duplicate records based on key fields"""
        hash_to_indices = {}
        
        for i, record in enumerate(data):
            record_hash = HashUtils.generate_record_hash(record, key_fields)
            
            if record_hash not in hash_to_indices:
                hash_to_indices[record_hash] = []
            hash_to_indices[record_hash].append(i)
        
        # Return only duplicates (more than one occurrence)
        return {k: v for k, v in hash_to_indices.items() if len(v) > 1}
    
    @staticmethod
    def deduplicate(data: List[Dict[str, Any]], key_fields: List[str] = None, 
                   keep: str = 'first') -> List[Dict[str, Any]]:
        """Remove duplicates from data"""
        if not data:
            return data
        
        seen_hashes = set()
        result = []
        
        if keep == 'last':
            data = reversed(data)
        
        for record in data:
            record_hash = HashUtils.generate_record_hash(record, key_fields)
            
            if record_hash not in seen_hashes:
                seen_hashes.add(record_hash)
                result.append(record)
        
        if keep == 'last':
            result = list(reversed(result))
        
        return result

class PerformanceUtils:
    """Performance monitoring and optimization utilities"""
    
    @staticmethod
    def timer(func):
        """Decorator to measure function execution time"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            
            logger.info(f"Function '{func.__name__}' executed in {end_time - start_time:.4f} seconds")
            return result
        return wrapper
    
    @staticmethod
    def async_timer(func):
        """Decorator to measure async function execution time"""
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            end_time = time.time()
            
            logger.info(f"Async function '{func.__name__}' executed in {end_time - start_time:.4f} seconds")
            return result
        return wrapper
    
    @staticmethod
    def chunk_data(data: List[Any], chunk_size: int) -> Iterator[List[Any]]:
        """Split data into chunks for batch processing"""
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    
    @staticmethod
    def memory_usage_mb() -> float:
        """Get current memory usage in MB"""
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024

class DateTimeUtils:
    """Date and time utilities"""
    
    @staticmethod
    def parse_date(date_str: str, formats: List[str] = None) -> Optional[datetime]:
        """Parse date string with multiple format attempts"""
        if not formats:
            formats = [
                '%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y',
                '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ'
            ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Try ISO format parsing as fallback
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None
    
    @staticmethod
    def date_range(start_date: datetime, end_date: datetime, 
                   interval: str = 'day') -> Iterator[datetime]:
        """Generate date range with specified interval"""
        current = start_date
        
        if interval == 'day':
            delta = timedelta(days=1)
        elif interval == 'hour':
            delta = timedelta(hours=1)
        elif interval == 'week':
            delta = timedelta(weeks=1)
        elif interval == 'month':
            delta = timedelta(days=30)  # Approximate
        else:
            raise ValueError(f"Unsupported interval: {interval}")
        
        while current <= end_date:
            yield current
            current += delta
    
    @staticmethod
    def time_ago(dt: datetime) -> str:
        """Get human-readable time difference"""
        now = datetime.utcnow()
        diff = now - dt
        
        seconds = diff.total_seconds()
        
        if seconds < 60:
            return f"{int(seconds)} seconds ago"
        elif seconds < 3600:
            return f"{int(seconds // 60)} minutes ago"
        elif seconds < 86400:
            return f"{int(seconds // 3600)} hours ago"
        else:
            return f"{int(seconds // 86400)} days ago"

class ConfigUtils:
    """Configuration management utilities"""
    
    @staticmethod
    def load_config(config_path: Union[str, Path]) -> Dict[str, Any]:
        """Load configuration from file"""
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        if config_path.suffix.lower() in ['.yaml', '.yml']:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        elif config_path.suffix.lower() == '.json':
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            raise ValueError(f"Unsupported config format: {config_path.suffix}")
    
    @staticmethod
    def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge multiple configuration dictionaries"""
        result = {}
        
        for config in configs:
            for key, value in config.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = ConfigUtils.merge_configs(result[key], value)
                else:
                    result[key] = value
        
        return result

class LoggingUtils:
    """Enhanced logging utilities"""
    
    @staticmethod
    def setup_logging(log_level: str = 'INFO', log_file: str = None) -> None:
        """Setup structured logging"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        handlers = [logging.StreamHandler()]
        
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(log_file))
        
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format=log_format,
            handlers=handlers
        )
    
    @staticmethod
    def log_execution(operation: str):
        """Decorator to log function execution"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                logger.info(f"Starting {operation}: {func.__name__}")
                try:
                    result = func(*args, **kwargs)
                    logger.info(f"Completed {operation}: {func.__name__}")
                    return result
                except Exception as e:
                    logger.error(f"Failed {operation}: {func.__name__} - {str(e)}")
                    raise
            return wrapper
        return decorator

class ValidationUtils:
    """Data validation utilities"""
    
    @staticmethod
    def is_valid_json(data: str) -> bool:
        """Check if string is valid JSON"""
        try:
            json.loads(data)
            return True
        except (json.JSONDecodeError, TypeError):
            return False
    
    @staticmethod
    def sanitize_string(text: str) -> str:
        """Sanitize string for safe processing"""
        if not isinstance(text, str):
            return str(text)
        
        # Remove control characters
        sanitized = ''.join(char for char in text if ord(char) >= 32)
        
        # Trim whitespace
        sanitized = sanitized.strip()
        
        return sanitized
    
    @staticmethod
    def validate_schema_compatibility(schema1: Dict[str, Any], schema2: Dict[str, Any]) -> bool:
        """Check if two schemas are compatible"""
        # Simple compatibility check - all required fields in schema1 exist in schema2
        for field, field_schema in schema1.items():
            if field_schema.get('required', False) and field not in schema2:
                return False
        
        return True