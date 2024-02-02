"""
Data Transformers for Multi-Source Data Lake ETL Pipeline
"""
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import re
import json
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseTransformer(ABC):
    """Abstract base class for data transformers"""
    
    def __init__(self, name: str):
        self.name = name
        self.transformed_count = 0
    
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record"""
        pass

class DataCleaningTransformer(BaseTransformer):
    """Clean and standardize data"""
    
    def __init__(self):
        super().__init__("Data Cleaning Transformer")
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean data by removing nulls, standardizing formats"""
        cleaned_data = {}
        
        for key, value in data.items():
            # Handle None/null values
            if value is None or value == '' or (isinstance(value, float) and np.isnan(value)):
                cleaned_data[key] = None
                continue
            
            # Clean string values
            if isinstance(value, str):
                # Remove extra whitespace
                value = value.strip()
                # Standardize case for certain fields
                if key.lower() in ['email']:
                    value = value.lower()
                elif key.lower() in ['country', 'state', 'status']:
                    value = value.title()
            
            # Standardize phone numbers
            if 'phone' in key.lower():
                value = self._clean_phone_number(str(value))
            
            # Clean numeric values
            if isinstance(value, str) and self._is_numeric(value):
                try:
                    value = float(value) if '.' in value else int(value)
                except ValueError:
                    pass
            
            cleaned_data[key] = value
        
        # Add cleaning metadata
        cleaned_data['_cleaned_at'] = datetime.utcnow().isoformat()
        self.transformed_count += 1
        
        return cleaned_data
    
    def _clean_phone_number(self, phone: str) -> str:
        """Standardize phone number format"""
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', phone)
        
        # Format based on length
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        else:
            return phone  # Return original if can't standardize
    
    def _is_numeric(self, value: str) -> bool:
        """Check if string represents a number"""
        try:
            float(value)
            return True
        except ValueError:
            return False

class DataValidationTransformer(BaseTransformer):
    """Validate data against business rules"""
    
    def __init__(self, validation_rules: Dict[str, Dict] = None):
        super().__init__("Data Validation Transformer")
        self.validation_rules = validation_rules or {}
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data and add validation flags"""
        validated_data = data.copy()
        validation_errors = []
        
        for field, rules in self.validation_rules.items():
            if field in data:
                value = data[field]
                
                # Required field check
                if rules.get('required', False) and (value is None or value == ''):
                    validation_errors.append(f"{field} is required")
                
                # Type validation
                expected_type = rules.get('type')
                if expected_type and value is not None:
                    if expected_type == 'email' and not self._is_valid_email(str(value)):
                        validation_errors.append(f"{field} is not a valid email")
                    elif expected_type == 'date' and not self._is_valid_date(str(value)):
                        validation_errors.append(f"{field} is not a valid date")
                
                # Range validation for numeric fields
                if isinstance(value, (int, float)):
                    min_val = rules.get('min')
                    max_val = rules.get('max')
                    if min_val is not None and value < min_val:
                        validation_errors.append(f"{field} is below minimum value {min_val}")
                    if max_val is not None and value > max_val:
                        validation_errors.append(f"{field} exceeds maximum value {max_val}")
        
        # Add validation metadata
        validated_data['_validation_errors'] = validation_errors
        validated_data['_is_valid'] = len(validation_errors) == 0
        validated_data['_validated_at'] = datetime.utcnow().isoformat()
        
        self.transformed_count += 1
        return validated_data
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    def _is_valid_date(self, date_str: str) -> bool:
        """Validate date format"""
        try:
            datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return True
        except:
            return False

class DataEnrichmentTransformer(BaseTransformer):
    """Enrich data with additional computed fields"""
    
    def __init__(self):
        super().__init__("Data Enrichment Transformer")
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add computed fields and enrichments"""
        enriched_data = data.copy()
        
        # Add timestamp fields
        enriched_data['_ingested_at'] = datetime.utcnow().isoformat()
        
        # Calculate age if birth_date exists
        if 'birth_date' in data and data['birth_date']:
            try:
                birth_date = datetime.fromisoformat(str(data['birth_date']).replace('Z', '+00:00'))
                age = (datetime.now(birth_date.tzinfo) - birth_date).days // 365
                enriched_data['calculated_age'] = age
            except:
                pass
        
        # Add derived location fields
        if 'address' in data:
            address = str(data['address'])
            # Extract postal code
            postal_match = re.search(r'\b\d{5}(-\d{4})?\b', address)
            if postal_match:
                enriched_data['extracted_postal_code'] = postal_match.group()
        
        # Generate hash for deduplication
        key_fields = ['email', 'phone', 'name'] if any(f in data for f in ['email', 'phone', 'name']) else list(data.keys())[:3]
        hash_input = ''.join([str(data.get(f, '')) for f in key_fields])
        enriched_data['_record_hash'] = str(hash(hash_input))
        
        self.transformed_count += 1
        return enriched_data

class DataNormalizationTransformer(BaseTransformer):
    """Normalize data to standard schema"""
    
    def __init__(self, schema_mapping: Dict[str, str] = None):
        super().__init__("Data Normalization Transformer")
        self.schema_mapping = schema_mapping or {}
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize field names and structure"""
        normalized_data = {}
        
        # Apply field mappings
        for old_field, new_field in self.schema_mapping.items():
            if old_field in data:
                normalized_data[new_field] = data[old_field]
        
        # Copy unmapped fields
        for field, value in data.items():
            if field not in self.schema_mapping and field not in normalized_data:
                normalized_data[field] = value
        
        # Standardize common field names
        field_standardizations = {
            'id': 'record_id',
            'created': 'created_at',
            'updated': 'updated_at',
            'name': 'full_name',
            'phone_number': 'phone',
            'email_address': 'email'
        }
        
        for old_name, new_name in field_standardizations.items():
            if old_name in normalized_data and new_name not in normalized_data:
                normalized_data[new_name] = normalized_data.pop(old_name)
        
        self.transformed_count += 1
        return normalized_data

class AggregationTransformer(BaseTransformer):
    """Perform data aggregations and calculations"""
    
    def __init__(self, group_by_fields: List[str], aggregations: Dict[str, str]):
        super().__init__("Aggregation Transformer")
        self.group_by_fields = group_by_fields
        self.aggregations = aggregations  # {'field': 'function'}
        self.data_buffer = []
    
    def add_record(self, data: Dict[str, Any]):
        """Add record to buffer for batch processing"""
        self.data_buffer.append(data)
    
    def process_batch(self) -> List[Dict[str, Any]]:
        """Process buffered records and return aggregated results"""
        if not self.data_buffer:
            return []
        
        df = pd.DataFrame(self.data_buffer)
        
        # Group by specified fields
        grouped = df.groupby(self.group_by_fields)
        
        # Apply aggregations
        results = []
        for name, group in grouped:
            result = {}
            
            # Add grouping fields
            if len(self.group_by_fields) == 1:
                result[self.group_by_fields[0]] = name
            else:
                for i, field in enumerate(self.group_by_fields):
                    result[field] = name[i]
            
            # Apply aggregation functions
            for field, func in self.aggregations.items():
                if field in group.columns:
                    if func == 'count':
                        result[f"{field}_count"] = len(group)
                    elif func == 'sum':
                        result[f"{field}_sum"] = group[field].sum()
                    elif func == 'avg':
                        result[f"{field}_avg"] = group[field].mean()
                    elif func == 'min':
                        result[f"{field}_min"] = group[field].min()
                    elif func == 'max':
                        result[f"{field}_max"] = group[field].max()
            
            result['_record_count'] = len(group)
            result['_aggregated_at'] = datetime.utcnow().isoformat()
            results.append(result)
            self.transformed_count += 1
        
        # Clear buffer
        self.data_buffer = []
        return results
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add to batch - actual transformation happens in process_batch"""
        self.add_record(data)
        return data  # Return original for chaining

class TransformationPipeline:
    """Chain multiple transformers together"""
    
    def __init__(self, transformers: List[BaseTransformer]):
        self.transformers = transformers
        self.total_processed = 0
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all transformers in sequence"""
        result = data
        
        for transformer in self.transformers:
            try:
                result = transformer.transform(result)
            except Exception as e:
                logger.error(f"Transformation failed in {transformer.name}: {e}")
                # Add error to result but continue pipeline
                result['_transformation_errors'] = result.get('_transformation_errors', [])
                result['_transformation_errors'].append(f"{transformer.name}: {str(e)}")
        
        self.total_processed += 1
        return result
    
    def get_stats(self) -> Dict[str, Any]:
        """Get transformation statistics"""
        return {
            'total_processed': self.total_processed,
            'transformers': [
                {
                    'name': t.name,
                    'transformed_count': t.transformed_count
                }
                for t in self.transformers
            ]
        }