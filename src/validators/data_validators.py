"""
Data Validators for Multi-Source Data Lake
"""
import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime
import re
import json
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class ValidationResult:
    """Result of data validation"""
    
    def __init__(self, is_valid: bool = True, errors: List[str] = None, 
                 warnings: List[str] = None, metrics: Dict[str, Any] = None):
        self.is_valid = is_valid
        self.errors = errors or []
        self.warnings = warnings or []
        self.metrics = metrics or {}
        self.timestamp = datetime.utcnow().isoformat()
    
    def add_error(self, error: str):
        """Add validation error"""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """Add validation warning"""
        self.warnings.append(warning)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings,
            'metrics': self.metrics,
            'timestamp': self.timestamp
        }

class BaseValidator(ABC):
    """Abstract base validator"""
    
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        """Validate data"""
        pass

class SchemaValidator(BaseValidator):
    """Validate data against expected schema"""
    
    def __init__(self, schema: Dict[str, Any]):
        super().__init__("Schema Validator")
        self.schema = schema
    
    def validate(self, data: Union[Dict, List[Dict]]) -> ValidationResult:
        """Validate data against schema"""
        result = ValidationResult()
        
        # Handle single record or list of records
        records = data if isinstance(data, list) else [data]
        
        for i, record in enumerate(records):
            if not isinstance(record, dict):
                result.add_error(f"Record {i}: Expected dict, got {type(record)}")
                continue
            
            # Check required fields
            for field, field_schema in self.schema.items():
                if field_schema.get('required', False) and field not in record:
                    result.add_error(f"Record {i}: Missing required field '{field}'")
                
                if field in record:
                    value = record[field]
                    
                    # Type validation
                    expected_type = field_schema.get('type')
                    if expected_type and not self._validate_type(value, expected_type):
                        result.add_error(f"Record {i}: Field '{field}' expected {expected_type}, got {type(value)}")
                    
                    # Range validation
                    if isinstance(value, (int, float)):
                        min_val = field_schema.get('min')
                        max_val = field_schema.get('max')
                        if min_val is not None and value < min_val:
                            result.add_error(f"Record {i}: Field '{field}' value {value} below minimum {min_val}")
                        if max_val is not None and value > max_val:
                            result.add_error(f"Record {i}: Field '{field}' value {value} above maximum {max_val}")
                    
                    # Length validation
                    if isinstance(value, str):
                        min_len = field_schema.get('min_length')
                        max_len = field_schema.get('max_length')
                        if min_len is not None and len(value) < min_len:
                            result.add_error(f"Record {i}: Field '{field}' length {len(value)} below minimum {min_len}")
                        if max_len is not None and len(value) > max_len:
                            result.add_error(f"Record {i}: Field '{field}' length {len(value)} above maximum {max_len}")
        
        result.metrics['total_records'] = len(records)
        result.metrics['validation_errors'] = len(result.errors)
        
        return result
    
    def _validate_type(self, value, expected_type: str) -> bool:
        """Validate field type"""
        if expected_type == 'string':
            return isinstance(value, str)
        elif expected_type == 'integer':
            return isinstance(value, int)
        elif expected_type == 'float':
            return isinstance(value, (int, float))
        elif expected_type == 'boolean':
            return isinstance(value, bool)
        elif expected_type == 'datetime':
            if isinstance(value, str):
                try:
                    datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return True
                except:
                    return False
            return isinstance(value, datetime)
        elif expected_type == 'email':
            if isinstance(value, str):
                pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                return re.match(pattern, value) is not None
        return True

class DataQualityValidator(BaseValidator):
    """Validate data quality metrics"""
    
    def __init__(self, quality_rules: Dict[str, Any] = None):
        super().__init__("Data Quality Validator")
        self.quality_rules = quality_rules or {
            'max_null_percentage': 0.1,  # 10% null values allowed
            'max_duplicate_percentage': 0.05,  # 5% duplicates allowed
            'min_records': 1
        }
    
    def validate(self, data: List[Dict]) -> ValidationResult:
        """Validate data quality"""
        result = ValidationResult()
        
        if not data:
            result.add_error("No data provided for validation")
            return result
        
        df = pd.DataFrame(data)
        total_records = len(df)
        
        # Check minimum records
        min_records = self.quality_rules.get('min_records', 1)
        if total_records < min_records:
            result.add_error(f"Insufficient data: {total_records} records, minimum required: {min_records}")
        
        # Check null percentages
        max_null_pct = self.quality_rules.get('max_null_percentage', 0.1)
        for column in df.columns:
            null_count = df[column].isnull().sum()
            null_percentage = null_count / total_records
            
            if null_percentage > max_null_pct:
                result.add_warning(f"Column '{column}' has {null_percentage:.2%} null values (threshold: {max_null_pct:.2%})")
        
        # Check for duplicates
        max_dup_pct = self.quality_rules.get('max_duplicate_percentage', 0.05)
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = duplicate_count / total_records
        
        if duplicate_percentage > max_dup_pct:
            result.add_warning(f"Found {duplicate_percentage:.2%} duplicate records (threshold: {max_dup_pct:.2%})")
        
        # Data distribution checks
        for column in df.select_dtypes(include=[np.number]).columns:
            if df[column].std() == 0:
                result.add_warning(f"Column '{column}' has no variance (all values identical)")
        
        # Store quality metrics
        result.metrics.update({
            'total_records': total_records,
            'duplicate_count': duplicate_count,
            'duplicate_percentage': duplicate_percentage,
            'null_percentages': {col: df[col].isnull().sum() / total_records for col in df.columns},
            'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()}
        })
        
        return result

class BusinessRuleValidator(BaseValidator):
    """Validate business-specific rules"""
    
    def __init__(self, rules: List[Dict[str, Any]]):
        super().__init__("Business Rule Validator")
        self.rules = rules
    
    def validate(self, data: List[Dict]) -> ValidationResult:
        """Validate business rules"""
        result = ValidationResult()
        
        for rule in self.rules:
            rule_name = rule.get('name', 'Unnamed Rule')
            rule_type = rule.get('type')
            
            if rule_type == 'range':
                self._validate_range_rule(data, rule, result)
            elif rule_type == 'relationship':
                self._validate_relationship_rule(data, rule, result)
            elif rule_type == 'custom':
                self._validate_custom_rule(data, rule, result)
        
        return result
    
    def _validate_range_rule(self, data: List[Dict], rule: Dict, result: ValidationResult):
        """Validate range-based rules"""
        field = rule.get('field')
        min_val = rule.get('min')
        max_val = rule.get('max')
        
        violations = 0
        for i, record in enumerate(data):
            if field in record:
                value = record[field]
                if isinstance(value, (int, float)):
                    if min_val is not None and value < min_val:
                        violations += 1
                    if max_val is not None and value > max_val:
                        violations += 1
        
        if violations > 0:
            result.add_error(f"Rule '{rule.get('name')}': {violations} violations found")
    
    def _validate_relationship_rule(self, data: List[Dict], rule: Dict, result: ValidationResult):
        """Validate relationship rules between fields"""
        field1 = rule.get('field1')
        field2 = rule.get('field2')
        operator = rule.get('operator')  # 'greater_than', 'less_than', 'equal', etc.
        
        violations = 0
        for record in data:
            if field1 in record and field2 in record:
                val1, val2 = record[field1], record[field2]
                
                if operator == 'greater_than' and not (val1 > val2):
                    violations += 1
                elif operator == 'less_than' and not (val1 < val2):
                    violations += 1
                elif operator == 'equal' and not (val1 == val2):
                    violations += 1
        
        if violations > 0:
            result.add_error(f"Rule '{rule.get('name')}': {violations} relationship violations")
    
    def _validate_custom_rule(self, data: List[Dict], rule: Dict, result: ValidationResult):
        """Validate custom rules using provided function"""
        custom_function = rule.get('function')
        if callable(custom_function):
            try:
                violations = custom_function(data)
                if violations > 0:
                    result.add_error(f"Rule '{rule.get('name')}': {violations} custom rule violations")
            except Exception as e:
                result.add_error(f"Rule '{rule.get('name')}': Custom validation failed - {str(e)}")

class ValidationPipeline:
    """Chain multiple validators together"""
    
    def __init__(self, validators: List[BaseValidator]):
        self.validators = validators
    
    def validate(self, data: Any) -> Dict[str, ValidationResult]:
        """Run all validators and return combined results"""
        results = {}
        
        for validator in self.validators:
            try:
                results[validator.name] = validator.validate(data)
            except Exception as e:
                error_result = ValidationResult(is_valid=False)
                error_result.add_error(f"Validator '{validator.name}' failed: {str(e)}")
                results[validator.name] = error_result
        
        return results
    
    def is_valid(self, results: Dict[str, ValidationResult] = None) -> bool:
        """Check if all validations passed"""
        if results is None:
            return True
        
        return all(result.is_valid for result in results.values())
    
    def get_summary(self, results: Dict[str, ValidationResult]) -> Dict[str, Any]:
        """Get validation summary"""
        total_errors = sum(len(result.errors) for result in results.values())
        total_warnings = sum(len(result.warnings) for result in results.values())
        
        return {
            'overall_valid': self.is_valid(results),
            'total_errors': total_errors,
            'total_warnings': total_warnings,
            'validator_results': {name: result.to_dict() for name, result in results.items()},
            'timestamp': datetime.utcnow().isoformat()
        }