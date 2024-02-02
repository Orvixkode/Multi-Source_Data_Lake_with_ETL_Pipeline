"""
Unit Tests for Multi-Source Data Lake ETL Pipeline
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime
import pandas as pd
import json

from src.etl.extractors.base_extractors import PostgreSQLExtractor, MongoExtractor, FileExtractor
from src.etl.transformers.data_transformers import DataCleaningTransformer, DataValidationTransformer, TransformationPipeline
from src.etl.loaders.base_loaders import PostgreSQLLoader, MongoLoader, FileLoader
from src.validators.data_validators import SchemaValidator, DataQualityValidator, ValidationPipeline
from src.utils.common_utils import HashUtils, FileUtils, DataTypeUtils

class TestExtractors:
    """Test data extractors"""
    
    @pytest.fixture
    def sample_data(self):
        return [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com'},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com'}
        ]
    
    @pytest.mark.asyncio
    async def test_postgres_extractor(self, sample_data):
        """Test PostgreSQL extractor"""
        extractor = PostgreSQLExtractor()
        
        # Mock the connector
        with patch.object(extractor, 'connector') as mock_connector:
            mock_connector.connect = AsyncMock()
            mock_connector.execute_query = AsyncMock(return_value=sample_data)
            mock_connector.disconnect = AsyncMock()
            
            # Test extraction
            results = []
            async for record in extractor.extract(table_name='users', batch_size=10):
                results.append(record)
            
            assert len(results) == 2
            assert results[0]['name'] == 'John Doe'
            mock_connector.connect.assert_called_once()
            mock_connector.disconnect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_mongo_extractor(self, sample_data):
        """Test MongoDB extractor"""
        extractor = MongoExtractor()
        
        with patch.object(extractor, 'connector') as mock_connector:
            mock_connector.connect = AsyncMock()
            mock_connector.find = AsyncMock(return_value=sample_data)
            mock_connector.disconnect = AsyncMock()
            
            results = []
            async for record in extractor.extract(collection_name='users'):
                results.append(record)
            
            assert len(results) == 2
            assert results[1]['email'] == 'jane@example.com'
    
    def test_file_extractor(self, tmp_path, sample_data):
        """Test file extractor"""
        # Create test JSON file
        test_file = tmp_path / "test_data.json"
        with open(test_file, 'w') as f:
            json.dump(sample_data, f)
        
        extractor = FileExtractor()
        
        # Test extraction
        results = []
        async def extract_test():
            async for record in extractor.extract(file_path=str(test_file)):
                results.append(record)
        
        asyncio.run(extract_test())
        
        assert len(results) == 2
        assert results[0]['id'] == 1

class TestTransformers:
    """Test data transformers"""
    
    @pytest.fixture
    def sample_record(self):
        return {
            'name': '  John Doe  ',
            'email': 'JOHN@EXAMPLE.COM',
            'phone': '(555) 123-4567',
            'age': '30',
            'status': 'active'
        }
    
    def test_data_cleaning_transformer(self, sample_record):
        """Test data cleaning transformer"""
        transformer = DataCleaningTransformer()
        result = transformer.transform(sample_record)
        
        # Check cleaning results
        assert result['name'] == 'John Doe'  # Trimmed whitespace
        assert result['email'] == 'john@example.com'  # Lowercase
        assert result['age'] == 30  # Converted to int
        assert '_cleaned_at' in result
    
    def test_data_validation_transformer(self):
        """Test data validation transformer"""
        validation_rules = {
            'email': {'required': True, 'type': 'email'},
            'age': {'type': 'integer', 'min': 0, 'max': 150}
        }
        
        transformer = DataValidationTransformer(validation_rules)
        
        # Valid record
        valid_record = {'email': 'test@example.com', 'age': 25}
        result = transformer.transform(valid_record)
        assert result['_is_valid'] is True
        assert len(result['_validation_errors']) == 0
        
        # Invalid record
        invalid_record = {'email': 'invalid-email', 'age': 200}
        result = transformer.transform(invalid_record)
        assert result['_is_valid'] is False
        assert len(result['_validation_errors']) > 0
    
    def test_transformation_pipeline(self):
        """Test transformation pipeline"""
        transformers = [
            DataCleaningTransformer(),
            DataValidationTransformer({'email': {'type': 'email'}})
        ]
        
        pipeline = TransformationPipeline(transformers)
        
        record = {'name': '  Test  ', 'email': 'test@example.com'}
        result = pipeline.transform(record)
        
        # Should have both cleaning and validation applied
        assert result['name'] == 'Test'
        assert '_cleaned_at' in result
        assert '_validation_errors' in result
        assert pipeline.total_processed == 1

class TestLoaders:
    """Test data loaders"""
    
    @pytest.fixture
    def sample_data(self):
        return [
            {'id': 1, 'name': 'John', 'email': 'john@test.com'},
            {'id': 2, 'name': 'Jane', 'email': 'jane@test.com'}
        ]
    
    @pytest.mark.asyncio
    async def test_postgres_loader(self, sample_data):
        """Test PostgreSQL loader"""
        loader = PostgreSQLLoader()
        
        with patch.object(loader, 'connector') as mock_connector:
            mock_connector.connect = AsyncMock()
            mock_connector.insert_dataframe = AsyncMock()
            mock_connector.disconnect = AsyncMock()
            
            result = await loader.load(sample_data, table_name='test_table')
            
            assert result is True
            assert loader.loaded_count == 2
            mock_connector.insert_dataframe.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_mongo_loader(self, sample_data):
        """Test MongoDB loader"""
        loader = MongoLoader()
        
        with patch.object(loader, 'connector') as mock_connector:
            mock_connector.connect = AsyncMock()
            mock_connector.insert_many = AsyncMock(return_value=['id1', 'id2'])
            mock_connector.disconnect = AsyncMock()
            
            result = await loader.load(sample_data, collection_name='test_collection')
            
            assert result is True
            assert loader.loaded_count == 2
    
    @pytest.mark.asyncio
    async def test_file_loader(self, sample_data, tmp_path):
        """Test file loader"""
        loader = FileLoader()
        output_file = tmp_path / "output.json"
        
        result = await loader.load(sample_data, file_path=str(output_file), file_format='json')
        
        assert result is True
        assert output_file.exists()
        
        # Verify file content
        with open(output_file, 'r') as f:
            loaded_data = json.load(f)
        
        assert len(loaded_data) == 2
        assert loaded_data[0]['name'] == 'John'

class TestValidators:
    """Test data validators"""
    
    def test_schema_validator(self):
        """Test schema validation"""
        schema = {
            'name': {'required': True, 'type': 'string'},
            'email': {'required': True, 'type': 'email'},
            'age': {'type': 'integer', 'min': 0, 'max': 150}
        }
        
        validator = SchemaValidator(schema)
        
        # Valid data
        valid_data = [
            {'name': 'John', 'email': 'john@test.com', 'age': 30},
            {'name': 'Jane', 'email': 'jane@test.com', 'age': 25}
        ]
        
        result = validator.validate(valid_data)
        assert result.is_valid is True
        assert len(result.errors) == 0
        
        # Invalid data
        invalid_data = [
            {'name': 'John', 'email': 'invalid-email'},  # Missing age, invalid email
            {'email': 'jane@test.com', 'age': 200}  # Missing name, age too high
        ]
        
        result = validator.validate(invalid_data)
        assert result.is_valid is False
        assert len(result.errors) > 0
    
    def test_data_quality_validator(self):
        """Test data quality validation"""
        validator = DataQualityValidator({
            'max_null_percentage': 0.1,
            'max_duplicate_percentage': 0.05
        })
        
        # Good quality data
        good_data = [
            {'name': 'John', 'age': 30},
            {'name': 'Jane', 'age': 25},
            {'name': 'Bob', 'age': 35}
        ]
        
        result = validator.validate(good_data)
        assert result.is_valid is True
        
        # Poor quality data with duplicates
        poor_data = [
            {'name': 'John', 'age': 30},
            {'name': 'John', 'age': 30},  # Duplicate
            {'name': None, 'age': None}   # Nulls
        ]
        
        result = validator.validate(poor_data)
        # Should have warnings about data quality
        assert len(result.warnings) > 0
    
    def test_validation_pipeline(self):
        """Test validation pipeline"""
        schema = {'name': {'required': True, 'type': 'string'}}
        validators = [
            SchemaValidator(schema),
            DataQualityValidator()
        ]
        
        pipeline = ValidationPipeline(validators)
        
        data = [{'name': 'John'}, {'name': 'Jane'}]
        results = pipeline.validate(data)
        
        assert len(results) == 2  # Two validators
        assert 'Schema Validator' in results
        assert 'Data Quality Validator' in results
        
        summary = pipeline.get_summary(results)
        assert 'overall_valid' in summary
        assert 'total_errors' in summary

class TestUtils:
    """Test utility functions"""
    
    def test_hash_utils(self):
        """Test hashing utilities"""
        record1 = {'name': 'John', 'age': 30}
        record2 = {'name': 'John', 'age': 30}
        record3 = {'name': 'Jane', 'age': 25}
        
        # Same records should have same hash
        hash1 = HashUtils.generate_record_hash(record1)
        hash2 = HashUtils.generate_record_hash(record2)
        hash3 = HashUtils.generate_record_hash(record3)
        
        assert hash1 == hash2
        assert hash1 != hash3
        
        # Test duplicate detection
        data = [record1, record2, record3]
        duplicates = HashUtils.find_duplicates(data)
        
        assert len(duplicates) == 1  # One set of duplicates
        
        # Test deduplication
        deduplicated = HashUtils.deduplicate(data)
        assert len(deduplicated) == 2  # Should remove one duplicate
    
    def test_data_type_utils(self):
        """Test data type detection"""
        assert DataTypeUtils.detect_data_type("john@example.com") == "email"
        assert DataTypeUtils.detect_data_type("(555) 123-4567") == "phone"
        assert DataTypeUtils.detect_data_type("2023-01-01") == "date"
        assert DataTypeUtils.detect_data_type("https://example.com") == "url"
        assert DataTypeUtils.detect_data_type(42) == "integer"
        assert DataTypeUtils.detect_data_type(3.14) == "float"
        assert DataTypeUtils.detect_data_type(True) == "boolean"
        assert DataTypeUtils.detect_data_type(None) == "null"
    
    def test_file_utils(self, tmp_path):
        """Test file utilities"""
        data = [{'name': 'John', 'age': 30}]
        
        # Test JSON
        json_file = tmp_path / "test.json"
        FileUtils.write_file(data, json_file)
        loaded_data = FileUtils.read_file(json_file)
        assert loaded_data == data
        
        # Test CSV
        csv_file = tmp_path / "test.csv"
        FileUtils.write_file(data, csv_file)
        loaded_data = FileUtils.read_file(csv_file)
        assert loaded_data[0]['name'] == 'John'

# Integration Tests
class TestETLIntegration:
    """Integration tests for full ETL pipeline"""
    
    @pytest.mark.asyncio
    async def test_full_etl_pipeline(self, tmp_path):
        """Test complete ETL pipeline"""
        # Create test data file
        input_data = [
            {'name': '  John Doe  ', 'email': 'JOHN@TEST.COM', 'age': '30'},
            {'name': 'Jane Smith', 'email': 'jane@test.com', 'age': '25'}
        ]
        
        input_file = tmp_path / "input.json"
        with open(input_file, 'w') as f:
            json.dump(input_data, f)
        
        # Extract
        extractor = FileExtractor()
        extracted_data = []
        async for record in extractor.extract(file_path=str(input_file)):
            extracted_data.append(record)
        
        assert len(extracted_data) == 2
        
        # Transform
        transformers = [
            DataCleaningTransformer(),
            DataValidationTransformer({'email': {'type': 'email'}})
        ]
        pipeline = TransformationPipeline(transformers)
        
        transformed_data = []
        for record in extracted_data:
            transformed_data.append(pipeline.transform(record))
        
        # Check transformations
        assert transformed_data[0]['name'] == 'John Doe'  # Cleaned
        assert transformed_data[0]['email'] == 'john@test.com'  # Lowercase
        assert transformed_data[0]['age'] == 30  # Type converted
        assert transformed_data[0]['_is_valid'] is True  # Validated
        
        # Load
        output_file = tmp_path / "output.json"
        loader = FileLoader()
        result = await loader.load(transformed_data, file_path=str(output_file))
        
        assert result is True
        assert output_file.exists()
        
        # Verify final output
        with open(output_file, 'r') as f:
            final_data = json.load(f)
        
        assert len(final_data) == 2
        assert final_data[0]['name'] == 'John Doe'

if __name__ == "__main__":
    pytest.main([__file__, "-v"])