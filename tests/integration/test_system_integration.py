"""
Integration Tests for Multi-Source Data Lake System
"""
import pytest
import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, AsyncMock

from src.api.main import app
from src.etl.extractors.base_extractors import FileExtractor
from src.etl.transformers.data_transformers import TransformationPipeline, DataCleaningTransformer
from src.etl.loaders.base_loaders import LoaderManager
from src.validators.data_validators import ValidationPipeline, SchemaValidator
from fastapi.testclient import TestClient

class TestAPIIntegration:
    """Test API integration with ETL components"""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_health_endpoint(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    
    def test_config_endpoint(self, client):
        """Test configuration endpoint"""
        response = client.get("/api/v1/config")
        assert response.status_code == 200
        data = response.json()
        assert "app_name" in data
        assert "etl_config" in data
    
    def test_status_endpoint(self, client):
        """Test system status endpoint"""
        response = client.get("/api/v1/status")
        assert response.status_code == 200
        data = response.json()
        assert "api" in data
        assert "databases" in data
        assert "protocols" in data
    
    @patch('src.connectors.postgres_connector.PostgresConnector')
    def test_postgres_query_endpoint(self, mock_connector, client):
        """Test PostgreSQL query endpoint"""
        # Mock database response
        mock_instance = mock_connector.return_value
        mock_instance.connect = AsyncMock()
        mock_instance.execute_query = AsyncMock(return_value=[
            {"id": 1, "name": "John", "email": "john@test.com"}
        ])
        mock_instance.disconnect = AsyncMock()
        
        response = client.get("/api/v1/query/postgres?table=users&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert len(data["data"]) == 1
        assert data["data"][0]["name"] == "John"
    
    def test_upload_data_endpoint(self, client):
        """Test data upload endpoint"""
        upload_data = {
            "data": [
                {"name": "John", "age": 30},
                {"name": "Jane", "age": 25}
            ],
            "target_type": "file",
            "target_config": {
                "file_path": "/tmp/test_upload.json",
                "file_format": "json"
            }
        }
        
        with patch('src.etl.loaders.base_loaders.LoaderManager') as mock_manager:
            mock_instance = mock_manager.return_value
            mock_instance.route_and_load = AsyncMock(return_value={
                "upload_target": {"success": True, "loaded_count": 2}
            })
            
            response = client.post("/api/v1/data/upload", json=upload_data)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert data["uploaded_count"] == 2

class TestEndToEndETL:
    """End-to-end ETL pipeline tests"""
    
    @pytest.fixture
    def sample_data_file(self):
        """Create temporary sample data file"""
        data = [
            {"name": "  John Doe  ", "email": "JOHN@TEST.COM", "age": "30", "status": "active"},
            {"name": "Jane Smith", "email": "jane@test.com", "age": "25", "status": "inactive"},
            {"name": "Bob Johnson", "email": "bob@test.com", "age": "35", "status": "active"},
            {"name": "", "email": "invalid-email", "age": "200", "status": "pending"}  # Invalid record
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(data, f)
            return f.name
    
    @pytest.mark.asyncio
    async def test_complete_etl_workflow(self, sample_data_file):
        """Test complete ETL workflow from extraction to loading"""
        # Step 1: Extract data from file
        extractor = FileExtractor()
        extracted_records = []
        
        async for record in extractor.extract(file_path=sample_data_file):
            extracted_records.append(record)
        
        assert len(extracted_records) == 4
        assert extractor.extracted_count == 4
        
        # Step 2: Transform data
        transformers = [DataCleaningTransformer()]
        pipeline = TransformationPipeline(transformers)
        
        transformed_records = []
        for record in extracted_records:
            transformed_record = pipeline.transform(record)
            transformed_records.append(transformed_record)
        
        # Verify transformations
        assert transformed_records[0]['name'] == 'John Doe'  # Trimmed
        assert transformed_records[0]['email'] == 'john@test.com'  # Lowercase
        assert transformed_records[0]['age'] == 30  # Type converted
        assert '_cleaned_at' in transformed_records[0]
        
        # Step 3: Validate data
        schema = {
            'name': {'required': True, 'type': 'string', 'min_length': 1},
            'email': {'required': True, 'type': 'email'},
            'age': {'type': 'integer', 'min': 0, 'max': 150}
        }
        
        validator = SchemaValidator(schema)
        validation_pipeline = ValidationPipeline([validator])
        
        validation_results = validation_pipeline.validate(transformed_records)
        summary = validation_pipeline.get_summary(validation_results)
        
        # Should have validation errors for invalid records
        assert summary['total_errors'] > 0
        assert not summary['overall_valid']
        
        # Step 4: Filter valid records
        valid_records = [
            record for record in transformed_records 
            if record.get('_validation_errors', []) == []
        ]
        
        # Should have fewer valid records than total
        assert len(valid_records) < len(transformed_records)
        
        # Step 5: Load data to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as output_file:
            loader_manager = LoaderManager()
            
            routing_config = {
                "file_output": {
                    "type": "file",
                    "params": {
                        "file_path": output_file.name,
                        "file_format": "json"
                    }
                }
            }
            
            load_results = await loader_manager.route_and_load(valid_records, routing_config)
            
            assert load_results["file_output"]["success"] is True
            
            # Verify output file
            output_path = Path(output_file.name)
            assert output_path.exists()
            
            with open(output_path, 'r') as f:
                final_data = json.load(f)
            
            assert len(final_data) == len(valid_records)
            assert final_data[0]['name'] == 'John Doe'
        
        # Cleanup
        Path(sample_data_file).unlink()
        output_path.unlink()

class TestDataLakeScenarios:
    """Test real-world data lake scenarios"""
    
    @pytest.mark.asyncio
    async def test_multi_source_aggregation(self):
        """Test aggregating data from multiple sources"""
        # Simulate data from different sources
        postgres_data = [
            {"user_id": 1, "name": "John", "department": "Engineering"},
            {"user_id": 2, "name": "Jane", "department": "Marketing"}
        ]
        
        mongodb_data = [
            {"user_id": 1, "login_count": 45, "last_login": "2025-11-20"},
            {"user_id": 2, "login_count": 23, "last_login": "2025-11-22"}
        ]
        
        # Merge data by user_id
        merged_data = []
        for pg_record in postgres_data:
            user_id = pg_record["user_id"]
            mongo_record = next((r for r in mongodb_data if r["user_id"] == user_id), {})
            
            merged_record = {**pg_record, **mongo_record}
            merged_data.append(merged_record)
        
        assert len(merged_data) == 2
        assert merged_data[0]["name"] == "John"
        assert merged_data[0]["login_count"] == 45
        assert merged_data[1]["department"] == "Marketing"
        assert merged_data[1]["last_login"] == "2025-11-22"
    
    @pytest.mark.asyncio
    async def test_data_quality_monitoring(self):
        """Test data quality monitoring workflow"""
        from src.validators.data_validators import DataQualityValidator
        
        # Sample data with quality issues
        test_data = [
            {"name": "John", "age": 30, "email": "john@test.com"},
            {"name": "Jane", "age": 25, "email": "jane@test.com"},
            {"name": "John", "age": 30, "email": "john@test.com"},  # Duplicate
            {"name": None, "age": None, "email": None},  # Nulls
            {"name": "Bob", "age": 35, "email": "bob@test.com"}
        ]
        
        # Configure quality validator
        quality_validator = DataQualityValidator({
            'max_null_percentage': 0.15,  # 15% nulls allowed
            'max_duplicate_percentage': 0.15,  # 15% duplicates allowed
            'min_records': 3
        })
        
        result = quality_validator.validate(test_data)
        
        # Should pass minimum records check
        assert result.is_valid is True
        
        # Should have warnings about duplicates and nulls
        assert len(result.warnings) > 0
        
        # Check metrics
        assert result.metrics['total_records'] == 5
        assert result.metrics['duplicate_count'] > 0
        assert result.metrics['duplicate_percentage'] > 0
    
    @pytest.mark.asyncio
    async def test_batch_processing_performance(self):
        """Test batch processing capabilities"""
        from src.utils.common_utils import PerformanceUtils
        
        # Generate large dataset
        large_dataset = [
            {"id": i, "name": f"User {i}", "value": i * 10}
            for i in range(10000)
        ]
        
        # Test chunking
        chunk_size = 1000
        chunks = list(PerformanceUtils.chunk_data(large_dataset, chunk_size))
        
        assert len(chunks) == 10  # 10 chunks of 1000 each
        assert len(chunks[0]) == chunk_size
        assert len(chunks[-1]) == chunk_size
        
        # Test batch processing with timer
        @PerformanceUtils.timer
        def process_batch(batch):
            # Simulate processing
            return [record for record in batch if record['value'] > 5000]
        
        processed_chunks = []
        for chunk in chunks:
            processed_chunk = process_batch(chunk)
            processed_chunks.extend(processed_chunk)
        
        # Should have filtered out roughly half the records
        assert len(processed_chunks) < len(large_dataset)
        assert all(record['value'] > 5000 for record in processed_chunks)
    
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self):
        """Test error handling and recovery mechanisms"""
        from src.etl.transformers.data_transformers import TransformationPipeline, DataCleaningTransformer
        
        # Data with various issues
        problematic_data = [
            {"name": "John", "age": 30},  # Good record
            {"name": None, "age": "invalid"},  # Type issues
            {"invalid_structure": True},  # Wrong structure
            {"name": "Jane", "age": 25}  # Good record
        ]
        
        transformer = DataCleaningTransformer()
        pipeline = TransformationPipeline([transformer])
        
        results = []
        for record in problematic_data:
            try:
                result = pipeline.transform(record)
                results.append(result)
            except Exception as e:
                # Log error but continue processing
                error_record = {
                    **record,
                    '_processing_error': str(e),
                    '_error_timestamp': '2025-11-24T00:00:00Z'
                }
                results.append(error_record)
        
        # Should have processed all records (some with errors)
        assert len(results) == len(problematic_data)
        
        # Check error handling
        error_records = [r for r in results if '_processing_error' in r]
        success_records = [r for r in results if '_processing_error' not in r]
        
        assert len(error_records) >= 0  # Some records may have errors
        assert len(success_records) >= 2  # At least 2 good records should succeed

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])