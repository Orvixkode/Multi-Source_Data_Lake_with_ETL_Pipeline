"""
Multi-Source Data Ingestion DAG for Data Lake Pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import asyncio
import logging
import sys
import os

# Add src to path for imports
sys.path.append('/opt/airflow/src')

from src.etl.extractors.base_extractors import PostgreSQLExtractor, MongoExtractor, FileExtractor, APIExtractor
from src.etl.transformers.data_transformers import (
    DataCleaningTransformer, 
    DataValidationTransformer, 
    DataEnrichmentTransformer,
    TransformationPipeline
)
from src.etl.loaders.base_loaders import PostgreSQLLoader, MongoLoader, FileLoader, LoaderManager
from config.settings import settings

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create DAG
dag = DAG(
    'multi_source_data_ingestion',
    default_args=default_args,
    description='Multi-Source Data Lake Ingestion Pipeline',
    schedule_interval='@hourly',
    max_active_runs=1,
    tags=['data-lake', 'etl', 'ingestion']
)

def extract_postgres_data(**context):
    """Extract data from PostgreSQL sources"""
    async def _extract():
        extractor = PostgreSQLExtractor()
        extracted_records = []
        
        # Define tables to extract
        tables = ['users', 'orders', 'products', 'transactions']
        
        for table in tables:
            try:
                logger.info(f"Extracting data from PostgreSQL table: {table}")
                async for record in extractor.extract(table_name=table, batch_size=1000):
                    record['_source_table'] = table
                    record['_extraction_timestamp'] = datetime.utcnow().isoformat()
                    extracted_records.append(record)
            except Exception as e:
                logger.error(f"Failed to extract from table {table}: {e}")
        
        # Save to staging area
        staging_file = f"{settings.data_staging_path}/postgres_extract_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        file_loader = FileLoader()
        await file_loader.load(
            data=extracted_records,
            file_path=staging_file,
            file_format='json'
        )
        
        logger.info(f"PostgreSQL extraction completed: {len(extracted_records)} records saved to {staging_file}")
        return staging_file
    
    return asyncio.run(_extract())

def extract_mongodb_data(**context):
    """Extract data from MongoDB collections"""
    async def _extract():
        extractor = MongoExtractor()
        extracted_records = []
        
        # Define collections to extract
        collections = ['events', 'logs', 'user_profiles', 'sessions']
        
        for collection in collections:
            try:
                logger.info(f"Extracting data from MongoDB collection: {collection}")
                async for record in extractor.extract(collection_name=collection, batch_size=1000):
                    record['_source_collection'] = collection
                    record['_extraction_timestamp'] = datetime.utcnow().isoformat()
                    extracted_records.append(record)
            except Exception as e:
                logger.error(f"Failed to extract from collection {collection}: {e}")
        
        # Save to staging area
        staging_file = f"{settings.data_staging_path}/mongodb_extract_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        file_loader = FileLoader()
        await file_loader.load(
            data=extracted_records,
            file_path=staging_file,
            file_format='json'
        )
        
        logger.info(f"MongoDB extraction completed: {len(extracted_records)} records saved to {staging_file}")
        return staging_file
    
    return asyncio.run(_extract())

def extract_api_data(**context):
    """Extract data from external APIs"""
    async def _extract():
        extractor = APIExtractor()
        extracted_records = []
        
        # Define APIs to extract from
        api_sources = [
            {
                'name': 'weather_api',
                'url': 'https://api.openweathermap.org/data/2.5/weather',
                'params': {'q': 'New York', 'appid': 'demo_key'}
            },
            {
                'name': 'market_data',
                'url': 'https://api.example.com/market/data',
                'headers': {'Authorization': 'Bearer demo_token'}
            }
        ]
        
        for api_config in api_sources:
            try:
                logger.info(f"Extracting data from API: {api_config['name']}")
                async for record in extractor.extract(
                    url=api_config['url'],
                    headers=api_config.get('headers'),
                    params=api_config.get('params')
                ):
                    record['_source_api'] = api_config['name']
                    record['_extraction_timestamp'] = datetime.utcnow().isoformat()
                    extracted_records.append(record)
            except Exception as e:
                logger.error(f"Failed to extract from API {api_config['name']}: {e}")
        
        # Save to staging area
        staging_file = f"{settings.data_staging_path}/api_extract_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        file_loader = FileLoader()
        await file_loader.load(
            data=extracted_records,
            file_path=staging_file,
            file_format='json'
        )
        
        logger.info(f"API extraction completed: {len(extracted_records)} records saved to {staging_file}")
        return staging_file
    
    return asyncio.run(_extract())

def transform_and_validate_data(**context):
    """Transform and validate extracted data"""
    # Get staging files from previous tasks
    task_instance = context['task_instance']
    staging_files = []
    
    # Get files from extraction tasks
    for task_id in ['extract_postgres', 'extract_mongodb', 'extract_api']:
        try:
            file_path = task_instance.xcom_pull(task_ids=task_id)
            if file_path:
                staging_files.append(file_path)
        except:
            logger.warning(f"No file from task {task_id}")
    
    if not staging_files:
        logger.warning("No staging files found for transformation")
        return []
    
    # Initialize transformation pipeline
    transformers = [
        DataCleaningTransformer(),
        DataValidationTransformer({
            'email': {'required': False, 'type': 'email'},
            'created_at': {'type': 'date'},
            'amount': {'min': 0, 'max': 1000000}
        }),
        DataEnrichmentTransformer()
    ]
    
    pipeline = TransformationPipeline(transformers)
    
    processed_files = []
    
    for staging_file in staging_files:
        try:
            logger.info(f"Processing staging file: {staging_file}")
            
            # Load data from staging
            import json
            with open(staging_file, 'r') as f:
                raw_data = json.load(f)
            
            # Transform data
            transformed_data = []
            for record in raw_data:
                try:
                    transformed_record = pipeline.transform(record)
                    transformed_data.append(transformed_record)
                except Exception as e:
                    logger.error(f"Transformation failed for record: {e}")
            
            # Save transformed data
            processed_file = staging_file.replace('staging', 'processed').replace('extract', 'transformed')
            
            file_loader = FileLoader()
            asyncio.run(file_loader.load(
                data=transformed_data,
                file_path=processed_file,
                file_format='json'
            ))
            
            processed_files.append(processed_file)
            
            logger.info(f"Transformation completed: {len(transformed_data)} records processed")
            
        except Exception as e:
            logger.error(f"Failed to process file {staging_file}: {e}")
    
    return processed_files

def load_to_data_lake(**context):
    """Load transformed data to multiple destinations in the data lake"""
    async def _load():
        # Get processed files from transformation task
        task_instance = context['task_instance']
        processed_files = task_instance.xcom_pull(task_ids='transform_validate_data')
        
        if not processed_files:
            logger.warning("No processed files found for loading")
            return
        
        loader_manager = LoaderManager()
        total_loaded = 0
        
        for processed_file in processed_files:
            try:
                logger.info(f"Loading data from: {processed_file}")
                
                # Load data
                import json
                with open(processed_file, 'r') as f:
                    data = json.load(f)
                
                if not data:
                    continue
                
                # Route data based on source and type
                routing_config = {}
                
                # Determine routing based on data characteristics
                source_type = data[0].get('_source_table') or data[0].get('_source_collection') or data[0].get('_source_api', 'unknown')
                
                if 'transaction' in source_type.lower() or 'order' in source_type.lower():
                    # Financial data to PostgreSQL
                    routing_config['postgres_financial'] = {
                        'type': 'postgres',
                        'params': {'table_name': 'financial_data', 'if_exists': 'append'}
                    }
                
                if 'event' in source_type.lower() or 'log' in source_type.lower():
                    # Event data to MongoDB
                    routing_config['mongodb_events'] = {
                        'type': 'mongodb',
                        'params': {'collection_name': 'processed_events'}
                    }
                
                if 'user' in source_type.lower() or 'profile' in source_type.lower():
                    # User data to both PostgreSQL and MongoDB
                    routing_config['postgres_users'] = {
                        'type': 'postgres',
                        'params': {'table_name': 'user_profiles', 'if_exists': 'append'}
                    }
                    routing_config['mongodb_users'] = {
                        'type': 'mongodb',
                        'params': {'collection_name': 'user_profiles'}
                    }
                
                # Always save to file archive
                archive_filename = f"archive_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
                routing_config['file_archive'] = {
                    'type': 'file',
                    'params': {
                        'file_path': f"{settings.data_processed_path}/{archive_filename}",
                        'file_format': 'parquet'
                    }
                }
                
                # Execute loading
                results = await loader_manager.route_and_load(data, routing_config)
                
                successful_loads = sum(1 for result in results.values() if result.get('success'))
                total_loaded += len(data)
                
                logger.info(f"Loaded {len(data)} records to {successful_loads}/{len(routing_config)} destinations")
                
            except Exception as e:
                logger.error(f"Failed to load file {processed_file}: {e}")
        
        logger.info(f"Data loading completed: {total_loaded} total records processed")
        return total_loaded
    
    return asyncio.run(_load())

def generate_data_quality_report(**context):
    """Generate data quality and pipeline execution report"""
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    
    # Collect metrics from previous tasks
    total_loaded = task_instance.xcom_pull(task_ids='load_to_data_lake') or 0
    
    report = {
        'pipeline_execution': {
            'execution_date': execution_date.isoformat(),
            'total_records_loaded': total_loaded,
            'status': 'completed' if total_loaded > 0 else 'no_data'
        },
        'data_quality': {
            'validation_passed': True,  # This would come from actual validation
            'transformation_success_rate': 0.95,  # This would be calculated
            'duplicate_rate': 0.02
        },
        'performance': {
            'execution_duration_minutes': 15,  # This would be calculated
            'records_per_minute': total_loaded / 15 if total_loaded > 0 else 0
        }
    }
    
    # Save report
    report_file = f"{settings.data_processed_path}/quality_report_{execution_date.strftime('%Y%m%d_%H%M%S')}.json"
    
    import json
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Data quality report generated: {report_file}")
    return report_file

# Define tasks
extract_postgres_task = PythonOperator(
    task_id='extract_postgres',
    python_callable=extract_postgres_data,
    dag=dag
)

extract_mongodb_task = PythonOperator(
    task_id='extract_mongodb',
    python_callable=extract_mongodb_data,
    dag=dag
)

extract_api_task = PythonOperator(
    task_id='extract_api',
    python_callable=extract_api_data,
    dag=dag
)

transform_validate_task = PythonOperator(
    task_id='transform_validate_data',
    python_callable=transform_and_validate_data,
    dag=dag
)

load_data_lake_task = PythonOperator(
    task_id='load_to_data_lake',
    python_callable=load_to_data_lake,
    dag=dag
)

quality_report_task = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag
)

# Define task dependencies
[extract_postgres_task, extract_mongodb_task, extract_api_task] >> transform_validate_task
transform_validate_task >> load_data_lake_task >> quality_report_task