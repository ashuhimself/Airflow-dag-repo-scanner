"""
Sample DAG 1 - Data Ingestion Pipeline
This DAG demonstrates a typical data ingestion workflow with common module usage.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.datasets import Dataset

# Sample imports from common modules (would be actual imports in real scenario)
from common.utils import data_validator, file_processor
from common.database import DatabaseConnection
from common.notifications import send_alert

# Dataset definitions
raw_data_dataset = Dataset("s3://data-lake/raw/daily/")
processed_data_dataset = Dataset("s3://data-lake/processed/daily/")

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Traditional DAG definition
dag = DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    description='Daily data ingestion from external sources',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'daily', 'critical']
)

def extract_external_data(**context):
    """Extract data from external APIs and file sources."""
    try:
        # Simulate data extraction with error handling
        data_sources = ['api_endpoint_1', 'sftp_source', 'database_export']
        extracted_files = []
        
        for source in data_sources:
            # This would be actual extraction logic
            file_path = f"/tmp/extracted_{source}_{context['ds']}.json"
            extracted_files.append(file_path)
        
        # Use common module function
        validation_results = data_validator.validate_extracted_files(extracted_files)
        
        if not validation_results['is_valid']:
            send_alert(f"Data validation failed: {validation_results['errors']}")
            raise ValueError("Data validation failed")
        
        return extracted_files
    
    except Exception as e:
        send_alert(f"Data extraction failed: {str(e)}")
        raise

def transform_and_clean_data(**context):
    """Transform and clean the extracted data."""
    extracted_files = context['task_instance'].xcom_pull(task_ids='extract_data')
    
    if not extracted_files:
        raise ValueError("No extracted files found")
    
    try:
        db_conn = DatabaseConnection()
        processed_files = []
        
        for file_path in extracted_files:
            # Use common file processor
            cleaned_data = file_processor.process_data_file(
                file_path, 
                remove_duplicates=True,
                apply_business_rules=True
            )
            
            # Save to staging table
            staging_table = f"staging_data_{context['ds_nodash']}"
            db_conn.bulk_insert(staging_table, cleaned_data)
            processed_files.append(staging_table)
        
        return processed_files
    
    except Exception as e:
        send_alert(f"Data transformation failed: {str(e)}")
        raise
    finally:
        if 'db_conn' in locals():
            db_conn.close()

def data_quality_checks(**context):
    """Perform comprehensive data quality checks."""
    staging_tables = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    quality_metrics = {
        'row_count': 0,
        'null_percentage': 0,
        'duplicate_count': 0,
        'schema_compliance': True
    }
    
    try:
        db_conn = DatabaseConnection()
        
        for table in staging_tables:
            # Perform quality checks using common validation functions
            table_metrics = data_validator.run_quality_checks(table)
            
            # Aggregate metrics
            quality_metrics['row_count'] += table_metrics['row_count']
            quality_metrics['null_percentage'] = max(
                quality_metrics['null_percentage'],
                table_metrics['null_percentage']
            )
            quality_metrics['duplicate_count'] += table_metrics['duplicate_count']
            
            if not table_metrics['schema_compliance']:
                quality_metrics['schema_compliance'] = False
        
        # Check quality thresholds
        if quality_metrics['null_percentage'] > 10:
            send_alert("High null percentage detected in data")
        
        if quality_metrics['duplicate_count'] > 100:
            send_alert("High duplicate count detected")
        
        if not quality_metrics['schema_compliance']:
            raise ValueError("Schema compliance check failed")
        
        return quality_metrics
    
    except Exception as e:
        send_alert(f"Data quality checks failed: {str(e)}")
        raise
    finally:
        if 'db_conn' in locals():
            db_conn.close()

def publish_to_data_lake(**context):
    """Publish processed data to the data lake."""
    quality_metrics = context['task_instance'].xcom_pull(task_ids='quality_checks')
    staging_tables = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    try:
        # Move data from staging to production
        for table in staging_tables:
            production_path = f"s3://data-lake/processed/daily/{context['ds']}/{table}/"
            
            # This would be actual data publishing logic
            file_processor.publish_to_s3(table, production_path)
        
        # Update dataset (triggers downstream DAGs)
        context['task_instance'].xcom_push(
            key='dataset_uri', 
            value=str(processed_data_dataset)
        )
        
        # Send success notification
        send_alert(
            f"Data ingestion completed successfully. "
            f"Processed {quality_metrics['row_count']} rows."
        )
    
    except Exception as e:
        send_alert(f"Data publishing failed: {str(e)}")
        raise

# Task definitions
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_external_data,
    dag=dag,
    provide_context=True,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_and_clean_data,
    dag=dag,
    provide_context=True,
)

quality_task = PythonOperator(
    task_id='quality_checks',
    python_callable=data_quality_checks,
    dag=dag,
    provide_context=True,
)

publish_task = PythonOperator(
    task_id='publish_data',
    python_callable=publish_to_data_lake,
    dag=dag,
    provide_context=True,
)

# Trigger downstream processing DAG
trigger_processing = TriggerDagRunOperator(
    task_id='trigger_data_processing',
    trigger_dag_id='data_processing_pipeline',
    conf={'source_dataset': str(processed_data_dataset)},
    dag=dag,
    wait_for_completion=False,
)

# Task dependencies
extract_task >> transform_task >> quality_task >> publish_task >> trigger_processing