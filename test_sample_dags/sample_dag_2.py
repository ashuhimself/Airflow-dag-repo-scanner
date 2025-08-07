"""
Sample DAG 2 - Data Processing Pipeline (TaskFlow API)
This DAG demonstrates the TaskFlow API with dataset-driven scheduling.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Common module imports
from common.utils import data_validator, file_processor
from common.ml_models import ModelTrainer, FeatureExtractor
from common.database import DatabaseConnection

# Dataset definitions
processed_data_dataset = Dataset("s3://data-lake/processed/daily/")
ml_features_dataset = Dataset("s3://ml-pipeline/features/")
model_artifacts_dataset = Dataset("s3://ml-pipeline/models/")

default_args = {
    'owner': 'ml-engineering',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id='data_processing_pipeline',
    default_args=default_args,
    description='ML data processing and feature engineering pipeline',
    schedule=[processed_data_dataset],  # Dataset-driven scheduling
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'processing', 'dataset-driven']
)
def data_processing_dag():
    """
    Data processing pipeline that transforms raw data into ML features.
    This DAG is triggered when new processed data is available.
    """
    
    @task
    def load_and_validate_data(ds=None, **context):
        """Load processed data and perform validation."""
        try:
            # Get dataset URI from triggering DAG
            source_config = context.get('dag_run').conf or {}
            source_dataset = source_config.get('source_dataset', str(processed_data_dataset))
            
            # Load data using common utilities
            db_conn = DatabaseConnection()
            raw_data = db_conn.load_dataset(source_dataset, ds)
            
            # Validate data quality
            validation_result = data_validator.validate_ml_data(
                raw_data,
                required_columns=['user_id', 'timestamp', 'features'],
                min_rows=1000
            )
            
            if not validation_result['is_valid']:
                raise ValueError(f"Data validation failed: {validation_result['errors']}")
            
            return {
                'dataset_path': source_dataset,
                'row_count': len(raw_data),
                'validation_score': validation_result['score']
            }
            
        except Exception as e:
            # Complex error handling with multiple nested try-catch blocks
            try:
                if 'db_conn' in locals():
                    db_conn.close()
            except:
                pass
            raise ValueError(f"Data loading failed: {str(e)}")
    
    @task
    def extract_features(data_info: dict):
        """Extract and engineer features for ML models."""
        dataset_path = data_info['dataset_path']
        
        try:
            # Initialize feature extractor
            feature_extractor = FeatureExtractor()
            
            # Extract different types of features
            features = {}
            
            # This is intentionally complex to demonstrate high cyclomatic complexity
            if data_info['row_count'] > 10000:
                if data_info['validation_score'] > 0.8:
                    # Extract full feature set
                    features['demographic'] = feature_extractor.extract_demographic_features(dataset_path)
                    features['behavioral'] = feature_extractor.extract_behavioral_features(dataset_path)
                    features['temporal'] = feature_extractor.extract_temporal_features(dataset_path)
                    features['interaction'] = feature_extractor.extract_interaction_features(dataset_path)
                    
                    # Additional processing for high-quality data
                    if data_info['validation_score'] > 0.9:
                        features['advanced'] = feature_extractor.extract_advanced_features(dataset_path)
                        
                        # Even more nested logic (code smell: deep nesting)
                        for feature_type, feature_data in features.items():
                            if len(feature_data) > 100:
                                if feature_type == 'behavioral':
                                    # Apply specialized behavioral processing
                                    processed_features = file_processor.apply_behavioral_transformations(
                                        feature_data
                                    )
                                    features[feature_type] = processed_features
                                elif feature_type == 'temporal':
                                    # Apply temporal smoothing
                                    smoothed_features = file_processor.apply_temporal_smoothing(
                                        feature_data
                                    )
                                    features[feature_type] = smoothed_features
                                else:
                                    # Standard normalization
                                    normalized_features = file_processor.normalize_features(
                                        feature_data
                                    )
                                    features[feature_type] = normalized_features
                else:
                    # Reduced feature set for lower quality data
                    features['basic'] = feature_extractor.extract_basic_features(dataset_path)
            else:
                # Minimal processing for small datasets
                features['minimal'] = feature_extractor.extract_minimal_features(dataset_path)
            
            # Long function with magic numbers (code smell)
            total_features = sum(len(f) for f in features.values())
            if total_features < 50:  # Magic number
                raise ValueError("Insufficient features extracted")
            elif total_features > 1000:  # Another magic number
                # Reduce feature dimensionality
                for feature_type in features:
                    if len(features[feature_type]) > 200:  # More magic numbers
                        features[feature_type] = feature_extractor.reduce_dimensionality(
                            features[feature_type], 
                            target_size=150  # Yet another magic number
                        )
            
            return {
                'features': features,
                'feature_count': total_features,
                'feature_types': list(features.keys())
            }
            
        except Exception as e:
            # Missing specific exception handling (code smell: bare except above)
            raise RuntimeError(f"Feature extraction failed: {str(e)}")
    
    @task
    def train_models(feature_info: dict):
        """Train multiple ML models with the extracted features."""
        features = feature_info['features']
        
        model_trainer = ModelTrainer()
        model_results = {}
        
        # Train different models based on feature types available
        if 'demographic' in features and 'behavioral' in features:
            # Train comprehensive model
            model_results['comprehensive'] = model_trainer.train_comprehensive_model(
                demographic_features=features['demographic'],
                behavioral_features=features['behavioral'],
                temporal_features=features.get('temporal', []),
                interaction_features=features.get('interaction', [])
            )
        
        if 'basic' in features or 'minimal' in features:
            # Train simple model
            simple_features = features.get('basic', features.get('minimal', []))
            model_results['simple'] = model_trainer.train_simple_model(simple_features)
        
        if 'advanced' in features:
            # Train advanced model
            model_results['advanced'] = model_trainer.train_advanced_model(
                features['advanced']
            )
        
        # Validate model performance
        for model_name, model_result in model_results.items():
            if model_result['accuracy'] < 0.7:  # Magic number threshold
                print(f"Warning: {model_name} model has low accuracy: {model_result['accuracy']}")
        
        return {
            'models': model_results,
            'best_model': max(model_results.keys(), key=lambda k: model_results[k]['accuracy']),
            'model_count': len(model_results)
        }
    
    @task
    def save_artifacts(model_info: dict, feature_info: dict):
        """Save model artifacts and feature metadata.""" 
        # This function has no error handling (code smell)
        models = model_info['models']
        features = feature_info['features']
        
        # Save models to S3
        for model_name, model_data in models.items():
            s3_path = f"s3://ml-pipeline/models/{model_name}/"
            file_processor.save_model_to_s3(model_data['model'], s3_path)
        
        # Save feature metadata
        feature_metadata = {
            'feature_types': feature_info['feature_types'],
            'feature_count': feature_info['feature_count'],
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        metadata_path = f"s3://ml-pipeline/features/metadata.json"
        file_processor.save_json_to_s3(feature_metadata, metadata_path)
        
        return {
            'artifacts_saved': True,
            'model_paths': [f"s3://ml-pipeline/models/{name}/" for name in models.keys()],
            'feature_metadata_path': metadata_path
        }
    
    # Define task dependencies using TaskFlow API
    data_info = load_and_validate_data()
    feature_info = extract_features(data_info)
    model_info = train_models(feature_info)
    artifacts = save_artifacts(model_info, feature_info)
    
    # Traditional operator for triggering downstream DAG
    trigger_deployment = TriggerDagRunOperator(
        task_id='trigger_model_deployment',
        trigger_dag_id='model_deployment_pipeline',
        conf={
            'model_paths': "{{ task_instance.xcom_pull(task_ids='save_artifacts')['model_paths'] }}",
            'best_model': "{{ task_instance.xcom_pull(task_ids='train_models')['best_model'] }}"
        },
        wait_for_completion=False
    )
    
    # Set up dependencies
    artifacts >> trigger_deployment

# Create the DAG instance
processing_dag = data_processing_dag()