"""
Common utility functions used across multiple DAGs.
This module demonstrates shared code that would be analyzed by the dependency analyzer.
"""

import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

class DataValidator:
    """Utility class for data validation operations."""
    
    def __init__(self):
        self.validation_rules = {}
    
    def validate_extracted_files(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Validate extracted data files for completeness and format.
        
        Args:
            file_paths: List of file paths to validate
            
        Returns:
            Dict containing validation results
        """
        try:
            errors = []
            warnings = []
            
            for file_path in file_paths:
                # Simulate file validation logic
                if not file_path.endswith('.json'):
                    errors.append(f"Invalid file format: {file_path}")
                
                # Check file size (simplified)
                if 'empty' in file_path:
                    warnings.append(f"File appears to be empty: {file_path}")
            
            return {
                'is_valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings,
                'validated_files': len(file_paths)
            }
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return {
                'is_valid': False,
                'errors': [f"Validation error: {str(e)}"],
                'warnings': [],
                'validated_files': 0
            }
    
    def validate_ml_data(self, data: Any, required_columns: List[str], min_rows: int = 100) -> Dict[str, Any]:
        """
        Validate ML dataset for required columns and minimum row count.
        
        Args:
            data: The dataset to validate
            required_columns: List of required column names
            min_rows: Minimum number of rows required
            
        Returns:
            Dict containing validation results
        """
        errors = []
        score = 1.0
        
        # This function is intentionally long and complex (code smell)
        if not data:
            errors.append("Dataset is empty or None")
            score = 0.0
        else:
            # Simulate data validation
            row_count = len(data) if hasattr(data, '__len__') else 0
            
            if row_count < min_rows:
                errors.append(f"Insufficient rows: {row_count} < {min_rows}")
                score -= 0.3
            
            # Check for required columns (simplified)
            missing_columns = []
            for col in required_columns:
                if col not in str(data):  # Simplified check
                    missing_columns.append(col)
            
            if missing_columns:
                errors.append(f"Missing required columns: {missing_columns}")
                score -= 0.4
            
            # Additional quality checks with nested conditions (code smell: deep nesting)
            if row_count > min_rows:
                if row_count > min_rows * 2:
                    if row_count > min_rows * 5:
                        if row_count > min_rows * 10:
                            score += 0.1  # Bonus for large datasets
                            if 'user_id' in required_columns:
                                if 'timestamp' in required_columns:
                                    score += 0.05  # Additional bonus
                        else:
                            score += 0.05
                    else:
                        score += 0.02
        
        score = max(0.0, min(1.0, score))
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'score': score,
            'row_count': row_count if 'row_count' in locals() else 0
        }
    
    def run_quality_checks(self, table_name: str) -> Dict[str, Any]:
        """
        Run comprehensive quality checks on a database table.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            Dict containing quality metrics
        """
        # Simulate quality check results
        import random
        
        return {
            'row_count': random.randint(1000, 50000),
            'null_percentage': random.uniform(0, 15),
            'duplicate_count': random.randint(0, 500),
            'schema_compliance': random.choice([True, True, True, False]),  # 75% chance of True
            'data_freshness_hours': random.uniform(0, 24)
        }

class FileProcessor:
    """Utility class for file processing operations."""
    
    @staticmethod
    def process_data_file(file_path: str, remove_duplicates: bool = True, 
                         apply_business_rules: bool = False) -> Dict[str, Any]:
        """
        Process a data file with various transformations.
        
        Args:
            file_path: Path to the file to process
            remove_duplicates: Whether to remove duplicate records
            apply_business_rules: Whether to apply business logic rules
            
        Returns:
            Dict containing processed data
        """
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Simulate file processing
            processed_data = {
                'source_file': file_path,
                'records_processed': 1000,  # Magic number (code smell)
                'duplicates_removed': 50 if remove_duplicates else 0,
                'business_rules_applied': apply_business_rules,
                'processing_timestamp': datetime.now().isoformat()
            }
            
            # Complex business rule application with many conditions (code smell: high complexity)
            if apply_business_rules:
                if 'customer' in file_path:
                    processed_data['customer_rules_applied'] = True
                    if 'premium' in file_path:
                        processed_data['premium_processing'] = True
                        if 'international' in file_path:
                            processed_data['international_rules'] = True
                elif 'transaction' in file_path:
                    processed_data['transaction_rules_applied'] = True
                    if 'high_value' in file_path:
                        processed_data['fraud_check'] = True
                elif 'product' in file_path:
                    processed_data['product_rules_applied'] = True
            
            return processed_data
            
        except Exception as e:
            logger.error(f"File processing failed: {str(e)}")
            raise RuntimeError(f"Failed to process file {file_path}: {str(e)}")
    
    @staticmethod
    def publish_to_s3(table_name: str, s3_path: str) -> bool:
        """
        Publish data from database table to S3.
        
        Args:
            table_name: Name of the source table
            s3_path: Target S3 path
            
        Returns:
            Boolean indicating success
        """
        # This function has no error handling (code smell)
        logger.info(f"Publishing {table_name} to {s3_path}")
        
        # Simulate S3 upload
        return True
    
    @staticmethod
    def apply_behavioral_transformations(feature_data: List[Dict]) -> List[Dict]:
        """Apply behavioral feature transformations."""
        return [{'transformed': True, **item} for item in feature_data]
    
    @staticmethod
    def apply_temporal_smoothing(feature_data: List[Dict]) -> List[Dict]:
        """Apply temporal smoothing to features."""
        return [{'smoothed': True, **item} for item in feature_data]
    
    @staticmethod
    def normalize_features(feature_data: List[Dict]) -> List[Dict]:
        """Normalize feature values."""
        return [{'normalized': True, **item} for item in feature_data]
    
    @staticmethod
    def save_model_to_s3(model_data: Any, s3_path: str) -> bool:
        """Save ML model to S3."""
        logger.info(f"Saving model to {s3_path}")
        return True
    
    @staticmethod
    def save_json_to_s3(data: Dict, s3_path: str) -> bool:
        """Save JSON data to S3."""
        logger.info(f"Saving JSON to {s3_path}")
        return True

# Module-level utility functions (also analyzed by dependency analyzer)
def calculate_data_freshness(timestamp: datetime) -> float:
    """Calculate data freshness in hours."""
    return (datetime.now() - timestamp).total_seconds() / 3600

def format_error_message(error: Exception, context: str) -> str:
    """Format error message with context information.""" 
    return f"[{context}] {type(error).__name__}: {str(error)}"

def validate_s3_path(s3_path: str) -> bool:
    """Validate S3 path format."""
    return s3_path.startswith('s3://') and len(s3_path) > 5

# Initialize module-level instances (commonly used pattern)
data_validator = DataValidator()
file_processor = FileProcessor()