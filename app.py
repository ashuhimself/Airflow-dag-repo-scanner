from flask import Flask, render_template, request, jsonify
from analyzers.repository_analyzer import RepositoryAnalyzer
from analyzers.dag_analyzer import DAGAnalyzer
from analyzers.quality_analyzer import QualityAnalyzer
from analyzers.impact_analyzer import ImpactAnalyzer
import os
import traceback
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dag-analyzer-secret-key'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/analyze', methods=['POST'])
def analyze():
    repo_analyzer = None
    try:
        # Validate request
        if not request.is_json:
            return jsonify({'error': 'Request must be JSON'}), 400
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Empty request body'}), 400
        
        source_type = data.get('source_type')
        source_path = data.get('source_path')
        
        # Validate inputs
        if not source_type or not source_path:
            return jsonify({'error': 'Missing source type or path'}), 400
        
        if source_type not in ['git', 'local']:
            return jsonify({'error': 'Invalid source type. Must be "git" or "local"'}), 400
        
        source_path = source_path.strip()
        if not source_path:
            return jsonify({'error': 'Source path cannot be empty'}), 400
        
        # Initialize repository analyzer
        repo_analyzer = RepositoryAnalyzer()
        temp_dir = None
        
        try:
            # Get the working directory
            if source_type == 'git':
                logging.info(f'Cloning repository: {source_path}')
                temp_dir = repo_analyzer.clone_repository(source_path)
                working_dir = temp_dir
            else:
                logging.info(f'Analyzing local directory: {source_path}')
                working_dir = source_path
                repo_analyzer.validate_local_directory(working_dir)
            
            # Initialize analyzers
            logging.info('Initializing analyzers')
            dag_analyzer = DAGAnalyzer(working_dir)
            quality_analyzer = QualityAnalyzer(working_dir)
            impact_analyzer = ImpactAnalyzer(working_dir)
            
            # Perform analysis
            logging.info('Discovering DAGs')
            dags = dag_analyzer.discover_dags()
            
            if not dags:
                return jsonify({'error': 'No DAGs found in the repository. Please ensure the directory contains Airflow DAG files.'}), 404
            
            logging.info(f'Found {len(dags)} DAGs, analyzing dependencies')
            code_dependencies = dag_analyzer.analyze_code_dependencies(dags)
            
            logging.info('Analyzing orchestration dependencies')
            orchestration_graph = dag_analyzer.analyze_orchestration_dependencies(dags)
            
            logging.info('Analyzing code quality')
            quality_metrics = quality_analyzer.analyze_quality(dags)
            
            logging.info('Analyzing impact')
            impact_data = impact_analyzer.analyze_impact(dags, code_dependencies)
            
            result = {
                'dags': dags,
                'code_dependencies': code_dependencies,
                'orchestration_graph': orchestration_graph,
                'quality_metrics': quality_metrics,
                'impact_analysis': impact_data,
                'metadata': {
                    'total_dags': len(dags),
                    'analysis_timestamp': traceback.format_exc()[:19] if traceback.format_exc() else None,
                    'source_type': source_type
                }
            }
            
            logging.info('Analysis completed successfully')
            return jsonify(result)
            
        finally:
            # Cleanup temp directory
            if temp_dir and os.path.exists(temp_dir):
                try:
                    repo_analyzer.cleanup_temp_directories()
                except Exception as cleanup_error:
                    logging.warning(f'Failed to cleanup temp directory: {cleanup_error}')
                
    except ValueError as e:
        logging.error(f'Validation error: {str(e)}')
        return jsonify({'error': f'Invalid input: {str(e)}'}), 400
    except FileNotFoundError as e:
        logging.error(f'File not found: {str(e)}')
        return jsonify({'error': f'File or directory not found: {str(e)}'}), 404
    except PermissionError as e:
        logging.error(f'Permission error: {str(e)}')
        return jsonify({'error': f'Permission denied: {str(e)}'}), 403
    except Exception as e:
        logging.error(f'Analysis failed: {str(e)}', exc_info=True)
        return jsonify({
            'error': f'Analysis failed: {str(e)}',
            'type': type(e).__name__
        }), 500

@app.route('/api/scannable-files', methods=['POST'])
def get_scannable_files():
    """Get all files that can be scanned for impact analysis."""
    repo_analyzer = None
    try:
        # Validate request
        if not request.is_json:
            return jsonify({'error': 'Request must be JSON'}), 400
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Empty request body'}), 400
        
        source_type = data.get('source_type')
        source_path = data.get('source_path')
        
        if not all([source_type, source_path]):
            return jsonify({'error': 'Missing required parameters: source_type, source_path'}), 400
        
        if source_type not in ['git', 'local']:
            return jsonify({'error': 'Invalid source type. Must be "git" or "local"'}), 400
        
        repo_analyzer = RepositoryAnalyzer()
        temp_dir = None
        
        try:
            # Get working directory
            if source_type == 'git':
                temp_dir = repo_analyzer.clone_repository(source_path.strip())
                working_dir = temp_dir
            else:
                working_dir = source_path.strip()
                repo_analyzer.validate_local_directory(working_dir)
            
            # Get scannable files
            impact_analyzer = ImpactAnalyzer(working_dir)
            scannable_files = impact_analyzer.get_all_scannable_files()
            
            return jsonify({
                'scannable_files': scannable_files,
                'total_files': len(scannable_files)
            })
            
        finally:
            if temp_dir and os.path.exists(temp_dir):
                try:
                    repo_analyzer.cleanup_temp_directories()
                except Exception as cleanup_error:
                    logging.warning(f'Failed to cleanup temp directory: {cleanup_error}')
                    
    except Exception as e:
        logging.error(f'Failed to get scannable files: {str(e)}', exc_info=True)
        return jsonify({
            'error': f'Failed to get scannable files: {str(e)}',
            'type': type(e).__name__
        }), 500

@app.route('/api/impact-analysis', methods=['POST'])
def impact_analysis():
    repo_analyzer = None
    try:
        # Validate request
        if not request.is_json:
            return jsonify({'error': 'Request must be JSON'}), 400
        
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Empty request body'}), 400
        
        source_type = data.get('source_type')
        source_path = data.get('source_path')
        target_file = data.get('target_file')
        target_function = data.get('target_function')
        
        # Validate required parameters
        if not all([source_type, source_path, target_file]):
            return jsonify({'error': 'Missing required parameters: source_type, source_path, target_file'}), 400
        
        if source_type not in ['git', 'local']:
            return jsonify({'error': 'Invalid source type. Must be "git" or "local"'}), 400
        
        # Clean inputs
        source_path = source_path.strip()
        target_file = target_file.strip()
        target_function = target_function.strip() if target_function else None
        
        repo_analyzer = RepositoryAnalyzer()
        temp_dir = None
        
        try:
            # Get working directory
            if source_type == 'git':
                logging.info(f'Cloning repository for impact analysis: {source_path}')
                temp_dir = repo_analyzer.clone_repository(source_path)
                working_dir = temp_dir
            else:
                logging.info(f'Analyzing local directory for impact: {source_path}')
                working_dir = source_path
                repo_analyzer.validate_local_directory(working_dir)
            
            # Verify target file exists
            target_file_path = os.path.join(working_dir, target_file)
            if not os.path.exists(target_file_path):
                return jsonify({'error': f'Target file not found: {target_file}'}), 404
            
            # Perform impact analysis
            logging.info(f'Analyzing impact for file: {target_file}')
            impact_analyzer = ImpactAnalyzer(working_dir)
            impact_result = impact_analyzer.analyze_change_impact(target_file, target_function)
            
            logging.info('Impact analysis completed successfully')
            return jsonify(impact_result)
            
        finally:
            # Cleanup temp directory
            if temp_dir and os.path.exists(temp_dir):
                try:
                    repo_analyzer.cleanup_temp_directories()
                except Exception as cleanup_error:
                    logging.warning(f'Failed to cleanup temp directory: {cleanup_error}')
                
    except ValueError as e:
        logging.error(f'Validation error in impact analysis: {str(e)}')
        return jsonify({'error': f'Invalid input: {str(e)}'}), 400
    except FileNotFoundError as e:
        logging.error(f'File not found in impact analysis: {str(e)}')
        return jsonify({'error': f'File not found: {str(e)}'}), 404
    except Exception as e:
        logging.error(f'Impact analysis failed: {str(e)}', exc_info=True)
        return jsonify({
            'error': f'Impact analysis failed: {str(e)}',
            'type': type(e).__name__
        }), 500

@app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(405)
def method_not_allowed_error(error):
    return jsonify({'error': 'Method not allowed'}), 405

@app.errorhandler(500)
def internal_error(error):
    logging.error(f'Internal server error: {str(error)}')
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5555)