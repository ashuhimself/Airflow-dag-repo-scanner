import ast
import os
import re
from collections import defaultdict
from .repository_analyzer import RepositoryAnalyzer

class DAGAnalyzer:
    """Analyzes DAG files and their dependencies using Python AST parsing."""
    
    def __init__(self, directory):
        self.directory = directory
        self.repo_analyzer = RepositoryAnalyzer()
        self.python_files = self.repo_analyzer.get_python_files(directory)
        
    def discover_dags(self):
        """Discover all DAG files by scanning for DAG patterns."""
        dags = []
        
        for file_path in self.python_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Parse the AST
                tree = ast.parse(content, filename=file_path)
                dag_info = self._extract_dag_info(tree, file_path, content)
                
                if dag_info:
                    dags.extend(dag_info)
                    
            except (SyntaxError, UnicodeDecodeError) as e:
                continue  # Skip files with syntax errors or encoding issues
            except Exception as e:
                continue  # Skip files that can't be processed
        
        return dags
    
    def _extract_dag_info(self, tree, file_path, content):
        """Extract DAG information from AST."""
        dags = []
        
        for node in ast.walk(tree):
            # Look for @dag decorator
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    if self._is_dag_decorator(decorator):
                        dag_info = self._extract_dag_from_function(node, file_path, content)
                        if dag_info:
                            dags.append(dag_info)
            
            # Look for DAG() constructor calls
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        if isinstance(node.value, ast.Call) and self._is_dag_constructor(node.value):
                            dag_info = self._extract_dag_from_constructor(node, file_path, content)
                            if dag_info:
                                dags.append(dag_info)
        
        return dags
    
    def _is_dag_decorator(self, decorator):
        """Check if a decorator is a DAG decorator."""
        if isinstance(decorator, ast.Name):
            return decorator.id == 'dag'
        elif isinstance(decorator, ast.Call):
            if isinstance(decorator.func, ast.Name):
                return decorator.func.id == 'dag'
        return False
    
    def _is_dag_constructor(self, call_node):
        """Check if a call is a DAG constructor."""
        if isinstance(call_node.func, ast.Name):
            return call_node.func.id == 'DAG'
        elif isinstance(call_node.func, ast.Attribute):
            return call_node.func.attr == 'DAG'
        return False
    
    def _extract_dag_from_function(self, func_node, file_path, content):
        """Extract DAG information from a function with @dag decorator."""
        dag_id = func_node.name
        
        # Try to extract parameters from decorator
        decorator_params = {}
        for decorator in func_node.decorator_list:
            if self._is_dag_decorator(decorator) and isinstance(decorator, ast.Call):
                for keyword in decorator.keywords:
                    if isinstance(keyword.value, ast.Constant):
                        decorator_params[keyword.arg] = keyword.value.value
        
        return {
            'dag_id': dag_id,
            'file_path': file_path,
            'relative_path': os.path.relpath(file_path, self.directory),
            'type': 'taskflow',
            'line_number': func_node.lineno,
            'parameters': decorator_params,
            'imports': self._extract_imports_from_file(file_path),
            'tasks': self._extract_tasks_from_function(func_node)
        }
    
    def _extract_dag_from_constructor(self, assign_node, file_path, content):
        """Extract DAG information from DAG constructor."""
        if not assign_node.targets or not isinstance(assign_node.targets[0], ast.Name):
            return None
        
        dag_variable = assign_node.targets[0].id
        dag_id = dag_variable
        
        # Extract parameters from DAG constructor
        constructor_params = {}
        if isinstance(assign_node.value, ast.Call):
            for keyword in assign_node.value.keywords:
                if isinstance(keyword.value, ast.Constant):
                    constructor_params[keyword.arg] = keyword.value.value
                    if keyword.arg == 'dag_id':
                        dag_id = keyword.value.value
        
        return {
            'dag_id': dag_id,
            'file_path': file_path,
            'relative_path': os.path.relpath(file_path, self.directory),
            'type': 'traditional',
            'line_number': assign_node.lineno,
            'parameters': constructor_params,
            'imports': self._extract_imports_from_file(file_path),
            'tasks': []  # Will be populated by task analysis
        }
    
    def _extract_imports_from_file(self, file_path):
        """Extract all imports from a file."""
        imports = {
            'modules': [],
            'functions': [],
            'classes': [],
            'from_common': []
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content, filename=file_path)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports['modules'].append({
                            'name': alias.name,
                            'alias': alias.asname,
                            'line_number': node.lineno
                        })
                
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ''
                    
                    # Check if importing from common/ directory
                    is_common = 'common' in module
                    
                    for alias in node.names:
                        import_info = {
                            'module': module,
                            'name': alias.name,
                            'alias': alias.asname,
                            'line_number': node.lineno
                        }
                        
                        if is_common:
                            imports['from_common'].append(import_info)
                        else:
                            imports['functions'].append(import_info)
        
        except Exception:
            pass  # Return empty imports on error
        
        return imports
    
    def _extract_tasks_from_function(self, func_node):
        """Extract task information from a taskflow DAG function."""
        tasks = []
        
        for node in ast.walk(func_node):
            if isinstance(node, ast.Call):
                # Look for task decorator calls
                if hasattr(node.func, 'attr') and node.func.attr in ['task', 'task_group']:
                    task_info = {
                        'type': node.func.attr,
                        'line_number': node.lineno,
                        'parameters': {}
                    }
                    
                    # Extract parameters
                    for keyword in node.keywords:
                        if isinstance(keyword.value, ast.Constant):
                            task_info['parameters'][keyword.arg] = keyword.value.value
                    
                    tasks.append(task_info)
        
        return tasks
    
    def analyze_code_dependencies(self, dags):
        """Analyze code dependencies for all DAGs."""
        dependencies = {}
        common_usage = defaultdict(list)
        
        for dag in dags:
            dag_deps = {
                'common_imports': [],
                'external_imports': [],
                'function_usage': [],
                'class_usage': []
            }
            
            # Analyze imports from common/ directories
            for imp in dag['imports']['from_common']:
                dag_deps['common_imports'].append(imp)
                common_usage[f"{imp['module']}.{imp['name']}"].append({
                    'dag_id': dag['dag_id'],
                    'file_path': dag['relative_path'],
                    'line_number': imp['line_number']
                })
            
            # Analyze other imports
            for imp in dag['imports']['functions']:
                dag_deps['external_imports'].append(imp)
            
            dependencies[dag['dag_id']] = dag_deps
        
        return {
            'dag_dependencies': dependencies,
            'common_usage_map': dict(common_usage)
        }
    
    def analyze_orchestration_dependencies(self, dags):
        """Analyze DAG orchestration dependencies."""
        orchestration_graph = {
            'nodes': [],
            'edges': [],
            'trigger_relationships': [],
            'dataset_dependencies': []
        }
        
        # Create nodes for each DAG
        for dag in dags:
            node = {
                'id': dag['dag_id'],
                'label': dag['dag_id'],
                'file_path': dag['relative_path'],
                'type': dag['type'],
                'schedule': dag['parameters'].get('schedule', 'None'),
                'catchup': dag['parameters'].get('catchup', True),
                'group': 'dag'
            }
            orchestration_graph['nodes'].append(node)
        
        # Analyze each DAG file for trigger relationships
        for dag in dags:
            triggers, datasets = self._extract_trigger_relationships(dag['file_path'])
            
            for trigger in triggers:
                orchestration_graph['trigger_relationships'].append({
                    'source_dag': dag['dag_id'],
                    'target_dag': trigger['target_dag'],
                    'trigger_type': 'TriggerDagRunOperator',
                    'task_id': trigger['task_id'],
                    'line_number': trigger['line_number']
                })
                
                # Add edge to graph
                orchestration_graph['edges'].append({
                    'from': dag['dag_id'],
                    'to': trigger['target_dag'],
                    'label': f"Trigger ({trigger['task_id']})",
                    'color': '#FF6B6B',
                    'arrows': 'to'
                })
            
            for dataset in datasets:
                orchestration_graph['dataset_dependencies'].append({
                    'dag_id': dag['dag_id'],
                    'dataset': dataset['dataset'],
                    'type': dataset['type'],  # 'produces' or 'consumes'
                    'line_number': dataset['line_number']
                })
        
        return orchestration_graph
    
    def _extract_trigger_relationships(self, file_path):
        """Extract TriggerDagRunOperator and Dataset dependencies from a file."""
        triggers = []
        datasets = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content, filename=file_path)
            
            for node in ast.walk(tree):
                # Look for TriggerDagRunOperator
                if isinstance(node, ast.Call):
                    if self._is_trigger_dag_operator(node):
                        trigger_info = self._extract_trigger_info(node)
                        if trigger_info:
                            triggers.append(trigger_info)
                
                # Look for Dataset usage
                elif isinstance(node, ast.Assign):
                    dataset_info = self._extract_dataset_info(node)
                    if dataset_info:
                        datasets.extend(dataset_info)
        
        except Exception:
            pass  # Skip files with parsing errors
        
        return triggers, datasets
    
    def _is_trigger_dag_operator(self, call_node):
        """Check if a call is TriggerDagRunOperator."""
        if isinstance(call_node.func, ast.Name):
            return call_node.func.id == 'TriggerDagRunOperator'
        elif isinstance(call_node.func, ast.Attribute):
            return call_node.func.attr == 'TriggerDagRunOperator'
        return False
    
    def _extract_trigger_info(self, call_node):
        """Extract trigger information from TriggerDagRunOperator call."""
        trigger_info = {
            'target_dag': None,
            'task_id': None,
            'line_number': call_node.lineno
        }
        
        for keyword in call_node.keywords:
            if keyword.arg == 'trigger_dag_id' and isinstance(keyword.value, ast.Constant):
                trigger_info['target_dag'] = keyword.value.value
            elif keyword.arg == 'task_id' and isinstance(keyword.value, ast.Constant):
                trigger_info['task_id'] = keyword.value.value
        
        return trigger_info if trigger_info['target_dag'] else None
    
    def _extract_dataset_info(self, assign_node):
        """Extract Dataset information from assignments."""
        datasets = []
        
        # This is a simplified implementation - you might need to enhance
        # this based on your specific Dataset usage patterns
        if isinstance(assign_node.value, ast.Call):
            if isinstance(assign_node.value.func, ast.Name) and assign_node.value.func.id == 'Dataset':
                if assign_node.value.args and isinstance(assign_node.value.args[0], ast.Constant):
                    datasets.append({
                        'dataset': assign_node.value.args[0].value,
                        'type': 'produces',  # This would need more sophisticated analysis
                        'line_number': assign_node.lineno
                    })
        
        return datasets