import ast
import os
import re
from collections import defaultdict
from .repository_analyzer import RepositoryAnalyzer

class QualityAnalyzer:
    """Analyzes code quality and provides risk assessment."""
    
    def __init__(self, directory):
        self.directory = directory
        self.repo_analyzer = RepositoryAnalyzer()
        self.python_files = self.repo_analyzer.get_python_files(directory)
    
    def analyze_quality(self, dags):
        """Perform comprehensive quality analysis."""
        quality_data = {
            'overall_score': 0,
            'metrics': {},
            'risk_assessment': {},
            'issues': [],
            'recommendations': [],
            'file_scores': {},
            'dag_scores': {}
        }
        
        # Analyze each DAG file
        for dag in dags:
            file_metrics = self.analyze_file_quality(dag['file_path'])
            quality_data['file_scores'][dag['relative_path']] = file_metrics
            quality_data['dag_scores'][dag['dag_id']] = self._calculate_dag_score(file_metrics)
        
        # Calculate overall metrics
        quality_data['metrics'] = self._calculate_overall_metrics(quality_data['file_scores'])
        quality_data['risk_assessment'] = self._assess_risks(dags, quality_data['file_scores'])
        quality_data['issues'] = self._identify_issues(quality_data['file_scores'])
        quality_data['recommendations'] = self._generate_recommendations(quality_data)
        quality_data['overall_score'] = self._calculate_overall_score(quality_data['metrics'])
        
        return quality_data
    
    def analyze_file_quality(self, file_path):
        """Analyze quality metrics for a single file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content, filename=file_path)
            
            return {
                'complexity': self._calculate_complexity(tree),
                'maintainability': self._calculate_maintainability(tree, content),
                'documentation': self._analyze_documentation(tree, content),
                'error_handling': self._analyze_error_handling(tree),
                'code_smells': self._detect_code_smells(tree, content),
                'coupling': self._analyze_coupling(tree),
                'lines_of_code': len(content.splitlines()),
                'function_count': self._count_functions(tree),
                'class_count': self._count_classes(tree)
            }
            
        except Exception as e:
            return self._get_default_metrics()
    
    def _calculate_complexity(self, tree):
        """Calculate various complexity metrics."""
        complexity = {
            'cyclomatic_complexity': 0,
            'cognitive_complexity': 0,
            'nesting_depth': 0,
            'function_lengths': []
        }
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_complexity = self._calculate_function_complexity(node)
                complexity['cyclomatic_complexity'] += func_complexity['cyclomatic']
                complexity['cognitive_complexity'] += func_complexity['cognitive']
                complexity['nesting_depth'] = max(complexity['nesting_depth'], func_complexity['nesting'])
                complexity['function_lengths'].append(func_complexity['length'])
        
        return complexity
    
    def _calculate_function_complexity(self, func_node):
        """Calculate complexity metrics for a single function."""
        complexity = {'cyclomatic': 1, 'cognitive': 0, 'nesting': 0, 'length': 0}
        
        # Count lines in function
        if hasattr(func_node, 'end_lineno'):
            complexity['length'] = func_node.end_lineno - func_node.lineno + 1
        
        # Calculate complexity by walking through the function
        for node in ast.walk(func_node):
            # Cyclomatic complexity
            if isinstance(node, (ast.If, ast.While, ast.For, ast.ExceptHandler, 
                               ast.With, ast.Assert, ast.AsyncWith, ast.AsyncFor)):
                complexity['cyclomatic'] += 1
            elif isinstance(node, ast.BoolOp):
                complexity['cyclomatic'] += len(node.values) - 1
            
            # Cognitive complexity (similar but with nesting penalties)
            if isinstance(node, (ast.If, ast.While, ast.For)):
                nesting = self._calculate_nesting_level(node, func_node)
                complexity['cognitive'] += 1 + nesting
                complexity['nesting'] = max(complexity['nesting'], nesting)
        
        return complexity
    
    def _calculate_nesting_level(self, node, root_node):
        """Calculate nesting level of a node."""
        level = 0
        current = node
        
        # This is a simplified approach - would need more sophisticated parent tracking
        return min(level, 4)  # Cap at 4 for practical purposes
    
    def _calculate_maintainability(self, tree, content):
        """Calculate maintainability metrics."""
        lines = content.splitlines()
        non_empty_lines = [line for line in lines if line.strip()]
        
        return {
            'maintainability_index': self._calculate_maintainability_index(tree, content),
            'code_to_comment_ratio': self._calculate_comment_ratio(content),
            'average_line_length': sum(len(line) for line in non_empty_lines) / max(len(non_empty_lines), 1),
            'blank_line_ratio': (len(lines) - len(non_empty_lines)) / max(len(lines), 1)
        }
    
    def _calculate_maintainability_index(self, tree, content):
        """Calculate simplified maintainability index."""
        # Simplified version of the Microsoft maintainability index
        lines_of_code = len([line for line in content.splitlines() if line.strip()])
        complexity = 1  # Would use actual cyclomatic complexity
        
        # Simplified formula (actual formula is more complex)
        if lines_of_code > 0:
            return max(0, min(100, 171 - 5.2 * complexity - 0.23 * lines_of_code))
        return 100
    
    def _calculate_comment_ratio(self, content):
        """Calculate ratio of comments to code."""
        lines = content.splitlines()
        comment_lines = 0
        code_lines = 0
        
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('#'):
                comment_lines += 1
            elif stripped and not stripped.startswith('"""') and not stripped.startswith("'''"):
                code_lines += 1
        
        return comment_lines / max(code_lines, 1)
    
    def _analyze_documentation(self, tree, content):
        """Analyze documentation quality."""
        doc_metrics = {
            'functions_with_docstrings': 0,
            'classes_with_docstrings': 0,
            'total_functions': 0,
            'total_classes': 0,
            'type_hints_coverage': 0,
            'docstring_quality_score': 0
        }
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                doc_metrics['total_functions'] += 1
                if ast.get_docstring(node):
                    doc_metrics['functions_with_docstrings'] += 1
                
                # Check for type hints
                if node.returns or any(arg.annotation for arg in node.args.args):
                    doc_metrics['type_hints_coverage'] += 1
            
            elif isinstance(node, ast.ClassDef):
                doc_metrics['total_classes'] += 1
                if ast.get_docstring(node):
                    doc_metrics['classes_with_docstrings'] += 1
        
        # Calculate coverage percentages
        if doc_metrics['total_functions'] > 0:
            doc_metrics['function_doc_coverage'] = doc_metrics['functions_with_docstrings'] / doc_metrics['total_functions']
            doc_metrics['type_hint_coverage'] = doc_metrics['type_hints_coverage'] / doc_metrics['total_functions']
        else:
            doc_metrics['function_doc_coverage'] = 1.0
            doc_metrics['type_hint_coverage'] = 1.0
        
        if doc_metrics['total_classes'] > 0:
            doc_metrics['class_doc_coverage'] = doc_metrics['classes_with_docstrings'] / doc_metrics['total_classes']
        else:
            doc_metrics['class_doc_coverage'] = 1.0
        
        return doc_metrics
    
    def _analyze_error_handling(self, tree):
        """Analyze error handling practices."""
        error_handling = {
            'try_except_blocks': 0,
            'bare_except_blocks': 0,
            'specific_exception_handling': 0,
            'error_handling_score': 0
        }
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Try):
                error_handling['try_except_blocks'] += 1
                
                for handler in node.handlers:
                    if handler.type is None:  # bare except
                        error_handling['bare_except_blocks'] += 1
                    else:
                        error_handling['specific_exception_handling'] += 1
        
        # Calculate error handling score
        total_handlers = error_handling['try_except_blocks']
        if total_handlers > 0:
            specific_ratio = error_handling['specific_exception_handling'] / total_handlers
            bare_penalty = error_handling['bare_except_blocks'] / total_handlers
            error_handling['error_handling_score'] = max(0, specific_ratio - bare_penalty)
        else:
            error_handling['error_handling_score'] = 0.5  # Neutral score for no error handling
        
        return error_handling
    
    def _detect_code_smells(self, tree, content):
        """Detect various code smells."""
        smells = {
            'long_functions': [],
            'deep_nesting': [],
            'duplicate_code': [],
            'large_classes': [],
            'magic_numbers': [],
            'long_parameter_lists': []
        }
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Long functions
                if hasattr(node, 'end_lineno'):
                    length = node.end_lineno - node.lineno + 1
                    if length > 50:  # Threshold for long function
                        smells['long_functions'].append({
                            'name': node.name,
                            'length': length,
                            'line_number': node.lineno
                        })
                
                # Long parameter lists
                if len(node.args.args) > 7:  # Threshold for too many parameters
                    smells['long_parameter_lists'].append({
                        'name': node.name,
                        'parameter_count': len(node.args.args),
                        'line_number': node.lineno
                    })
            
            elif isinstance(node, ast.ClassDef):
                # Large classes (simplified check)
                methods = [n for n in node.body if isinstance(n, ast.FunctionDef)]
                if len(methods) > 20:  # Threshold for large class
                    smells['large_classes'].append({
                        'name': node.name,
                        'method_count': len(methods),
                        'line_number': node.lineno
                    })
            
            # Magic numbers
            elif isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
                if node.value not in [0, 1, -1, 2] and abs(node.value) > 1:
                    smells['magic_numbers'].append({
                        'value': node.value,
                        'line_number': node.lineno
                    })
        
        return smells
    
    def _analyze_coupling(self, tree):
        """Analyze coupling metrics."""
        coupling = {
            'imports_count': 0,
            'external_dependencies': 0,
            'internal_dependencies': 0,
            'coupling_score': 0
        }
        
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                coupling['imports_count'] += 1
                
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ''
                    if any(keyword in module.lower() for keyword in ['common', 'utils', 'helpers']):
                        coupling['internal_dependencies'] += 1
                    else:
                        coupling['external_dependencies'] += 1
        
        # Calculate coupling score (lower is better)
        total_deps = coupling['imports_count']
        if total_deps > 0:
            # Penalize high number of dependencies
            coupling['coupling_score'] = min(1.0, total_deps / 20.0)  # Normalize to 0-1
        
        return coupling
    
    def _count_functions(self, tree):
        """Count functions in the AST."""
        return len([node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)])
    
    def _count_classes(self, tree):
        """Count classes in the AST."""
        return len([node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)])
    
    def _calculate_overall_metrics(self, file_scores):
        """Calculate overall metrics across all files."""
        if not file_scores:
            return self._get_default_overall_metrics()
        
        total_files = len(file_scores)
        metrics = {
            'average_complexity': 0,
            'average_maintainability': 0,
            'documentation_coverage': 0,
            'error_handling_coverage': 0,
            'total_issues': 0,
            'high_risk_files': 0
        }
        
        for file_path, file_metrics in file_scores.items():
            # Average complexity
            if file_metrics['complexity']['cyclomatic_complexity'] > 0:
                metrics['average_complexity'] += file_metrics['complexity']['cyclomatic_complexity']
            
            # Average maintainability
            metrics['average_maintainability'] += file_metrics['maintainability']['maintainability_index']
            
            # Documentation coverage
            metrics['documentation_coverage'] += file_metrics['documentation']['function_doc_coverage']
            
            # Error handling coverage
            metrics['error_handling_coverage'] += file_metrics['error_handling']['error_handling_score']
            
            # Count issues
            for smell_type, smells in file_metrics['code_smells'].items():
                metrics['total_issues'] += len(smells)
            
            # High risk files (low maintainability or high complexity)
            if (file_metrics['maintainability']['maintainability_index'] < 50 or 
                file_metrics['complexity']['cyclomatic_complexity'] > 20):
                metrics['high_risk_files'] += 1
        
        # Calculate averages
        metrics['average_complexity'] /= total_files
        metrics['average_maintainability'] /= total_files
        metrics['documentation_coverage'] /= total_files
        metrics['error_handling_coverage'] /= total_files
        
        return metrics
    
    def _assess_risks(self, dags, file_scores):
        """Assess risks across the codebase."""
        risks = {
            'high_risk_components': [],
            'dependency_risks': [],
            'quality_risks': [],
            'overall_risk_level': 'LOW'
        }
        
        # Identify high-risk files
        for file_path, metrics in file_scores.items():
            risk_score = self._calculate_file_risk_score(metrics)
            
            if risk_score > 0.7:  # High risk threshold
                risks['high_risk_components'].append({
                    'file_path': file_path,
                    'risk_score': risk_score,
                    'issues': self._get_file_issues(metrics)
                })
        
        # Assess dependency risks (files used by many DAGs)
        dependency_usage = defaultdict(int)
        for dag in dags:
            for imp in dag['imports']['from_common']:
                dependency_usage[imp['module']] += 1
        
        for module, usage_count in dependency_usage.items():
            if usage_count > 3:  # Used by many DAGs
                risks['dependency_risks'].append({
                    'module': module,
                    'usage_count': usage_count,
                    'risk_level': 'HIGH' if usage_count > 5 else 'MEDIUM'
                })
        
        # Determine overall risk level
        if risks['high_risk_components'] or any(r['risk_level'] == 'HIGH' for r in risks['dependency_risks']):
            risks['overall_risk_level'] = 'HIGH'
        elif risks['dependency_risks']:
            risks['overall_risk_level'] = 'MEDIUM'
        
        return risks
    
    def _calculate_file_risk_score(self, metrics):
        """Calculate risk score for a file (0-1, higher is riskier)."""
        risk_factors = []
        
        # Complexity risk
        complexity_risk = min(1.0, metrics['complexity']['cyclomatic_complexity'] / 30.0)
        risk_factors.append(complexity_risk * 0.3)
        
        # Maintainability risk (inverse)
        maintainability_risk = 1.0 - (metrics['maintainability']['maintainability_index'] / 100.0)
        risk_factors.append(maintainability_risk * 0.25)
        
        # Documentation risk
        doc_risk = 1.0 - metrics['documentation']['function_doc_coverage']
        risk_factors.append(doc_risk * 0.2)
        
        # Error handling risk
        error_risk = 1.0 - metrics['error_handling']['error_handling_score']
        risk_factors.append(error_risk * 0.15)
        
        # Code smells risk
        total_smells = sum(len(smells) for smells in metrics['code_smells'].values())
        smell_risk = min(1.0, total_smells / 10.0)
        risk_factors.append(smell_risk * 0.1)
        
        return sum(risk_factors)
    
    def _get_file_issues(self, metrics):
        """Get list of issues for a file."""
        issues = []
        
        if metrics['complexity']['cyclomatic_complexity'] > 20:
            issues.append("High cyclomatic complexity")
        
        if metrics['maintainability']['maintainability_index'] < 50:
            issues.append("Low maintainability index")
        
        if metrics['documentation']['function_doc_coverage'] < 0.5:
            issues.append("Poor documentation coverage")
        
        for smell_type, smells in metrics['code_smells'].items():
            if smells:
                issues.append(f"{smell_type.replace('_', ' ').title()}: {len(smells)} instances")
        
        return issues
    
    def _identify_issues(self, file_scores):
        """Identify and prioritize issues across all files."""
        issues = []
        
        for file_path, metrics in file_scores.items():
            file_issues = self._get_file_issues(metrics)
            risk_score = self._calculate_file_risk_score(metrics)
            
            for issue in file_issues:
                issues.append({
                    'file_path': file_path,
                    'issue': issue,
                    'severity': 'HIGH' if risk_score > 0.7 else 'MEDIUM' if risk_score > 0.4 else 'LOW',
                    'risk_score': risk_score
                })
        
        # Sort by risk score (highest first)
        issues.sort(key=lambda x: x['risk_score'], reverse=True)
        
        return issues[:20]  # Return top 20 issues
    
    def _generate_recommendations(self, quality_data):
        """Generate improvement recommendations."""
        recommendations = []
        
        metrics = quality_data['metrics']
        
        if metrics['average_complexity'] > 15:
            recommendations.append({
                'category': 'Complexity',
                'priority': 'HIGH',
                'recommendation': 'Reduce cyclomatic complexity by breaking down complex functions into smaller ones'
            })
        
        if metrics['documentation_coverage'] < 0.6:
            recommendations.append({
                'category': 'Documentation',
                'priority': 'MEDIUM',
                'recommendation': 'Improve documentation coverage by adding docstrings to functions and classes'
            })
        
        if metrics['error_handling_coverage'] < 0.4:
            recommendations.append({
                'category': 'Error Handling',
                'priority': 'HIGH',
                'recommendation': 'Improve error handling by adding try-except blocks and avoiding bare except clauses'
            })
        
        if quality_data['risk_assessment']['high_risk_components']:
            recommendations.append({
                'category': 'Risk Management',
                'priority': 'HIGH',
                'recommendation': f'Address high-risk components: {len(quality_data["risk_assessment"]["high_risk_components"])} files need attention'
            })
        
        return recommendations
    
    def _calculate_dag_score(self, file_metrics):
        """Calculate quality score for a DAG (0-100)."""
        # Weighted average of different metrics
        complexity_score = max(0, 100 - file_metrics['complexity']['cyclomatic_complexity'] * 2)
        maintainability_score = file_metrics['maintainability']['maintainability_index']
        documentation_score = file_metrics['documentation']['function_doc_coverage'] * 100
        error_handling_score = file_metrics['error_handling']['error_handling_score'] * 100
        
        # Calculate smell penalty
        total_smells = sum(len(smells) for smells in file_metrics['code_smells'].values())
        smell_penalty = min(30, total_smells * 3)  # Max 30 point penalty
        
        weighted_score = (
            complexity_score * 0.3 +
            maintainability_score * 0.25 +
            documentation_score * 0.2 +
            error_handling_score * 0.15 +
            (100 - smell_penalty) * 0.1
        )
        
        return max(0, min(100, weighted_score))
    
    def _calculate_overall_score(self, metrics):
        """Calculate overall quality score (0-100)."""
        complexity_score = max(0, 100 - metrics['average_complexity'] * 3)
        maintainability_score = metrics['average_maintainability']
        documentation_score = metrics['documentation_coverage'] * 100
        error_handling_score = metrics['error_handling_coverage'] * 100
        
        # Risk penalty
        risk_penalty = min(20, metrics['high_risk_files'] * 5)
        
        overall_score = (
            complexity_score * 0.3 +
            maintainability_score * 0.25 +
            documentation_score * 0.2 +
            error_handling_score * 0.15 +
            (100 - risk_penalty) * 0.1
        )
        
        return max(0, min(100, overall_score))
    
    def _get_default_metrics(self):
        """Return default metrics for files that can't be analyzed."""
        return {
            'complexity': {'cyclomatic_complexity': 0, 'cognitive_complexity': 0, 'nesting_depth': 0, 'function_lengths': []},
            'maintainability': {'maintainability_index': 50, 'code_to_comment_ratio': 0, 'average_line_length': 0, 'blank_line_ratio': 0},
            'documentation': {'function_doc_coverage': 0, 'class_doc_coverage': 0, 'type_hint_coverage': 0},
            'error_handling': {'try_except_blocks': 0, 'error_handling_score': 0},
            'code_smells': {'long_functions': [], 'deep_nesting': [], 'duplicate_code': [], 'large_classes': [], 'magic_numbers': [], 'long_parameter_lists': []},
            'coupling': {'imports_count': 0, 'coupling_score': 0},
            'lines_of_code': 0,
            'function_count': 0,
            'class_count': 0
        }
    
    def _get_default_overall_metrics(self):
        """Return default overall metrics."""
        return {
            'average_complexity': 0,
            'average_maintainability': 50,
            'documentation_coverage': 0,
            'error_handling_coverage': 0,
            'total_issues': 0,
            'high_risk_files': 0
        }