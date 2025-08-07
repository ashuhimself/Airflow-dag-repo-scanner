import ast
import os
import networkx as nx
from collections import defaultdict
from .repository_analyzer import RepositoryAnalyzer

class ImpactAnalyzer:
    """Analyzes impact of changes and provides change management recommendations."""
    
    def __init__(self, directory):
        self.directory = directory
        self.repo_analyzer = RepositoryAnalyzer()
        self.python_files = self.repo_analyzer.get_python_files(directory)
        self.dependency_graph = nx.DiGraph()
        
    def analyze_impact(self, dags, code_dependencies):
        """Perform comprehensive impact analysis."""
        self._build_dependency_graph(dags, code_dependencies)
        
        impact_data = {
            'dependency_graph': self._serialize_graph(),
            'high_impact_modules': self._identify_high_impact_modules(),
            'risk_matrix': self._create_risk_matrix(dags),
            'change_propagation_paths': self._analyze_propagation_paths(),
            'test_coverage_mapping': self._analyze_test_coverage(),
            'deployment_recommendations': self._generate_deployment_recommendations(dags)
        }
        
        return impact_data
    
    def analyze_change_impact(self, target_file, target_function=None):
        """Analyze impact of changing a specific file or function."""
        impact_result = {
            'target': {
                'file': target_file,
                'function': target_function
            },
            'direct_impacts': [],
            'indirect_impacts': [],
            'affected_dags': [],
            'risk_assessment': {},
            'recommendations': []
        }
        
        # Find all files that import from the target file
        target_module = self._file_to_module_name(target_file)
        
        for file_path in self.python_files:
            imports = self._analyze_file_imports(file_path)
            
            for imp in imports:
                if self._is_target_import(imp, target_module, target_function):
                    impact_info = {
                        'file_path': os.path.relpath(file_path, self.directory),
                        'import_type': imp['type'],
                        'import_name': imp['name'],
                        'line_number': imp['line_number'],
                        'usage_details': self._analyze_usage_in_file(file_path, imp['name'])
                    }
                    
                    if imp['type'] == 'direct':
                        impact_result['direct_impacts'].append(impact_info)
                    else:
                        impact_result['indirect_impacts'].append(impact_info)
        
        # Find affected DAGs
        impact_result['affected_dags'] = self._find_affected_dags(impact_result['direct_impacts'])
        
        # Assess risk
        impact_result['risk_assessment'] = self._assess_change_risk(
            target_file, impact_result['direct_impacts'], impact_result['indirect_impacts']
        )
        
        # Generate recommendations
        impact_result['recommendations'] = self._generate_change_recommendations(impact_result)
        
        return impact_result
    
    def _build_dependency_graph(self, dags, code_dependencies):
        """Build a comprehensive dependency graph."""
        self.dependency_graph.clear()
        
        # Add DAGs as nodes
        for dag in dags:
            self.dependency_graph.add_node(dag['dag_id'], type='dag', file_path=dag['file_path'])
        
        # Add code dependencies
        for dag_id, deps in code_dependencies['dag_dependencies'].items():
            for imp in deps['common_imports']:
                module_name = f"{imp['module']}.{imp['name']}"
                self.dependency_graph.add_node(module_name, type='module')
                self.dependency_graph.add_edge(dag_id, module_name, relationship='imports')
        
        # Add common module usage relationships
        for module_func, usages in code_dependencies['common_usage_map'].items():
            if len(usages) > 1:  # Used by multiple DAGs
                self.dependency_graph.add_node(module_func, type='shared_module')
                for usage in usages:
                    if self.dependency_graph.has_node(usage['dag_id']):
                        self.dependency_graph.add_edge(usage['dag_id'], module_func, relationship='uses')
    
    def _serialize_graph(self):
        """Serialize the dependency graph for visualization."""
        nodes = []
        edges = []
        
        for node_id, data in self.dependency_graph.nodes(data=True):
            nodes.append({
                'id': node_id,
                'label': node_id,
                'type': data.get('type', 'unknown'),
                'group': data.get('type', 'unknown')
            })
        
        for source, target, data in self.dependency_graph.edges(data=True):
            edges.append({
                'from': source,
                'to': target,
                'label': data.get('relationship', ''),
                'arrows': 'to'
            })
        
        return {'nodes': nodes, 'edges': edges}
    
    def _identify_high_impact_modules(self):
        """Identify modules that would have high impact if changed."""
        high_impact = []
        
        # Calculate in-degree (how many things depend on each module)
        for node in self.dependency_graph.nodes():
            if self.dependency_graph.nodes[node].get('type') in ['module', 'shared_module']:
                in_degree = self.dependency_graph.in_degree(node)
                if in_degree > 2:  # Used by more than 2 DAGs
                    high_impact.append({
                        'module': node,
                        'dependency_count': in_degree,
                        'dependent_dags': list(self.dependency_graph.predecessors(node)),
                        'risk_level': 'HIGH' if in_degree > 5 else 'MEDIUM'
                    })
        
        # Sort by dependency count (highest first)
        high_impact.sort(key=lambda x: x['dependency_count'], reverse=True)
        
        return high_impact
    
    def _create_risk_matrix(self, dags):
        """Create a risk matrix based on complexity and impact."""
        risk_matrix = {
            'low_risk': [],
            'medium_risk': [],
            'high_risk': [],
            'critical_risk': []
        }
        
        for dag in dags:
            # Calculate impact score (number of dependencies)
            dag_id = dag['dag_id']
            impact_score = 0
            
            if self.dependency_graph.has_node(dag_id):
                impact_score = len(list(self.dependency_graph.successors(dag_id)))
            
            # Calculate complexity score (simplified)
            complexity_score = len(dag['imports']['from_common']) + len(dag['imports']['functions'])
            
            # Determine risk category
            if impact_score > 10 or complexity_score > 15:
                category = 'critical_risk'
            elif impact_score > 5 or complexity_score > 10:
                category = 'high_risk'
            elif impact_score > 2 or complexity_score > 5:
                category = 'medium_risk'
            else:
                category = 'low_risk'
            
            risk_matrix[category].append({
                'dag_id': dag_id,
                'file_path': dag['relative_path'],
                'impact_score': impact_score,
                'complexity_score': complexity_score
            })
        
        return risk_matrix
    
    def _analyze_propagation_paths(self):
        """Analyze how changes propagate through the dependency graph."""
        propagation_paths = []
        
        # Find all paths between high-impact modules and DAGs
        high_impact_modules = [
            item['module'] for item in self._identify_high_impact_modules()[:10]  # Top 10
        ]
        
        dag_nodes = [
            node for node, data in self.dependency_graph.nodes(data=True) 
            if data.get('type') == 'dag'
        ]
        
        for module in high_impact_modules:
            if self.dependency_graph.has_node(module):
                for dag in dag_nodes:
                    try:
                        if nx.has_path(self.dependency_graph, dag, module):
                            path = nx.shortest_path(self.dependency_graph, dag, module)
                            if len(path) > 1:  # Exclude direct relationships
                                propagation_paths.append({
                                    'source': dag,
                                    'target': module,
                                    'path': path,
                                    'length': len(path) - 1,
                                    'risk_level': 'HIGH' if len(path) > 3 else 'MEDIUM'
                                })
                    except nx.NetworkXNoPath:
                        continue
        
        # Sort by path length (longer paths are more risky)
        propagation_paths.sort(key=lambda x: x['length'], reverse=True)
        
        return propagation_paths[:20]  # Return top 20 most complex paths
    
    def _analyze_test_coverage(self):
        """Analyze test coverage mapping (simplified implementation)."""
        test_coverage = {
            'test_files': [],
            'coverage_gaps': [],
            'test_to_dag_mapping': {}
        }
        
        # Find test files
        for file_path in self.python_files:
            if 'test' in os.path.basename(file_path).lower():
                test_coverage['test_files'].append({
                    'file_path': os.path.relpath(file_path, self.directory),
                    'type': 'unit_test' if 'unit' in file_path else 'integration_test'
                })
        
        # Identify coverage gaps (DAGs without corresponding test files)
        test_file_names = {os.path.basename(f['file_path']).replace('test_', '').replace('.py', '') 
                          for f in test_coverage['test_files']}
        
        dag_files = set()
        for file_path in self.python_files:
            if self._file_contains_dag(file_path):
                dag_name = os.path.basename(file_path).replace('.py', '')
                dag_files.add(dag_name)
                
                if dag_name not in test_file_names:
                    test_coverage['coverage_gaps'].append({
                        'dag_file': os.path.relpath(file_path, self.directory),
                        'missing_tests': ['unit_test', 'integration_test']
                    })
        
        return test_coverage
    
    def _generate_deployment_recommendations(self, dags):
        """Generate deployment order recommendations."""
        recommendations = {
            'deployment_order': [],
            'parallel_deployable': [],
            'risk_mitigation': [],
            'rollback_strategy': []
        }
        
        # Create deployment order based on dependencies
        dag_dependency_counts = {}
        for dag in dags:
            dag_id = dag['dag_id']
            if self.dependency_graph.has_node(dag_id):
                # Count outgoing dependencies (things this DAG depends on)
                dependency_count = len(list(self.dependency_graph.successors(dag_id)))
                dag_dependency_counts[dag_id] = dependency_count
            else:
                dag_dependency_counts[dag_id] = 0
        
        # Sort by dependency count (fewer dependencies should be deployed first)
        sorted_dags = sorted(dag_dependency_counts.items(), key=lambda x: x[1])
        
        recommendations['deployment_order'] = [
            {
                'dag_id': dag_id,
                'dependency_count': count,
                'deployment_phase': 'early' if count < 3 else 'middle' if count < 8 else 'late'
            }
            for dag_id, count in sorted_dags
        ]
        
        # Identify DAGs that can be deployed in parallel (no dependencies between them)
        independent_dags = []
        for dag_id in dag_dependency_counts.keys():
            has_dag_dependencies = False
            if self.dependency_graph.has_node(dag_id):
                for successor in self.dependency_graph.successors(dag_id):
                    if dag_id in dag_dependency_counts:  # If successor is also a DAG
                        has_dag_dependencies = True
                        break
            
            if not has_dag_dependencies:
                independent_dags.append(dag_id)
        
        recommendations['parallel_deployable'] = independent_dags
        
        # Risk mitigation recommendations
        high_risk_dags = [item['dag_id'] for item in self._create_risk_matrix(dags)['high_risk'] + 
                         self._create_risk_matrix(dags)['critical_risk']]
        
        for dag_id in high_risk_dags:
            recommendations['risk_mitigation'].append({
                'dag_id': dag_id,
                'recommendations': [
                    'Deploy during low-traffic hours',
                    'Implement feature flags',
                    'Prepare rollback scripts',
                    'Monitor closely after deployment'
                ]
            })
        
        return recommendations
    
    def _file_to_module_name(self, file_path):
        """Convert file path to module name."""
        rel_path = os.path.relpath(file_path, self.directory)
        return rel_path.replace(os.path.sep, '.').replace('.py', '')
    
    def _analyze_file_imports(self, file_path):
        """Analyze imports in a specific file."""
        imports = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content, filename=file_path)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ''
                    for alias in node.names:
                        imports.append({
                            'type': 'direct',
                            'module': module,
                            'name': alias.name,
                            'alias': alias.asname,
                            'line_number': node.lineno
                        })
        
        except Exception:
            pass  # Skip files with parsing errors
        
        return imports
    
    def _is_target_import(self, imp, target_module, target_function):
        """Check if an import matches the target module/function."""
        if target_function:
            return imp['module'] == target_module and imp['name'] == target_function
        else:
            return target_module in imp['module']
    
    def _analyze_usage_in_file(self, file_path, import_name):
        """Analyze how an import is used within a file."""
        usage_details = {
            'usage_count': 0,
            'usage_locations': []
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.splitlines()
            for i, line in enumerate(lines, 1):
                if import_name in line and not line.strip().startswith('from') and not line.strip().startswith('import'):
                    usage_details['usage_count'] += 1
                    usage_details['usage_locations'].append({
                        'line_number': i,
                        'line_content': line.strip()
                    })
        
        except Exception:
            pass
        
        return usage_details
    
    def _find_affected_dags(self, direct_impacts):
        """Find DAGs that are affected by the changes."""
        affected_dags = []
        
        for impact in direct_impacts:
            file_path = impact['file_path']
            if self._file_contains_dag(os.path.join(self.directory, file_path)):
                dag_id = self._extract_dag_id_from_file(os.path.join(self.directory, file_path))
                if dag_id:
                    affected_dags.append({
                        'dag_id': dag_id,
                        'file_path': file_path,
                        'impact_type': 'direct'
                    })
        
        return affected_dags
    
    def _file_contains_dag(self, file_path):
        """Check if a file contains DAG definitions."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Simple check for DAG patterns
            return '@dag' in content or 'DAG(' in content
        
        except Exception:
            return False
    
    def _extract_dag_id_from_file(self, file_path):
        """Extract DAG ID from a file (simplified)."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Look for common DAG ID patterns
            import re
            
            # Pattern for dag_id parameter
            dag_id_match = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', content)
            if dag_id_match:
                return dag_id_match.group(1)
            
            # Pattern for @dag decorator on function
            dag_func_match = re.search(r'@dag[^\n]*\ndef\s+(\w+)', content)
            if dag_func_match:
                return dag_func_match.group(1)
        
        except Exception:
            pass
        
        return None
    
    def _assess_change_risk(self, target_file, direct_impacts, indirect_impacts):
        """Assess the risk level of making changes to the target."""
        risk_assessment = {
            'risk_level': 'LOW',
            'risk_score': 0,
            'risk_factors': [],
            'confidence': 'HIGH'
        }
        
        risk_score = 0
        
        # Direct impact factor
        direct_count = len(direct_impacts)
        if direct_count > 10:
            risk_score += 40
            risk_assessment['risk_factors'].append(f'High number of direct dependencies ({direct_count})')
        elif direct_count > 5:
            risk_score += 20
            risk_assessment['risk_factors'].append(f'Moderate number of direct dependencies ({direct_count})')
        
        # Indirect impact factor
        indirect_count = len(indirect_impacts)
        if indirect_count > 5:
            risk_score += 20
            risk_assessment['risk_factors'].append(f'Multiple indirect dependencies ({indirect_count})')
        
        # File type factor
        if 'common' in target_file or 'util' in target_file:
            risk_score += 30
            risk_assessment['risk_factors'].append('Target is in common/utility module')
        
        # DAG impact factor
        affected_dag_count = len([imp for imp in direct_impacts if self._file_contains_dag(
            os.path.join(self.directory, imp['file_path'])
        )])
        if affected_dag_count > 3:
            risk_score += 25
            risk_assessment['risk_factors'].append(f'Multiple DAGs affected ({affected_dag_count})')
        
        # Determine risk level
        if risk_score >= 70:
            risk_assessment['risk_level'] = 'CRITICAL'
        elif risk_score >= 50:
            risk_assessment['risk_level'] = 'HIGH'
        elif risk_score >= 25:
            risk_assessment['risk_level'] = 'MEDIUM'
        else:
            risk_assessment['risk_level'] = 'LOW'
        
        risk_assessment['risk_score'] = min(100, risk_score)
        
        return risk_assessment
    
    def _generate_change_recommendations(self, impact_result):
        """Generate recommendations for managing the change."""
        recommendations = []
        
        risk_level = impact_result['risk_assessment']['risk_level']
        direct_count = len(impact_result['direct_impacts'])
        affected_dag_count = len(impact_result['affected_dags'])
        
        # Testing recommendations
        if direct_count > 0:
            recommendations.append({
                'category': 'Testing',
                'priority': 'HIGH',
                'action': f'Test all {direct_count} directly impacted files before deployment'
            })
        
        if affected_dag_count > 0:
            recommendations.append({
                'category': 'Testing',
                'priority': 'HIGH',
                'action': f'Run integration tests for all {affected_dag_count} affected DAGs'
            })
        
        # Deployment recommendations
        if risk_level in ['HIGH', 'CRITICAL']:
            recommendations.extend([
                {
                    'category': 'Deployment',
                    'priority': 'HIGH',
                    'action': 'Deploy during maintenance window or low-traffic hours'
                },
                {
                    'category': 'Monitoring',
                    'priority': 'HIGH',
                    'action': 'Set up enhanced monitoring for all affected components'
                },
                {
                    'category': 'Rollback',
                    'priority': 'HIGH',
                    'action': 'Prepare automated rollback procedures'
                }
            ])
        
        # Communication recommendations
        if direct_count > 5:
            recommendations.append({
                'category': 'Communication',
                'priority': 'MEDIUM',
                'action': 'Notify teams responsible for affected DAGs'
            })
        
        # Code review recommendations
        recommendations.append({
            'category': 'Review',
            'priority': 'MEDIUM' if risk_level == 'LOW' else 'HIGH',
            'action': 'Require additional code review from senior developers'
        })
        
        return recommendations