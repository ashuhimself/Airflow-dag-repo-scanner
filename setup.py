#!/usr/bin/env python3
"""
Setup script for DAG Dependency Analyzer
"""

import subprocess
import sys
import os

def install_dependencies():
    """Install required Python packages."""
    print("Installing Python dependencies...")
    
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✓ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install dependencies: {e}")
        return False

def test_imports():
    """Test that all required modules can be imported."""
    print("Testing imports...")
    
    required_modules = [
        'flask',
        'git',
        'radon', 
        'vulture',
        'bandit',
        'networkx',
        'astroid'
    ]
    
    failed_imports = []
    
    for module in required_modules:
        try:
            __import__(module)
            print(f"✓ {module}")
        except ImportError:
            print(f"✗ {module}")
            failed_imports.append(module)
    
    return len(failed_imports) == 0

def run_basic_tests():
    """Run basic functionality tests.""" 
    print("Running basic tests...")
    
    try:
        # Test DAG discovery
        from analyzers.dag_analyzer import DAGAnalyzer
        analyzer = DAGAnalyzer('./test_sample_dags')
        dags = analyzer.discover_dags()
        
        if not dags:
            print("✗ No DAGs discovered in test samples")
            return False
        
        print(f"✓ Discovered {len(dags)} test DAGs")
        
        # Test quality analysis
        from analyzers.quality_analyzer import QualityAnalyzer
        quality_analyzer = QualityAnalyzer('./test_sample_dags')
        quality_metrics = quality_analyzer.analyze_quality(dags)
        
        print(f"✓ Quality analysis completed (score: {quality_metrics['overall_score']:.1f})")
        
        # Test repository analyzer  
        from analyzers.repository_analyzer import RepositoryAnalyzer
        repo_analyzer = RepositoryAnalyzer()
        python_files = repo_analyzer.get_python_files('./test_sample_dags')
        
        print(f"✓ Found {len(python_files)} Python files")
        
        return True
        
    except Exception as e:
        print(f"✗ Basic tests failed: {e}")
        return False

def main():
    """Main setup routine."""
    print("=== DAG Dependency Analyzer Setup ===\n")
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("✗ Python 3.8+ is required")
        sys.exit(1)
    
    print(f"✓ Python {sys.version.split()[0]} detected\n")
    
    # Install dependencies
    if not install_dependencies():
        print("\n✗ Setup failed at dependency installation")
        sys.exit(1)
    
    print()
    
    # Test imports
    if not test_imports():
        print("\n✗ Setup failed at import testing")
        sys.exit(1)
    
    print()
    
    # Run basic tests
    if not run_basic_tests():
        print("\n✗ Setup failed at basic functionality tests")
        sys.exit(1)
    
    print("\n=== Setup Complete! ===")
    print("\nTo start the application:")
    print("  python3 app.py")
    print("\nThen open: http://localhost:5000")

if __name__ == '__main__':
    main()