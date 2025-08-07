# DAG Dependency Analyzer

A comprehensive Flask web application for analyzing Airflow DAG dependencies, code quality, and impact assessment with advanced visualization capabilities.

## Features

### Core Analysis Capabilities
- **Repository Analysis**: Support for both Git repositories and local directories
- **DAG Discovery**: Automatic detection of DAGs using Python AST parsing
  - `@dag` decorator patterns
  - `DAG()` constructor calls
  - Dynamic DAG generation patterns

### Three Types of Analysis

#### 1. Code Dependencies Analysis
- Extract all imports from `common/` folders
- Categorize imports into functions, classes, and variables
- Show which specific functions/classes each DAG uses
- Drill-down view with exact usage details
- Identify shared modules and their usage patterns

#### 2. DAG Orchestration Analysis
- Detect `TriggerDagRunOperator` instances and extract target DAGs
- Parse Dataset dependencies (produces/consumes)
- Identify schedule types (@daily, @hourly, Dataset-driven)
- Build DAG-to-DAG trigger relationships
- Interactive network visualization with vis.js

#### 3. Code Quality & Risk Assessment
- **Complexity Analysis**: Cyclomatic complexity, function length, nesting depth
- **Dependency Risk**: Identify high-risk modules used by many DAGs
- **Code Smells**: Anti-patterns, duplicate code, overly complex functions
- **Coupling Analysis**: Measure tight vs loose coupling between modules
- **Documentation Quality**: Check for docstrings, type hints, comments
- **Error Handling**: Assess try-catch coverage and error propagation
- **Performance Risks**: Identify potential bottlenecks

### Impact Analysis & Change Management

#### "What If" Analysis
- **File Change Impact**: Show all DAGs/modules affected by file changes
- **Function Change Impact**: Identify all usages of specific functions/classes
- **Breaking Change Detection**: Predict potential breaking changes
- **Cascade Effect Visualization**: Show ripple effects through dependency chain
- **Risk Scoring**: Rate change risk from Low/Medium/High/Critical

#### Recommendations Engine
- **Refactoring Suggestions**: Recommend code improvements to reduce coupling
- **Testing Strategy**: Suggest which DAGs to test when changing dependencies
- **Deployment Order**: Recommend safe deployment sequence for changes
- **Migration Paths**: Suggest how to safely update dependencies

### Web Interface Features

#### Four Main Tabs:
1. **Orchestration Tab**: Interactive network diagram of DAG relationships
2. **Code Dependencies Tab**: Expandable dependency tree and common module usage
3. **Quality Dashboard Tab**: Code quality metrics, scores, and recommendations
4. **Impact Analysis Tab**: Change impact visualization and risk assessment

#### Quality Dashboard:
- **Overall Health Score**: 0-100 rating with color coding
- **Risk Matrix**: Visual grid showing high-risk components
- **Quality Metrics**: Radar chart for complexity, maintainability, documentation
- **Top Issues**: Prioritized list of code quality problems
- **Recommendations**: Step-by-step guidance for improvements

#### Impact Analysis:
- **File/Function Selector**: Dropdown to select analysis targets
- **Risk Assessment**: Color-coded impact severity
- **Change Recommendations**: Automated guidance for safe changes
- **Risk Matrix**: Categorize DAGs by risk level

## Installation

### Prerequisites
- Python 3.8+
- Git (for repository cloning functionality)

### Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd DagAnalyser
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application:**
   ```bash
   python app.py
   ```

4. **Access the web interface:**
   Open your browser and navigate to `http://localhost:5000`

## Dependencies

- **Flask 2.3.3**: Web framework
- **GitPython 3.1.40**: Git repository operations
- **radon 6.0.1**: Code complexity analysis
- **vulture 2.10**: Dead code detection
- **bandit 1.7.5**: Security analysis
- **networkx 3.2.1**: Graph operations for impact analysis
- **astroid 3.0.1**: Python AST utilities
- **Werkzeug 2.3.7**: WSGI utilities

### Frontend Libraries (CDN):
- **Tailwind CSS**: Responsive styling
- **vis.js**: Network visualization
- **Chart.js**: Quality metrics charts
- **Font Awesome**: Icons

## Usage

### Analyzing a Git Repository

1. Select "Git Repository" as the source type
2. Enter the Git repository URL (e.g., `https://github.com/username/airflow-dags.git`)
3. Click "Analyze Repository"
4. Wait for the analysis to complete (may take several minutes for large repositories)
5. Explore results across the four analysis tabs

### Analyzing a Local Directory

1. Select "Local Directory" as the source type
2. Enter the full path to your DAGs directory (e.g., `/path/to/your/dags`)
3. Click "Analyze Repository"
4. Explore the analysis results

### Using Impact Analysis

1. Complete a full repository analysis first
2. Go to the "Impact Analysis" tab
3. Select a target file from the dropdown
4. Optionally specify a function name
5. Click "Analyze Impact" to see detailed impact assessment
6. Review risk assessment and change recommendations

## API Endpoints

### POST /api/analyze
Perform full repository analysis.

**Request Body:**
```json
{
  "source_type": "git|local",
  "source_path": "repository_url_or_directory_path"
}
```

**Response:** Complete analysis results including DAGs, dependencies, quality metrics, and impact data.

### POST /api/impact-analysis
Analyze impact of specific file/function changes.

**Request Body:**
```json
{
  "source_type": "git|local", 
  "source_path": "repository_url_or_directory_path",
  "target_file": "relative/path/to/file.py",
  "target_function": "function_name" // optional
}
```

**Response:** Detailed impact analysis with risk assessment and recommendations.

## Architecture

### Backend Components
- **RepositoryAnalyzer**: Handles Git cloning and local directory validation
- **DAGAnalyzer**: Python AST parsing for DAG discovery and dependency extraction
- **QualityAnalyzer**: Code quality assessment using multiple metrics
- **ImpactAnalyzer**: Change impact analysis and risk assessment

### Frontend Components
- **Responsive Design**: Works on desktop and mobile devices
- **Interactive Visualizations**: Network graphs and charts
- **Real-time Analysis**: Progress indicators and error handling
- **Tabbed Interface**: Organized analysis results

## Sample DAG Structures Supported

The analyzer supports various DAG patterns:

```python
# TaskFlow API (@dag decorator)
@dag(schedule="@daily", catchup=False)
def my_taskflow_dag():
    @task
    def extract_data():
        pass
    
    @task 
    def transform_data(data):
        pass
    
    extract_data() >> transform_data()

# Traditional DAG
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag = DAG(
    "my_traditional_dag",
    schedule_interval="@hourly",
    catchup=False
)

trigger_task = TriggerDagRunOperator(
    task_id="trigger_downstream_dag",
    trigger_dag_id="downstream_dag_id",
    dag=dag
)

# Dataset dependencies
from airflow.datasets import Dataset

input_dataset = Dataset("s3://bucket/input/")
output_dataset = Dataset("s3://bucket/output/")

@dag(schedule=[input_dataset])
def dataset_driven_dag():
    pass
```

## Troubleshooting

### Common Issues

1. **"No DAGs found"**: Ensure your directory contains Python files with DAG definitions
2. **Git clone failures**: Check repository URL and network connectivity
3. **Permission errors**: Ensure read access to local directories
4. **Analysis timeout**: Large repositories may take several minutes to analyze

### Logs
The application logs detailed information about the analysis process. Check the console output for debugging information.

### Performance Considerations
- Large repositories (1000+ files) may take 5-10 minutes to analyze
- Git cloning time depends on repository size and network speed
- Analysis results are not cached; each request performs a fresh analysis

## Contributing

This analyzer is designed to be extensible. Key areas for enhancement:

1. **Additional Code Quality Metrics**: Add more complexity and quality assessments
2. **Visualization Improvements**: Enhanced charts and interactive elements
3. **Export Capabilities**: PDF reports, CSV exports
4. **Caching**: Cache analysis results for better performance
5. **Database Integration**: Store analysis history and trends

## License

[Specify your license here]