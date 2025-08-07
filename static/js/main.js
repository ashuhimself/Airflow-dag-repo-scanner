// Global variables
let analysisData = null;
let orchestrationNetwork = null;
let qualityMetricsChart = null;
let riskDistributionChart = null;

// DOM elements
const elements = {
    form: document.getElementById('analysisForm'),
    sourceTypeInputs: document.querySelectorAll('input[name="sourceType"]'),
    pathLabel: document.getElementById('pathLabel'),
    sourcePath: document.getElementById('sourcePath'),
    analyzeBtn: document.getElementById('analyzeBtn'),
    loadingState: document.getElementById('loadingState'),
    errorState: document.getElementById('errorState'),
    errorMessage: document.getElementById('errorMessage'),
    resultsSection: document.getElementById('resultsSection'),
    summaryCards: document.getElementById('summaryCards'),
    tabButtons: document.querySelectorAll('.tab-button'),
    tabContents: document.querySelectorAll('.tab-content'),
    impactAnalysisForm: document.getElementById('impactAnalysisForm')
};

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    initializeEventListeners();
    updatePlaceholder();
});

function initializeEventListeners() {
    // Source type change
    elements.sourceTypeInputs.forEach(input => {
        input.addEventListener('change', updatePlaceholder);
    });

    // Main analysis form
    elements.form.addEventListener('submit', handleAnalysisSubmit);

    // Tab navigation
    elements.tabButtons.forEach(button => {
        button.addEventListener('click', () => switchTab(button.id));
    });

    // Impact analysis form
    elements.impactAnalysisForm.addEventListener('submit', handleImpactAnalysisSubmit);
}

function updatePlaceholder() {
    const selectedType = document.querySelector('input[name="sourceType"]:checked').value;
    
    if (selectedType === 'git') {
        elements.pathLabel.textContent = 'Git Repository URL';
        elements.sourcePath.placeholder = 'https://github.com/username/repository.git';
    } else {
        elements.pathLabel.textContent = 'Local Directory Path';
        elements.sourcePath.placeholder = '/path/to/your/dags/directory';
    }
}

async function handleAnalysisSubmit(e) {
    e.preventDefault();
    
    const formData = new FormData(elements.form);
    const sourceType = formData.get('sourceType');
    const sourcePath = formData.get('sourcePath').trim();
    
    if (!sourcePath) {
        showError('Please provide a valid source path.');
        return;
    }
    
    setLoadingState(true);
    
    try {
        const response = await fetch('/api/analyze', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                source_type: sourceType,
                source_path: sourcePath
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Analysis failed');
        }
        
        analysisData = await response.json();
        displayResults();
        
    } catch (error) {
        console.error('Analysis error:', error);
        showError(error.message);
    } finally {
        setLoadingState(false);
    }
}

function setLoadingState(loading) {
    elements.analyzeBtn.disabled = loading;
    elements.loadingState.classList.toggle('hidden', !loading);
    elements.errorState.classList.add('hidden');
    
    if (!loading) {
        elements.resultsSection.classList.remove('hidden');
    }
}

function showError(message) {
    elements.errorMessage.textContent = message;
    elements.errorState.classList.remove('hidden');
    elements.resultsSection.classList.add('hidden');
}

function displayResults() {
    if (!analysisData) return;
    
    displaySummaryCards();
    displayOrchestrationTab();
    displayDependenciesTab();
    displayQualityTab();
    displayImpactTab();
    
    // Switch to orchestration tab by default
    switchTab('orchestrationTab');
}

function displaySummaryCards() {
    const { dags, quality_metrics, orchestration_graph, impact_analysis } = analysisData;
    
    const cards = [
        {
            title: 'Total DAGs',
            value: dags.length,
            icon: 'fas fa-project-diagram',
            color: 'blue'
        },
        {
            title: 'Health Score',
            value: Math.round(quality_metrics.overall_score),
            suffix: '/100',
            icon: 'fas fa-heart',
            color: getScoreColor(quality_metrics.overall_score)
        },
        {
            title: 'High Risk Components',
            value: quality_metrics.risk_assessment.high_risk_components.length,
            icon: 'fas fa-exclamation-triangle',
            color: 'red'
        },
        {
            title: 'Trigger Relationships',
            value: orchestration_graph.trigger_relationships.length,
            icon: 'fas fa-arrows-alt',
            color: 'green'
        }
    ];
    
    elements.summaryCards.innerHTML = cards.map(card => `
        <div class="bg-white p-6 rounded-lg shadow-md">
            <div class="flex items-center">
                <div class="flex-shrink-0">
                    <div class="w-8 h-8 bg-${card.color}-100 rounded-md flex items-center justify-center">
                        <i class="${card.icon} text-${card.color}-600"></i>
                    </div>
                </div>
                <div class="ml-5 w-0 flex-1">
                    <dl>
                        <dt class="text-sm font-medium text-gray-500 truncate">${card.title}</dt>
                        <dd class="text-lg font-medium text-gray-900">${card.value}${card.suffix || ''}</dd>
                    </dl>
                </div>
            </div>
        </div>
    `).join('');
}

function getScoreColor(score) {
    if (score >= 80) return 'green';
    if (score >= 60) return 'yellow';
    return 'red';
}

function displayOrchestrationTab() {
    const { orchestration_graph } = analysisData;
    
    // Create network visualization
    const container = document.getElementById('orchestrationViz');
    const data = {
        nodes: new vis.DataSet(orchestration_graph.nodes.map(node => ({
            ...node,
            color: getNodeColor(node.type),
            shape: node.type === 'dag' ? 'box' : 'ellipse',
            font: { color: '#333' }
        }))),
        edges: new vis.DataSet(orchestration_graph.edges.map(edge => ({
            ...edge,
            color: { color: edge.color || '#666' },
            width: 2
        })))
    };
    
    const options = {
        layout: {
            improvedLayout: true
        },
        physics: {
            enabled: true,
            stabilization: { iterations: 100 }
        },
        interaction: {
            dragNodes: true,
            dragView: true,
            zoomView: true
        },
        edges: {
            arrows: {
                to: { enabled: true, scaleFactor: 1 }
            }
        }
    };
    
    orchestrationNetwork = new vis.Network(container, data, options);
    
    // Display orchestration details
    displayOrchestrationDetails();
}

function getNodeColor(type) {
    const colors = {
        'dag': '#3B82F6',
        'module': '#10B981',
        'dataset': '#F59E0B'
    };
    return colors[type] || '#6B7280';
}

function displayOrchestrationDetails() {
    const { orchestration_graph } = analysisData;
    const detailsContainer = document.getElementById('orchestrationDetails');
    
    const triggerDetails = `
        <div class="bg-gray-50 p-4 rounded-lg">
            <h4 class="font-medium text-gray-800 mb-3">Trigger Relationships</h4>
            ${orchestration_graph.trigger_relationships.length === 0 ? 
                '<p class="text-gray-500">No trigger relationships found.</p>' :
                orchestration_graph.trigger_relationships.map(trigger => `
                    <div class="mb-2 p-2 bg-white rounded border-l-4 border-blue-500">
                        <div class="font-medium">${trigger.source_dag} → ${trigger.target_dag}</div>
                        <div class="text-sm text-gray-600">Task: ${trigger.task_id} (Line ${trigger.line_number})</div>
                    </div>
                `).join('')
            }
        </div>
    `;
    
    const datasetDetails = `
        <div class="bg-gray-50 p-4 rounded-lg">
            <h4 class="font-medium text-gray-800 mb-3">Dataset Dependencies</h4>
            ${orchestration_graph.dataset_dependencies.length === 0 ? 
                '<p class="text-gray-500">No dataset dependencies found.</p>' :
                orchestration_graph.dataset_dependencies.map(dataset => `
                    <div class="mb-2 p-2 bg-white rounded border-l-4 border-green-500">
                        <div class="font-medium">${dataset.dag_id}</div>
                        <div class="text-sm text-gray-600">${dataset.type}: ${dataset.dataset}</div>
                    </div>
                `).join('')
            }
        </div>
    `;
    
    detailsContainer.innerHTML = triggerDetails + datasetDetails;
}

function displayDependenciesTab() {
    const { code_dependencies } = analysisData;
    
    displayDependencyTree(code_dependencies.dag_dependencies);
    displayCommonUsage(code_dependencies.common_usage_map);
}

function displayDependencyTree(dagDependencies) {
    const treeContainer = document.getElementById('dependencyTreeContent');
    let treeHTML = '';
    
    for (const [dagId, deps] of Object.entries(dagDependencies)) {
        treeHTML += `
            <div class="mb-4">
                <div class="font-medium text-blue-600 cursor-pointer flex items-center" onclick="toggleDependencyNode('${dagId}')">
                    <i class="fas fa-chevron-right mr-2 transform transition-transform" id="arrow-${dagId}"></i>
                    ${dagId}
                </div>
                <div class="ml-6 mt-2 hidden" id="deps-${dagId}">
                    ${deps.common_imports.length > 0 ? `
                        <div class="mb-2">
                            <div class="text-sm font-medium text-gray-700">Common Imports:</div>
                            ${deps.common_imports.map(imp => `
                                <div class="text-xs text-gray-600 ml-2">
                                    ${imp.module}.${imp.name} (Line ${imp.line_number})
                                </div>
                            `).join('')}
                        </div>
                    ` : ''}
                    ${deps.external_imports.length > 0 ? `
                        <div class="mb-2">
                            <div class="text-sm font-medium text-gray-700">External Imports:</div>
                            ${deps.external_imports.slice(0, 5).map(imp => `
                                <div class="text-xs text-gray-600 ml-2">
                                    ${imp.module}.${imp.name}
                                </div>
                            `).join('')}
                            ${deps.external_imports.length > 5 ? `<div class="text-xs text-gray-500 ml-2">... and ${deps.external_imports.length - 5} more</div>` : ''}
                        </div>
                    ` : ''}
                </div>
            </div>
        `;
    }
    
    treeContainer.innerHTML = treeHTML || '<p class="text-gray-500">No dependencies found.</p>';
}

function displayCommonUsage(commonUsageMap) {
    const usageContainer = document.getElementById('commonUsageContent');
    let usageHTML = '';
    
    const sortedUsage = Object.entries(commonUsageMap).sort((a, b) => b[1].length - a[1].length);
    
    for (const [moduleFunc, usages] of sortedUsage.slice(0, 10)) {
        const riskLevel = usages.length > 5 ? 'HIGH' : usages.length > 2 ? 'MEDIUM' : 'LOW';
        const riskColor = riskLevel === 'HIGH' ? 'red' : riskLevel === 'MEDIUM' ? 'yellow' : 'green';
        
        usageHTML += `
            <div class="mb-3 p-3 bg-white rounded border-l-4 border-${riskColor}-500">
                <div class="flex justify-between items-start">
                    <div class="font-medium text-gray-800">${moduleFunc}</div>
                    <span class="text-xs px-2 py-1 bg-${riskColor}-100 text-${riskColor}-800 rounded">
                        ${riskLevel} RISK
                    </span>
                </div>
                <div class="text-sm text-gray-600 mt-1">
                    Used by ${usages.length} DAGs: ${usages.map(u => u.dag_id).join(', ')}
                </div>
            </div>
        `;
    }
    
    usageContainer.innerHTML = usageHTML || '<p class="text-gray-500">No common usage found.</p>';
}

function displayQualityTab() {
    const { quality_metrics } = analysisData;
    
    // Display overall score
    document.getElementById('overallScore').textContent = Math.round(quality_metrics.overall_score);
    document.getElementById('scoreGrade').textContent = `Grade: ${getScoreGrade(quality_metrics.overall_score)}`;
    
    // Create quality metrics chart
    createQualityMetricsChart(quality_metrics.metrics);
    
    // Create risk distribution chart
    createRiskDistributionChart(quality_metrics.risk_assessment);
    
    // Display issues
    displayIssues(quality_metrics.issues);
    
    // Display recommendations
    displayRecommendations(quality_metrics.recommendations);
}

function getScoreGrade(score) {
    if (score >= 90) return 'A+';
    if (score >= 80) return 'A';
    if (score >= 70) return 'B';
    if (score >= 60) return 'C';
    if (score >= 50) return 'D';
    return 'F';
}

function createQualityMetricsChart(metrics) {
    const ctx = document.getElementById('qualityMetricsChart').getContext('2d');
    
    if (qualityMetricsChart) {
        qualityMetricsChart.destroy();
    }
    
    qualityMetricsChart = new Chart(ctx, {
        type: 'radar',
        data: {
            labels: ['Complexity', 'Maintainability', 'Documentation', 'Error Handling'],
            datasets: [{
                label: 'Quality Metrics',
                data: [
                    Math.max(0, 100 - metrics.average_complexity * 2),
                    metrics.average_maintainability,
                    metrics.documentation_coverage * 100,
                    metrics.error_handling_coverage * 100
                ],
                backgroundColor: 'rgba(59, 130, 246, 0.2)',
                borderColor: 'rgba(59, 130, 246, 1)',
                pointBackgroundColor: 'rgba(59, 130, 246, 1)',
                pointBorderColor: '#fff',
                pointHoverBackgroundColor: '#fff',
                pointHoverBorderColor: 'rgba(59, 130, 246, 1)'
            }]
        },
        options: {
            elements: {
                line: {
                    borderWidth: 3
                }
            },
            scales: {
                r: {
                    angleLines: {
                        display: false
                    },
                    suggestedMin: 0,
                    suggestedMax: 100
                }
            }
        }
    });
}

function createRiskDistributionChart(riskAssessment) {
    const ctx = document.getElementById('riskDistributionChart').getContext('2d');
    
    if (riskDistributionChart) {
        riskDistributionChart.destroy();
    }
    
    const riskCounts = {
        'Low Risk': analysisData.dags.length - riskAssessment.high_risk_components.length,
        'High Risk': riskAssessment.high_risk_components.length
    };
    
    riskDistributionChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: Object.keys(riskCounts),
            datasets: [{
                data: Object.values(riskCounts),
                backgroundColor: [
                    'rgba(16, 185, 129, 0.8)',
                    'rgba(239, 68, 68, 0.8)'
                ],
                borderColor: [
                    'rgba(16, 185, 129, 1)',
                    'rgba(239, 68, 68, 1)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false
        }
    });
}

function displayIssues(issues) {
    const issuesList = document.getElementById('issuesList');
    
    if (!issues || issues.length === 0) {
        issuesList.innerHTML = '<p class="text-gray-500">No major issues found.</p>';
        return;
    }
    
    const issuesHTML = issues.slice(0, 10).map(issue => {
        const severityColor = issue.severity === 'HIGH' ? 'red' : issue.severity === 'MEDIUM' ? 'yellow' : 'green';
        
        return `
            <div class="mb-2 p-2 border-l-4 border-${severityColor}-500 bg-${severityColor}-50">
                <div class="flex justify-between items-start">
                    <div class="text-sm font-medium text-gray-800">${issue.issue}</div>
                    <span class="text-xs px-2 py-1 bg-${severityColor}-100 text-${severityColor}-800 rounded">
                        ${issue.severity}
                    </span>
                </div>
                <div class="text-xs text-gray-600 mt-1">${issue.file_path}</div>
            </div>
        `;
    }).join('');
    
    issuesList.innerHTML = issuesHTML;
}

function displayRecommendations(recommendations) {
    const recommendationsList = document.getElementById('recommendationsList');
    
    if (!recommendations || recommendations.length === 0) {
        recommendationsList.innerHTML = '<p class="text-gray-500">No specific recommendations.</p>';
        return;
    }
    
    const recommendationsHTML = recommendations.map(rec => {
        const priorityColor = rec.priority === 'HIGH' ? 'red' : rec.priority === 'MEDIUM' ? 'yellow' : 'blue';
        
        return `
            <div class="mb-3 p-3 border-l-4 border-${priorityColor}-500 bg-gray-50">
                <div class="flex justify-between items-start mb-1">
                    <div class="text-sm font-medium text-gray-800">${rec.category}</div>
                    <span class="text-xs px-2 py-1 bg-${priorityColor}-100 text-${priorityColor}-800 rounded">
                        ${rec.priority}
                    </span>
                </div>
                <div class="text-sm text-gray-600">${rec.recommendation}</div>
            </div>
        `;
    }).join('');
    
    recommendationsList.innerHTML = recommendationsHTML;
}

function displayImpactTab() {
    const { impact_analysis } = analysisData;
    
    // Populate file selector
    populateFileSelector();
    
    // Display risk matrix
    displayRiskMatrix(impact_analysis.risk_matrix);
}

function populateFileSelector() {
    const targetFileSelect = document.getElementById('targetFile');
    const files = new Set();
    
    // Add all Python files from DAGs
    analysisData.dags.forEach(dag => {
        files.add(dag.relative_path);
    });
    
    // Add files from dependencies
    Object.values(analysisData.code_dependencies.dag_dependencies).forEach(deps => {
        deps.common_imports.forEach(imp => {
            files.add(imp.module.replace('.', '/') + '.py');
        });
    });
    
    targetFileSelect.innerHTML = '<option value="">Select a file...</option>';
    Array.from(files).sort().forEach(file => {
        const option = document.createElement('option');
        option.value = file;
        option.textContent = file;
        targetFileSelect.appendChild(option);
    });
}

function displayRiskMatrix(riskMatrix) {
    const riskMatrixContainer = document.getElementById('riskMatrix');
    
    let matrixHTML = '<div class="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-4">';
    
    for (const [riskLevel, dags] of Object.entries(riskMatrix)) {
        const levelName = riskLevel.replace('_', ' ').toUpperCase();
        const levelColor = getRiskLevelColor(riskLevel);
        
        matrixHTML += `
            <div class="bg-${levelColor}-50 border border-${levelColor}-200 rounded-lg p-4">
                <h5 class="font-medium text-${levelColor}-800 mb-2">${levelName} (${dags.length})</h5>
                <div class="space-y-2">
                    ${dags.slice(0, 5).map(dag => `
                        <div class="text-sm bg-white p-2 rounded border">
                            <div class="font-medium">${dag.dag_id}</div>
                            <div class="text-xs text-gray-600">
                                Impact: ${dag.impact_score} | Complexity: ${dag.complexity_score}
                            </div>
                        </div>
                    `).join('')}
                    ${dags.length > 5 ? `<div class="text-xs text-gray-500">... and ${dags.length - 5} more</div>` : ''}
                </div>
            </div>
        `;
    }
    
    matrixHTML += '</div>';
    riskMatrixContainer.innerHTML = matrixHTML;
}

function getRiskLevelColor(riskLevel) {
    const colors = {
        'low_risk': 'green',
        'medium_risk': 'yellow',
        'high_risk': 'orange',
        'critical_risk': 'red'
    };
    return colors[riskLevel] || 'gray';
}

async function handleImpactAnalysisSubmit(e) {
    e.preventDefault();
    
    const formData = new FormData(elements.impactAnalysisForm);
    const targetFile = formData.get('targetFile');
    const targetFunction = formData.get('targetFunction').trim() || null;
    
    if (!targetFile) {
        alert('Please select a target file.');
        return;
    }
    
    const sourceType = document.querySelector('input[name="sourceType"]:checked').value;
    const sourcePath = document.getElementById('sourcePath').value;
    
    try {
        const response = await fetch('/api/impact-analysis', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                source_type: sourceType,
                source_path: sourcePath,
                target_file: targetFile,
                target_function: targetFunction
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Impact analysis failed');
        }
        
        const impactData = await response.json();
        displayImpactResults(impactData);
        
    } catch (error) {
        console.error('Impact analysis error:', error);
        alert(error.message);
    }
}

function displayImpactResults(impactData) {
    const resultsContainer = document.getElementById('impactResults');
    resultsContainer.classList.remove('hidden');
    
    // Risk assessment
    displayRiskAssessment(impactData.risk_assessment);
    
    // Direct impacts
    displayDirectImpacts(impactData.direct_impacts);
    
    // Affected DAGs
    displayAffectedDags(impactData.affected_dags);
    
    // Change recommendations
    displayChangeRecommendations(impactData.recommendations);
}

function displayRiskAssessment(riskAssessment) {
    const container = document.getElementById('riskAssessment');
    const riskColor = getRiskColor(riskAssessment.risk_level);
    
    container.innerHTML = `
        <div class="text-center">
            <div class="w-16 h-16 mx-auto mb-2 bg-${riskColor}-100 rounded-full flex items-center justify-center">
                <i class="fas fa-exclamation-triangle text-2xl text-${riskColor}-600"></i>
            </div>
            <div class="text-lg font-semibold text-${riskColor}-600">${riskAssessment.risk_level}</div>
            <div class="text-sm text-gray-600">Risk Score: ${riskAssessment.risk_score}/100</div>
            <div class="text-xs text-gray-500 mt-2">
                ${riskAssessment.risk_factors.join('<br>')}
            </div>
        </div>
    `;
}

function displayDirectImpacts(directImpacts) {
    const container = document.getElementById('directImpacts');
    
    if (directImpacts.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-sm">No direct impacts found.</p>';
        return;
    }
    
    const impactsHTML = directImpacts.slice(0, 5).map(impact => `
        <div class="text-sm mb-2 p-2 bg-gray-50 rounded">
            <div class="font-medium">${impact.file_path}</div>
            <div class="text-xs text-gray-600">
                ${impact.import_name} (${impact.usage_details.usage_count} usages)
            </div>
        </div>
    `).join('');
    
    container.innerHTML = impactsHTML + 
        (directImpacts.length > 5 ? `<div class="text-xs text-gray-500">... and ${directImpacts.length - 5} more</div>` : '');
}

function displayAffectedDags(affectedDags) {
    const container = document.getElementById('affectedDags');
    
    if (affectedDags.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-sm">No affected DAGs found.</p>';
        return;
    }
    
    const dagsHTML = affectedDags.map(dag => `
        <div class="text-sm mb-2 p-2 bg-blue-50 rounded border-l-4 border-blue-500">
            <div class="font-medium">${dag.dag_id}</div>
            <div class="text-xs text-gray-600">${dag.impact_type} impact</div>
        </div>
    `).join('');
    
    container.innerHTML = dagsHTML;
}

function displayChangeRecommendations(recommendations) {
    const container = document.getElementById('changeRecommendations');
    
    if (!recommendations || recommendations.length === 0) {
        container.innerHTML = '<p class="text-gray-500">No specific recommendations for this change.</p>';
        return;
    }
    
    const groupedRecommendations = recommendations.reduce((groups, rec) => {
        if (!groups[rec.category]) {
            groups[rec.category] = [];
        }
        groups[rec.category].push(rec);
        return groups;
    }, {});
    
    let recommendationsHTML = '';
    
    for (const [category, recs] of Object.entries(groupedRecommendations)) {
        recommendationsHTML += `
            <div class="mb-4">
                <h5 class="font-medium text-gray-800 mb-2">${category}</h5>
                ${recs.map(rec => {
                    const priorityColor = rec.priority === 'HIGH' ? 'red' : rec.priority === 'MEDIUM' ? 'yellow' : 'blue';
                    return `
                        <div class="mb-2 p-2 border-l-4 border-${priorityColor}-500 bg-${priorityColor}-50">
                            <div class="text-sm">${rec.action}</div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    }
    
    container.innerHTML = recommendationsHTML;
}

function getRiskColor(riskLevel) {
    const colors = {
        'LOW': 'green',
        'MEDIUM': 'yellow',
        'HIGH': 'orange',
        'CRITICAL': 'red'
    };
    return colors[riskLevel] || 'gray';
}

// Tab switching functionality
function switchTab(tabId) {
    elements.tabButtons.forEach(button => {
        button.classList.remove('active', 'border-blue-500', 'text-blue-600');
        button.classList.add('border-transparent', 'text-gray-500');
    });
    
    elements.tabContents.forEach(content => {
        content.classList.add('hidden');
    });
    
    const activeButton = document.getElementById(tabId);
    activeButton.classList.add('active', 'border-blue-500', 'text-blue-600');
    activeButton.classList.remove('border-transparent', 'text-gray-500');
    
    const contentId = tabId.replace('Tab', 'Content');
    document.getElementById(contentId).classList.remove('hidden');
}

// Utility functions
function toggleDependencyNode(dagId) {
    const depsElement = document.getElementById(`deps-${dagId}`);
    const arrowElement = document.getElementById(`arrow-${dagId}`);
    
    depsElement.classList.toggle('hidden');
    arrowElement.classList.toggle('rotate-90');
}