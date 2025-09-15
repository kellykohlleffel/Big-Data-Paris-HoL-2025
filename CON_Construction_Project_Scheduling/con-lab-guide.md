# Fivetran Connector SDK Hands on Lab at Big Data London 2025: Construction Project Handling

## Overview
In this 20-minute hands on lab, you'll build a **custom Fivetran connector** using the **Fivetran Connector SDK** and the **Anthropic Workbench** to integrate an industry specific dataset from a custom REST API into Snowflake. You'll then create a **Streamlit (in Snowflake)** application with various tools and dashboards powering key metrics as well as a **Snowflake Cortex AI-driven** feature to drive even deeper analytics, descriptive, and prescriptive insights.

The Construction (CON) custom connector should fetch project scheduling records from a REST API and load them into a single table called `con_records` in your Snowflake database. The connector should deliver detailed information that simulates data from project management platforms, enterprise resource planning systems, weather forecasting services, and equipment management databases. It should handle authentication, pagination, error handling, and maintain state between sync runs using a cursor-based approach.

## Lab Steps Quick Access

- [Step 1: Create a Custom Connector with the Fivetran Connector SDK (8 minutes)](#step-1-create-a-custom-connector-with-the-fivetran-connector-sdk-8-minutes)
- [Step 2: Start Data Sync in Fivetran (3 minutes)](#step-2-start-data-sync-in-fivetran-3-minutes)
- [Step 3: Create a Streamlit in Snowflake Gen AI Data App (5 minutes)](#step-3-create-a-streamlit-in-snowflake-gen-ai-data-app-5-minutes)

## Lab Environment
- MacBook Pro laptop with Chrome browser, VS Code, DBeaver and the Fivetran Connector SDK
- 6 Chrome tabs are pre-configured (leave them open throughout the lab):
  - Tab 1: GitHub Lab Repo: Lab Guide
  - Tab 2: Anthropic Workbench: AI Code Generation Assistant (Claude)
  - Tab 3: Fivetran: Automated Data Movement Platform
  - Tab 4: Snowflake: Data and AI Platform including Cortex (AI functions) and Streamlit (data apps)
  - Tab 5: Fivetran Connector SDK Examples Open Source Github Repository
  - Tab 6: Fivetran Connector SDK Docs

## Mac Keyboard Shortcuts Reference
- **Command+A**: Select all
- **Command+C**: Copy
- **Command+V**: Paste
- **Command+S**: Save
- **Command+Tab**: Switch between applications
- **Control+`**: Open terminal in VS Code

## Trackpad/MousePad Reference
- **Single finger tap**: Left click
- **Two finger tap**: Right click
- **Two finger slide**: Scroll up and down

## Step 1: Create a Custom Connector with the Fivetran Connector SDK (8 minutes)

### 1.1 Generate the Custom Connector Code Using AI Code Generation Assistance
1. Switch to **Chrome Tab 2 (Anthropic Workbench)**
2. Copy and paste the following **User prompt** into the workbench:

<details>
  <summary>Click to expand the User prompt and click the Copy icon in the right corner</summary>

```
Here is the API spec for this dataset: https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/con_api_spec

Provide a custom Fivetran connector for Construction Project Scheduling for the con_data endpoint. There is only one dataset called con_records.

Here is a sample record:
{
    "actual_end_date": "2024-10-24",
    "actual_start_date": "2024-09-08",
    "cost_performance_index": 1.02,
    "critical_path_flag": true,
    "data_timestamp": "2024-11-17 04:46:00",
    "equipment_id": "EQ_3861",
    "equipment_status": "Setup",
    "equipment_utilization_rate": 0.82,
    "last_updated_epoch": 1748797200,
    "material_delivery_date": "2024-11-08",
    "material_delivery_status": "Cancelled",
    "percent_complete": 66.1,
    "precipitation_probability": 32.3,
    "project_id": "PROJ_2824",
    "project_name": "Hotel Resort Complex",
    "record_id": "44638d61-7843-4319-9353-ecb170f3e441",
    "resource_availability": 0.88,
    "resource_cost_per_hour": 131.68,
    "resource_id": "RES_5372",
    "resource_type": "Material",
    "risk_score": 77.5,
    "schedule_performance_index": 1.15,
    "scheduled_end_date": "2024-10-09",
    "scheduled_start_date": "2024-08-30",
    "supplier_id": "SUP_149",
    "task_id": "TASK_72163",
    "task_name": "Drywall Installation",
    "task_status": "Completed",
    "temperature_fahrenheit": 89.7,
    "weather_condition": "Snow",
    "wind_speed_mph": 0.1
}
```
</details>

3. Click the black **Run** button in the upper right
4. After Claude generates the connector.py code, you will see a response similar to the example connector, but updated for the construction dataset.
5. Click [con_api_spec](https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/con_api_spec) if you'd like to see the API spec.

### 1.2 Debug and Deploy the Custom Connector in VS Code
1. When you see the connector.py code generated in the Anthropic Workbench, click the **Copy** button in the upper right of the code connector.py code block
2. Go to **VS Code** with Command + Tab or open from the dock
3. Click on the `connector.py` file in your project
4. Press Command+V to paste the connector code into the `connector.py` file
5. Save the updated `connector.py` by pressing Command+S
6. To test your code locally with configuration values specified in `configuration.json`, you can run the default Fivetran Connector SDK debug command:  
   `fivetran debug --configuration configuration.json`  
   This command provides out-of-the-box debugging without any additional scripting. 
7. We have created a helper script to debug and validate your connector with enhanced logging, state clearing and data validation. To run the helper script please run it in the VS Code terminal (bottom right). You can copy the `debug_and_validate` command using the icon on the right:

```
./debug_and_validate.sh
```

8. When prompted with "Do you want to continue? (Y/N):", type `Y` and press Enter.

    - You'll see output displaying the results of the debug script including:

        - Resets the connector state by deleting the existing warehouse.db file and any saved sync checkpoints to start with a clean slate.

        - Runs the fivetran debug command using your configuration file to test the connector in real time.(debug emulates a regular Fivetran sync where the schema() and update() methods are called).

        - Execute the Custom `Connector.py` code you wrote fetching data and executing pagination and checkpoint saving for incremental sync as per your custom code and the current state variable. The helper script emulates an initial full sync.

        - Verifies data loading and schema creation by simulating a full sync (in this case, upserting 600 records into con_records).

        - Queries and displays sample records from the resulting DuckDB table to confirm the connector outputs expected data.

9. Fivetran provides a built-in command to deploy your connector directly using the SDK:  
   `fivetran deploy --api-key <BASE64_API_KEY> --destination <DESTINATION_NAME> --connection <CONNECTION_NAME>`  
   This command deploys your code to Fivetran and creates or updates the connection. If the connection already exists, it prompts you before overwriting.  
   You can also provide additional optional parameters:  
   - `--configuration` to pass configuration values  
   - `--force` to bypass confirmation prompts, great for CI/CD uses  
   - `--python-version` to specify Python runtime  
   - `--hybrid-deployment-agent-id` for non-default hybrid agent selection  

10. To simplify the lab experience, we've created a helper script that wraps the deploy logic. Run the following command in the VS Code terminal (copy the command using the icon in the right corner):

```
./deploy.sh
```

11. Click enter twice to accept the default values for the Fivetran Account Name and the Fivetran Destination. When prompted for the **connection name**, type in:

```
construction_con_connector
```

12. Press Enter to deploy your new custom connector to Fivetran.

## Step 2: Start Data Sync in Fivetran (3 minutes)

1. Switch to **Chrome Tab 3 (Fivetran Automated Data Movement)**
2. Refresh the page and find your newly created connection named "construction_con_connector" in the connections list
3. Click on the connection to open the **Status** page
4. Click the **Start Initial Sync** button
5. You should see a status message indicating that the sync is **Active** and that it is the first time syncing data for this connection.
6. Once your sync completes, you will see a message "Next sync will run in x hours" and if you click on the **1 HOUR** selection on the right side, you will see some sync metrics.
    * You may need to refresh the UI to see updated sync progress and logs in the UI. 
7. Once your sync completes, you will see a message "Next sync will run in x hours" and if you click on the **1 HOUR** selection on the right side, you will see some sync metrics.

## Step 3: Create a Streamlit in Snowflake Gen AI Data App (5 minutes)

### 3.1 Copy the Streamlit Data App Code
1. Copy the Streamlit code below (click the Copy icon in the right corner)

<details>
  <summary>Click to expand the Streamlit Data App Code and click the Copy icon in the right corner</summary>

```python
import streamlit as st
import pandas as pd
import altair as alt
import time
import json
import re
from datetime import datetime
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="projectflow_ai_–_intelligent_construction_schedule_optimization",
    page_icon="https://i.imgur.com/Og6gFnB.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add CSS for agent progress styling
st.markdown("""
<style>
.agent-current {
    background-color: #e3f2fd;
    border-left: 4px solid #2196f3;
    padding: 10px;
    margin: 5px 0;
    border-radius: 5px;
    font-weight: 500;
}

.agent-completed {
    background-color: #e8f5e8;
    border-left: 4px solid #4caf50;
    padding: 8px;
    margin: 3px 0;
    border-radius: 5px;
    font-size: 0.9em;
    color: #2e7d32;
}

.agent-container {
    background-color: #f8f9fa;
    padding: 15px;
    border-radius: 10px;
    margin: 10px 0;
    border: 1px solid #e0e0e0;
}

.agent-status-active {
    color: #4CAF50;
    font-weight: bold;
    font-size: 1.1em;
}

.agent-button-container {
    display: flex;
    gap: 10px;
    align-items: center;
    margin: 10px 0;
}

.agent-report-header {
    background-color: #f8f9fa;
    padding: 10px;
    border-radius: 5px;
    margin: 10px 0;
    border-left: 4px solid #2196F3;
}
</style>
""", unsafe_allow_html=True)

solution_name = '''Solution 2: ProjectFlow AI – Intelligent Construction Schedule Optimization'''
solution_name_clean = '''projectflow_ai_–_intelligent_construction_schedule_optimization'''
table_name = '''CON_RECORDS'''
table_description = '''Consolidated table containing project schedules, resource allocation, weather conditions, supply chain status, and equipment data for AI-driven construction schedule optimization'''
solution_content = '''Solution 2: ProjectFlow AI – Intelligent Construction Schedule Optimization**

**Primary Business Challenge:**
Construction projects consistently experience delays and cost overruns, with 77% of projects finishing late and 55% over budget due to poor scheduling coordination, resource conflicts, and inability to predict and adapt to disruptions.

**Key Features:**
• Dynamic schedule optimization based on real-time project conditions
• Automated resource allocation and conflict resolution
• Weather-aware scheduling with contingency planning
• Supplier and subcontractor coordination optimization
• Predictive delay identification with mitigation recommendations

**Data Sources:**
• Project Management Platforms: Microsoft Project, Primavera P6, Smartsheet
• Enterprise Resource Planning (ERP): SAP, Oracle Construction and Engineering, Sage 300 CRE
• Weather Forecasting: The Weather Channel, DTN, Weatherzone
• Supply Chain Management: Oracle SCM, SAP Ariba, Coupa
• Equipment Management: B2W Software, HCSS Equipment360, Fleetio

**Competitive Advantage:**
ProjectFlow AI provides dynamic, AI-driven schedule optimization that continuously adapts to changing conditions, unlike static traditional scheduling tools, enabling proactive decision-making and resource optimization that competitors cannot match.

**Key Stakeholders:**
• Project Managers, Construction Schedulers, Resource Coordinators, Operations Directors, Portfolio Managers
• **Top C-Level Executive:** Chief Operating Officer (COO)

**Technical Approach:**
Employs reinforcement learning algorithms for dynamic schedule optimization, natural language processing to analyze project communications and identify risks, and generative AI to create alternative scheduling scenarios and resource allocation strategies.

**Expected Business Results:**

• 45 fewer project delays per year
**500 annual projects × 18% baseline delay rate × 50% reduction = 45 fewer delays/year**

• $ 3,600,000 in project cost savings annually
**$ 50,000,000 annual project costs × 12% typical overrun × 60% reduction = $ 3,600,000 savings/year**

• 7,200 hours of improved resource utilization annually
**60,000 total resource hours × 20% idle time × 60% reduction = 7,200 hours/year**

• $ 900,000 in penalty avoidance annually
**$ 12,000,000 potential annual penalties × 7.5% reduction = $ 900,000 penalty avoidance/year**

**Success Metrics:**
• Schedule performance index (SPI)
• Resource utilization rates
• Project completion variance from baseline
• Critical path optimization effectiveness
• Stakeholder satisfaction scores

**Risk Assessment:**
**Challenges:** Data quality inconsistencies across systems, resistance to AI-driven scheduling changes, complexity of multi-project resource optimization
**Mitigation:** Implement data validation protocols, provide comprehensive change management training, start with pilot projects to demonstrate value

**Long-term Evolution:**
Development toward autonomous project orchestration with IoT integration...'''

# Display logo and title inline
st.markdown(f'''
<div style="display:flex; align-items:center; margin-bottom:15px">
    <img src="https://i.imgur.com/Og6gFnB.png" width="100" style="margin-right:15px">
    <div>
        <h1 style="font-size:2.2rem; margin:0; padding:0">{solution_name_clean.replace('_', ' ').title()}</h1>
        <p style="font-size:1.1rem; color:gray; margin:0; padding:0">Fivetran and Cortex-powered Streamlit in Snowflake data application for Construction Schedule Optimization</p>
    </div>
</div>
''', unsafe_allow_html=True)

# Define available models as strings
MODELS = [
    "openai-gpt-oss-120b", "openai-gpt-4.1", "openai-gpt-5", "openai-gpt-5-mini", "openai-gpt-5-nano", "openai-gpt-5-chat", "claude-4-sonnet", "claude-3-7-sonnet", "claude-3-5-sonnet", "llama3.1-8b", "llama3.1-70b", "llama4-maverick", "llama4-scout", "llama3.2-1b", "snowflake-llama-3.1-405b", "snowflake-llama-3.3-70b", "mistral-large2", "mistral-7b", "deepseek-r1", "snowflake-arctic", "reka-flash", "jamba-instruct", "gemma-7b"
]

if 'insights_history' not in st.session_state:
    st.session_state.insights_history = []

if 'data_cache' not in st.session_state:
    st.session_state.data_cache = {}

# Initialize completed steps for each focus area
focus_areas = ["Overall Performance", "Optimization Opportunities", "Financial Impact", "Strategic Recommendations"]
for area in focus_areas:
    if f'{area.lower().replace(" ", "_")}_completed_steps' not in st.session_state:
        st.session_state[f'{area.lower().replace(" ", "_")}_completed_steps'] = []

try:
    session = get_active_session()
except Exception as e:
    st.error(f"❌ Error connecting to Snowflake: {str(e)}")
    st.stop()

def query_snowflake(query):
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        return pd.DataFrame()

def load_data():
    query = f"SELECT * FROM {table_name} LIMIT 1000"
    df = query_snowflake(query)
    df.columns = [col.lower() for col in df.columns]
    return df

def call_cortex_model(prompt, model_name):
    try:
        cortex_query = "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS response"
        response = session.sql(cortex_query, params=[model_name, prompt]).collect()[0][0]
        return response
    except Exception as e:
        st.error(f"❌ Cortex error: {str(e)}")
        return None

def get_focus_area_info(focus_area):
    """Get business challenge and agent solution for each focus area"""
    
    focus_info = {
        "Overall Performance": {
            "challenge": "Project Managers and Chief Operating Officers manually review hundreds of construction project schedules, resource allocations, and performance metrics daily, spending 4+ hours analyzing schedule performance indices, weather impacts, and equipment utilization to identify critical project bottlenecks and scheduling optimization opportunities.",
            "solution": "Autonomous construction scheduling workflow that analyzes project schedules, resource metrics, performance data, and weather conditions to generate automated project summaries, identify scheduling bottlenecks, and produce prioritized construction insights with adaptive schedule optimization recommendations."
        },
        "Optimization Opportunities": {
            "challenge": "Construction Schedulers and Operations Directors spend 5+ hours daily manually identifying inefficiencies in project scheduling strategies, resource allocation criteria, and weather contingency planning across multiple construction projects and geographic locations.",
            "solution": "AI-powered construction scheduling optimization analysis that automatically detects schedule performance gaps, resource utilization inefficiencies, and project delivery improvements with specific implementation recommendations for Microsoft Project, Primavera P6, and SAP Construction system integration."
        },
        "Financial Impact": {
            "challenge": "Chief Operating Officers manually calculate complex ROI metrics across construction project activities and resource management performance, requiring 4+ hours of cost modeling to assess project efficiency and schedule optimization across the construction portfolio.",
            "solution": "Automated construction financial analysis that calculates comprehensive project scheduling ROI, identifies resource cost reduction opportunities across project categories, and projects schedule performance benefits with detailed construction cost forecasting."
        },
        "Strategic Recommendations": {
            "challenge": "Chief Operating Officers spend hours manually analyzing digital transformation opportunities and developing strategic construction technology roadmaps for scheduling advancement and adaptive project management implementation across construction portfolios.",
            "solution": "Strategic construction scheduling intelligence workflow that analyzes competitive advantages against traditional project scheduling processes, identifies AI and adaptive scheduling integration opportunities, and creates prioritized digital construction transformation roadmaps."
        }
    }
    
    return focus_info.get(focus_area, {"challenge": "", "solution": ""})

def generate_insights_with_agent_workflow(data, focus_area, model_name, progress_placeholder=None):
    """Generate insights using AI agent workflow - Construction Schedule Optimization focused version"""
    
    try:
        # FIRST: Generate the actual insights (behind the scenes)
        insights = generate_insights(data, focus_area, model_name)
        
        # THEN: Prepare for animation
        session_key = f'{focus_area.lower().replace(" ", "_")}_completed_steps'
        st.session_state[session_key] = []
        
        def update_progress(step_name, progress_percent, details, results):
            """Update progress display with completed steps"""
            if progress_placeholder:
                with progress_placeholder.container():
                    # Progress bar
                    st.progress(progress_percent / 100)
                    st.write(f"**{step_name} ({progress_percent}%)**")
                    
                    if details:
                        st.markdown(f'<div class="agent-current">{details}</div>', unsafe_allow_html=True)
                    
                    if results:
                        st.session_state[session_key].append((step_name, results))
                        
                    # Display completed steps
                    for completed_step, completed_result in st.session_state[session_key]:
                        st.markdown(f'<div class="agent-completed">✅ {completed_step}: {completed_result}</div>', unsafe_allow_html=True)
        
        # Calculate real data for enhanced context
        total_records = len(data)
        key_metrics = ["schedule_performance_index", "cost_performance_index", "equipment_utilization_rate", "percent_complete"]
        available_metrics = [col for col in key_metrics if col in data.columns]
        
        # Calculate enhanced construction data insights
        avg_spi = data['schedule_performance_index'].mean() if 'schedule_performance_index' in data.columns else 0
        avg_cpi = data['cost_performance_index'].mean() if 'cost_performance_index' in data.columns else 0
        unique_projects = len(data['project_id'].unique()) if 'project_id' in data.columns else 0
        unique_resources = len(data['resource_id'].unique()) if 'resource_id' in data.columns else 0
        avg_completion = data['percent_complete'].mean() if 'percent_complete' in data.columns else 0
        critical_path_rate = data['critical_path_flag'].mean() if 'critical_path_flag' in data.columns else 0
        
        # Define enhanced agent workflows for each focus area
        if focus_area == "Overall Performance":
            steps = [
                ("Construction Project Data Initialization", 15, f"Loading comprehensive construction scheduling dataset with enhanced validation across {total_records} project records and {unique_projects} active projects", f"Connected to {len(available_metrics)} performance metrics across {len(data.columns)} total construction data dimensions"),
                ("Schedule Performance Assessment", 35, f"Advanced calculation of construction scheduling indicators with performance analysis (avg SPI: {avg_spi:.3f})", f"Computed construction metrics: {avg_spi:.3f} schedule performance, {avg_cpi:.3f} cost performance, {avg_completion:.1%} completion rate"),
                ("Construction Pattern Recognition", 55, f"Sophisticated identification of scheduling patterns with resource correlation analysis across {unique_resources} resources", f"Detected significant patterns in {len(data['weather_condition'].unique()) if 'weather_condition' in data.columns else 'N/A'} weather conditions with resource correlation analysis completed"),
                ("AI Construction Intelligence Processing", 75, f"Processing comprehensive construction data through {model_name} with advanced reasoning for scheduling optimization insights", f"Enhanced AI analysis of construction scheduling effectiveness across {total_records} project records completed"),
                ("Construction Performance Report Compilation", 100, f"Professional construction scheduling analysis with evidence-based recommendations and actionable project insights ready", f"Comprehensive construction performance report with {len(available_metrics)} performance metrics analysis and resource optimization recommendations generated")
            ]
            
        elif focus_area == "Optimization Opportunities":
            resource_utilization = data['equipment_utilization_rate'].mean() if 'equipment_utilization_rate' in data.columns else 0
            schedule_efficiency = avg_spi * 100 if avg_spi > 0 else 0
            
            steps = [
                ("Construction Optimization Data Preparation", 12, f"Advanced loading of construction scheduling data with enhanced validation across {total_records} records for scheduling improvement identification", f"Prepared {unique_projects} active projects, {unique_resources} resources for optimization analysis with {critical_path_rate:.1%} critical path coverage"),
                ("Schedule Performance Inefficiency Detection", 28, f"Sophisticated analysis of construction scheduling strategies and resource performance with evidence-based inefficiency identification", f"Identified optimization opportunities across {unique_projects} project categories with scheduling and resource management gaps"),
                ("Construction Resource Correlation Analysis", 45, f"Enhanced examination of relationships between project types, weather conditions, and schedule performance rates", f"Analyzed correlations between construction characteristics and project outcomes across {total_records} construction records"),
                ("ERP Integration Optimization", 65, f"Comprehensive evaluation of construction scheduling integration with existing Microsoft Project, Primavera P6, and SAP Construction systems", f"Assessed integration opportunities across {len(data.columns)} data points and construction scheduling system optimization needs"),
                ("AI Construction Intelligence", 85, f"Generating advanced construction optimization recommendations using {model_name} with scheduling reasoning and implementation strategies", f"AI-powered construction scheduling optimization strategy across {unique_projects} project categories and performance improvements completed"),
                ("Construction Strategy Finalization", 100, f"Professional construction optimization report with prioritized implementation roadmap and resource impact analysis ready", f"Comprehensive optimization strategy with {len(available_metrics)} performance improvement areas and construction scheduling implementation plan generated")
            ]
            
        elif focus_area == "Financial Impact":
            total_resource_cost = data['resource_cost_per_hour'].mean() * unique_resources * 2000 if 'resource_cost_per_hour' in data.columns else 0
            cost_savings = total_resource_cost * 0.15 if total_resource_cost > 0 else 0
            
            steps = [
                ("Construction Financial Data Integration", 15, f"Advanced loading of construction project financial data and resource cost metrics with enhanced validation across {total_records} project records", f"Integrated construction financial data: {avg_spi:.3f} avg SPI, {avg_cpi:.3f} avg CPI across {unique_projects} projects"),
                ("Construction Cost-Benefit Calculation", 30, f"Sophisticated ROI metrics calculation with resource analysis and construction scheduling efficiency cost savings", f"Computed comprehensive cost analysis: resource expenses, delay costs, and ${cost_savings:,.0f} estimated resource optimization potential"),
                ("Resource Management Impact Assessment", 50, f"Enhanced analysis of construction revenue impact with resource utilization metrics and schedule correlation analysis", f"Assessed construction implications: {critical_path_rate:.1%} critical path rate with {unique_resources} resources requiring optimization"),
                ("Construction Resource Efficiency Analysis", 70, f"Comprehensive evaluation of resource allocation efficiency across construction activities with project lifecycle cost optimization", f"Analyzed resource efficiency: {unique_projects} project categories with resource cost reduction opportunities identified"),
                ("AI Construction Financial Modeling", 90, f"Advanced construction project financial projections and scheduling ROI calculations using {model_name} with comprehensive resource cost-benefit analysis", f"Enhanced financial impact analysis and forecasting across {len(available_metrics)} construction cost metrics completed"),
                ("Construction Economics Report Generation", 100, f"Professional construction financial impact analysis with detailed project scheduling ROI calculations and resource cost forecasting ready", f"Comprehensive construction financial report with ${cost_savings:,.0f} cost optimization analysis and scheduling efficiency strategy generated")
            ]
            
        elif focus_area == "Strategic Recommendations":
            # Calculate scheduling efficiency score for Strategic Recommendations
            schedule_efficiency = avg_spi * 100 if avg_spi > 0 else 0
            scheduling_efficiency_score = schedule_efficiency if schedule_efficiency > 0 else 0
            
            steps = [
                ("Construction Technology Assessment", 15, f"Advanced loading of construction scheduling digital context with competitive positioning analysis across {total_records} project records and {unique_projects} active projects", f"Analyzed construction technology landscape: {unique_projects} project categories, {unique_resources} resources, comprehensive construction scheduling digitization assessment completed"),
                ("Construction Competitive Advantage Analysis", 30, f"Sophisticated evaluation of competitive positioning against traditional project scheduling with AI-powered construction optimization effectiveness", f"Assessed competitive advantages: {scheduling_efficiency_score:.1f}% scheduling efficiency, {avg_spi:.3f} SPI vs industry benchmarks"),
                ("Advanced Construction Technology Integration", 50, f"Enhanced analysis of integration opportunities with IoT sensors, real-time weather data, and AI-powered construction sensing across {len(data.columns)} construction data dimensions", f"Identified strategic technology integration: real-time project sensing, adaptive scheduling algorithms, automated resource optimization opportunities"),
                ("Digital Construction Strategy Development", 70, f"Comprehensive development of prioritized digital transformation roadmap with evidence-based construction technology adoption strategies", f"Created sequenced implementation plan across {unique_projects} project categories with advanced construction scheduling technology integration opportunities"),
                ("AI Construction Strategic Processing", 85, f"Advanced construction scheduling strategic recommendations using {model_name} with long-term competitive positioning and construction technology analysis", f"Enhanced strategic analysis with construction competitive positioning and digital transformation roadmap completed"),
                ("Digital Construction Report Generation", 100, f"Professional digital construction transformation roadmap with competitive analysis and scheduling technology implementation plan ready for COO executive review", f"Comprehensive strategic report with {unique_projects}-project implementation plan and construction competitive advantage analysis generated")
            ]
        
        # NOW: Animate the progress with pre-calculated results
        for step_name, progress_percent, details, results in steps:
            update_progress(step_name, progress_percent, details, results)
            time.sleep(1.2)
        
        return insights
        
    except Exception as e:
        if progress_placeholder:
            progress_placeholder.error(f"❌ Enhanced Agent Analysis failed: {str(e)}")
        return f"Enhanced Agent Analysis failed: {str(e)}"

def generate_insights(data, focus_area, model_name):
    data_summary = f"Table: {table_name}\n"
    data_summary += f"Description: {table_description}\n"
    data_summary += f"Records analyzed: {len(data)}\n"

    # Calculate basic statistics for numeric columns only
    numeric_stats = {}
    # Define key construction metrics that should be numeric
    key_metrics = ["percent_complete", "resource_availability", "resource_cost_per_hour", 
                   "temperature_fahrenheit", "precipitation_probability", "wind_speed_mph", 
                   "equipment_utilization_rate", "schedule_performance_index", 
                   "cost_performance_index", "risk_score"]
    
    # Filter to only columns that exist and are actually numeric
    available_metrics = []
    for col in key_metrics:
        if col in data.columns:
            try:
                # Test if the column is actually numeric by trying to calculate mean
                test_mean = pd.to_numeric(data[col], errors='coerce').mean()
                if not pd.isna(test_mean):
                    available_metrics.append(col)
            except:
                # Skip columns that can't be converted to numeric
                continue
    
    for col in available_metrics:
        try:
            numeric_data = pd.to_numeric(data[col], errors='coerce')
            numeric_stats[col] = {
                "mean": numeric_data.mean(),
                "min": numeric_data.min(),
                "max": numeric_data.max(),
                "std": numeric_data.std()
            }
            data_summary += f"- {col} (avg: {numeric_data.mean():.2f}, min: {numeric_data.min():.2f}, max: {numeric_data.max():.2f})\n"
        except Exception as e:
            # Skip columns that cause errors
            continue

    # Get top values for categorical columns
    categorical_stats = {}
    categorical_options = ["project_id", "task_status", "resource_type", "weather_condition", 
                          "material_delivery_status", "equipment_status"]
    for cat_col in categorical_options:
        if cat_col in data.columns:
            try:
                top = data[cat_col].value_counts().head(3)
                categorical_stats[cat_col] = top.to_dict()
                data_summary += f"\nTop {cat_col} values:\n" + "\n".join(f"- {k}: {v}" for k, v in top.items())
            except:
                # Skip columns that cause errors
                continue

    # Calculate correlations if enough numeric columns available
    correlation_info = ""
    if len(available_metrics) >= 2:
        try:
            # Create a dataframe with only the numeric columns
            numeric_df = data[available_metrics].apply(pd.to_numeric, errors='coerce')
            correlations = numeric_df.corr()
            
            # Get the top 3 strongest correlations (absolute value)
            corr_pairs = []
            for i in range(len(correlations.columns)):
                for j in range(i+1, len(correlations.columns)):
                    col1 = correlations.columns[i]
                    col2 = correlations.columns[j]
                    corr_value = correlations.iloc[i, j]
                    if not pd.isna(corr_value):
                        corr_pairs.append((col1, col2, abs(corr_value), corr_value))

            # Sort by absolute correlation value
            corr_pairs.sort(key=lambda x: x[2], reverse=True)

            # Add top correlations to the summary
            if corr_pairs:
                correlation_info = "Top correlations between metrics:\n"
                for col1, col2, _, corr_value in corr_pairs[:3]:
                    correlation_info += f"- {col1} and {col2}: r = {corr_value:.2f}\n"
        except Exception as e:
            correlation_info = "Could not calculate correlations between metrics.\n"

    # Define specific instructions for each focus area tailored to construction industry
    focus_area_instructions = {
        "Overall Performance": """
        For the Overall Performance analysis of ProjectFlow AI in Construction:
        1. Provide a comprehensive analysis of the construction project management and schedule optimization system using project completion rates, resource utilization, and performance indices
        2. Identify significant patterns in project performance, resource allocation efficiency, weather impact on construction activities, and equipment utilization across construction operations
        3. Highlight 3-5 key construction metrics that best indicate project performance (Schedule Performance Index, Cost Performance Index, resource utilization rates, critical path efficiency)
        4. Discuss both strengths and areas for improvement in the AI-powered construction schedule optimization process
        5. Include 3-5 actionable insights for improving construction project delivery based on the project management data
        
        Structure your response with these construction industry focused sections:
        - Construction Project Performance Insights (5 specific insights with supporting schedule and resource data)
        - Project Delivery Trends (3-4 significant trends in schedule adherence and resource optimization)
        - Construction Optimization Recommendations (3-5 data-backed recommendations for improving project delivery operations)
        - Implementation Steps (3-5 concrete next steps for project managers and construction teams)
        """,
        
        "Optimization Opportunities": """
        For the Optimization Opportunities analysis of ProjectFlow AI in Construction:
        1. Focus specifically on areas where construction schedule optimization, resource allocation efficiency, and project delivery can be improved
        2. Identify inefficiencies in project scheduling, resource management, weather-related delays, and equipment utilization across construction operations
        3. Analyze correlations between critical path activities, resource availability, weather conditions, and project performance indices
        4. Prioritize optimization opportunities based on potential impact on reducing project delays and cost overruns
        5. Suggest specific technical or process improvements for integration with existing project management systems (Microsoft Project, Primavera P6, SAP)
        
        Structure your response with these construction industry focused sections:
        - Construction Schedule Optimization Priorities (3-5 areas with highest delay reduction and cost savings potential)
        - Project Delivery Impact Analysis (quantified benefits of addressing each opportunity in terms of schedule performance improvement)
        - ERP Integration Strategy (specific steps for construction teams to implement each optimization)
        - System Integration Recommendations (specific technical changes needed for seamless integration with Microsoft Project, Primavera P6, and SAP systems)
        - Construction Risk Assessment (potential challenges for project managers and how to mitigate them)
        """,
        
        "Financial Impact": """
        For the Financial Impact analysis of ProjectFlow AI in Construction:
        1. Focus on cost-benefit analysis and ROI in construction terms (project cost overruns vs. schedule optimization improvements)
        2. Quantify financial impacts through delay reduction, resource optimization, and penalty avoidance in construction projects
        3. Identify cost savings opportunities across different project types and resource categories
        4. Analyze resource allocation efficiency across different construction phases and weather conditions
        5. Project future financial outcomes based on improved schedule performance and reduced construction delays
        
        Structure your response with these construction industry focused sections:
        - Construction Cost Analysis (breakdown of project costs and potential savings by resource type and project phase)
        - Schedule Optimization Impact (how improved project scheduling affects costs and project delivery)
        - Construction ROI Calculation (specific calculations showing return on investment in terms of delay reduction and penalty avoidance)
        - Cost Reduction Opportunities (specific areas to reduce project costs and improve resource efficiency)
        - Financial Forecasting (projections based on improved construction project performance metrics)
        """,
        
        "Strategic Recommendations": """
        For the Strategic Recommendations analysis of ProjectFlow AI in Construction:
        1. Focus on long-term strategic implications for digital transformation in construction project management
        2. Identify competitive advantages against traditional project scheduling approaches
        3. Suggest new directions for AI integration with emerging construction technologies (e.g., IoT sensors, real-time weather data, autonomous equipment)
        4. Connect recommendations to broader construction goals of reducing project delays and improving client satisfaction
        5. Provide a digital construction roadmap with prioritized initiatives
        
        Structure your response with these construction industry focused sections:
        - Digital Construction Context (how ProjectFlow AI fits into broader digital transformation in construction management)
        - Competitive Advantage Analysis (how to maximize efficiency advantages compared to traditional project scheduling)
        - Construction Technology Strategic Priorities (3-5 high-impact strategic initiatives for improving project management operations)
        - Advanced Scheduling Technology Integration Vision (how to evolve ProjectFlow AI with IoT and real-time data over 1-3 years)
        - Construction Transformation Roadmap (sequenced steps for expanding to predictive project management and autonomous resource allocation)
        """
    }

    # Get the specific instructions for the selected focus area
    selected_focus_instructions = focus_area_instructions.get(focus_area, "")

    prompt = f'''
    You are an expert data analyst specializing in {focus_area.lower()} analysis for construction project management and schedule optimization.

    SOLUTION CONTEXT:
    {solution_name}

    {solution_content}

    DATA SUMMARY:
    {data_summary}

    {correlation_info}

    ANALYSIS INSTRUCTIONS:
    {selected_focus_instructions}

    IMPORTANT GUIDELINES:
    - Base all insights directly on the data provided
    - Use specific metrics and numbers from the data in your analysis
    - Maintain a professional, analytical tone
    - Be concise but thorough in your analysis
    - Focus specifically on {focus_area} as defined in the instructions
    - Ensure your response is unique and tailored to this specific focus area
    - Include a mix of observations, analysis, and actionable recommendations
    - Use bullet points and clear section headers for readability
    - Frame all insights in the context of construction project management and schedule optimization
    '''

    return call_cortex_model(prompt, model_name)

def create_metrics_charts(data):
    """Create metric visualizations for the construction project management data"""
    charts = []
    
    # Schedule Performance Index Distribution
    if 'schedule_performance_index' in data.columns:
        spi_chart = alt.Chart(data).mark_bar().encode(
            alt.X('schedule_performance_index:Q', bin=alt.Bin(maxbins=15), title='Schedule Performance Index (SPI)'),
            alt.Y('count()', title='Number of Projects'),
            color=alt.value('#1f77b4')
        ).properties(
            title='Schedule Performance Index Distribution',
            width=380,
            height=340
        )
        charts.append(('Schedule Performance Distribution', spi_chart))
    
    # Project Completion Status by Resource Type
    if 'percent_complete' in data.columns and 'resource_type' in data.columns:
        completion_chart = alt.Chart(data).mark_boxplot().encode(
            alt.X('resource_type:N', title='Resource Type'),
            alt.Y('percent_complete:Q', title='Percent Complete'),
            color=alt.Color('resource_type:N', legend=None)
        ).properties(
            title='Project Completion by Resource Type',
            width=380,
            height=340
        )
        charts.append(('Project Completion by Resource Type', completion_chart))
    
    # Cost vs Schedule Performance
    if 'cost_performance_index' in data.columns and 'schedule_performance_index' in data.columns:
        performance_chart = alt.Chart(data).mark_point(size=60, opacity=0.7).encode(
            alt.X('schedule_performance_index:Q', title='Schedule Performance Index'),
            alt.Y('cost_performance_index:Q', title='Cost Performance Index'),
            color=alt.Color('risk_score:Q', title='Risk Score', scale=alt.Scale(scheme='reds')),
            tooltip=['schedule_performance_index:Q', 'cost_performance_index:Q', 'risk_score:Q', 'project_id:N']
        ).properties(
            title='Cost vs Schedule Performance',
            width=380,
            height=340
        )
        charts.append(('Cost vs Schedule Performance', performance_chart))
    
    # Weather Impact on Project Progress
    if 'weather_condition' in data.columns and 'percent_complete' in data.columns:
        weather_chart = alt.Chart(data).mark_bar().encode(
            alt.X('weather_condition:N', title='Weather Condition'),
            alt.Y('mean(percent_complete):Q', title='Average Completion %'),
            color=alt.Color('weather_condition:N', legend=None),
            tooltip=['weather_condition:N', 'mean(percent_complete):Q']
        ).properties(
            title='Project Progress by Weather Condition',
            width=380,
            height=340
        )
        charts.append(('Weather Impact on Progress', weather_chart))
    
    # Equipment Utilization Distribution
    if 'equipment_utilization_rate' in data.columns:
        equipment_chart = alt.Chart(data).mark_bar().encode(
            alt.X('equipment_utilization_rate:Q', bin=alt.Bin(maxbins=12), title='Equipment Utilization Rate'),
            alt.Y('count()', title='Number of Records'),
            color=alt.value('#2ca02c')
        ).properties(
            title='Equipment Utilization Distribution',
            width=380,
            height=340
        )
        charts.append(('Equipment Utilization Distribution', equipment_chart))
    
    # Resource Cost Analysis by Type
    if 'resource_cost_per_hour' in data.columns and 'resource_type' in data.columns:
        cost_chart = alt.Chart(data).mark_bar().encode(
            alt.X('resource_type:N', title='Resource Type'),
            alt.Y('mean(resource_cost_per_hour):Q', title='Average Cost per Hour ($)'),
            color=alt.Color('resource_type:N', legend=None),
            tooltip=['resource_type:N', 'mean(resource_cost_per_hour):Q']
        ).properties(
            title='Average Resource Cost by Type',
            width=380,
            height=340
        )
        charts.append(('Resource Cost by Type', cost_chart))
    
    # Critical Path Analysis
    if 'critical_path_flag' in data.columns and 'task_status' in data.columns:
        critical_data = data.groupby(['task_status', 'critical_path_flag']).size().reset_index(name='count')
        
        critical_chart = alt.Chart(critical_data).mark_bar().encode(
            alt.X('task_status:N', title='Task Status'),
            alt.Y('count:Q', title='Number of Tasks'),
            color=alt.Color('critical_path_flag:N', title='Critical Path'),
            tooltip=['task_status:N', 'critical_path_flag:N', 'count:Q']
        ).properties(
            title='Critical Path Tasks by Status',
            width=380,
            height=340
        )
        charts.append(('Critical Path Analysis', critical_chart))
    
    # Risk Score vs Resource Availability
    if 'risk_score' in data.columns and 'resource_availability' in data.columns:
        risk_chart = alt.Chart(data).mark_point(size=60, opacity=0.7).encode(
            alt.X('resource_availability:Q', title='Resource Availability'),
            alt.Y('risk_score:Q', title='Risk Score'),
            color=alt.value('#9467bd'),
            tooltip=['resource_availability:Q', 'risk_score:Q', 'project_id:N']
        ).properties(
            title='Risk Score vs Resource Availability',
            width=380,
            height=340
        )
        charts.append(('Risk vs Resource Availability', risk_chart))
    
    return charts

data = load_data()
if data.empty:
    st.error("No data found.")
    st.stop()

# Identify column types based on actual data
categorical_cols = [col for col in ["project_id", "task_id", "project_name", "task_name", "task_status", "resource_id", "resource_type", "weather_condition", "supplier_id", "material_delivery_status", "equipment_id", "equipment_status"] if col in data.columns]
numeric_cols = [col for col in ["percent_complete", "resource_availability", "resource_cost_per_hour", "temperature_fahrenheit", "precipitation_probability", "wind_speed_mph", "equipment_utilization_rate", "schedule_performance_index", "cost_performance_index", "risk_score"] if col in data.columns]
date_cols = [col for col in ["scheduled_start_date", "scheduled_end_date", "actual_start_date", "actual_end_date", "material_delivery_date", "data_timestamp"] if col in data.columns]

sample_cols = data.columns.tolist()
numeric_candidates = [col for col in sample_cols if data[col].dtype in ['float64', 'int64'] and 'id' not in col.lower()]
date_candidates = [col for col in sample_cols if 'date' in col.lower() or 'timestamp' in col.lower()]
cat_candidates = [col for col in sample_cols if data[col].dtype == 'object' and data[col].nunique() < 1000]

# Calculate key variables that will be used throughout the application
if 'schedule_performance_index' in data.columns:
    forecast_efficiency = (data['schedule_performance_index'].mean() - 1.0) * 100
else:
    forecast_efficiency = 0

# Four tabs - Metrics first, then AI Insights
tabs = st.tabs(["📊 Metrics", "✨ AI Insights", "📁 Insights History", "🔍 Data Explorer"])

# Metrics tab (PRIMARY - position 1)
with tabs[0]:
    st.subheader("📊 Key Construction Performance Metrics")
    
    # Display key metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'schedule_performance_index' in data.columns:
            avg_spi = data['schedule_performance_index'].mean()
            st.metric("Avg Schedule Performance Index", f"{avg_spi:.3f}", delta=f"{(avg_spi - 1.0):.3f} vs target")
    
    with col2:
        if 'cost_performance_index' in data.columns:
            avg_cpi = data['cost_performance_index'].mean()
            st.metric("Avg Cost Performance Index", f"{avg_cpi:.3f}", delta=f"{(avg_cpi - 1.0):.3f} vs target")
    
    with col3:
        if 'equipment_utilization_rate' in data.columns:
            avg_utilization = data['equipment_utilization_rate'].mean()
            st.metric("Avg Equipment Utilization", f"{avg_utilization:.1%}", delta=f"{(avg_utilization - 0.85):.1%} vs target")
    
    with col4:
        if 'critical_path_flag' in data.columns:
            critical_path_rate = data['critical_path_flag'].mean()
            st.metric("Critical Path Coverage", f"{critical_path_rate:.1%}")
    
    st.markdown("---")
    
    # Create and display charts
    charts = create_metrics_charts(data)
    
    # ---- Title clipping fix (Altair) ----
    def fixed_title(text: str) -> alt.TitleParams:
        return alt.TitleParams(
            text=text,
            fontSize=16,
            fontWeight='bold',
            anchor='start',
            offset=14  # key: moves title downward so it won't be cut off
        )
    PAD = {"top": 28, "left": 6, "right": 6, "bottom": 6}  # key: explicit top padding
    charts_fixed = []
    if charts:
        for item in charts:
            # Expected shape: (title_text, chart_object). Fallback if a bare chart arrives.
            try:
                title_text, ch = item
            except Exception:
                title_text, ch = "", item
            ch = ch.properties(title=fixed_title(title_text or ""), padding=PAD)
            ch = ch.configure_title(anchor='start')
            charts_fixed.append((title_text, ch))
    
    if charts_fixed:
        st.subheader("📈 Construction Performance Visualizations")
        # Display in a 2-column grid
        num_charts = len(charts_fixed)
        for i in range(0, num_charts, 2):
            cols = st.columns(2)
            if i < num_charts:
                _, ch = charts_fixed[i]
                with cols[0]:
                    st.altair_chart(ch, use_container_width=True)
            if i + 1 < num_charts:
                _, ch = charts_fixed[i + 1]
                with cols[1]:
                    st.altair_chart(ch, use_container_width=True)
        st.caption(f"Displaying {num_charts} performance charts")
    else:
        st.info("No suitable data found for creating visualizations.")
    
    # Enhanced Summary statistics table
    st.subheader("📈 Construction Summary Statistics")
    if numeric_candidates:
        # Create enhanced summary statistics
        summary_stats = data[numeric_candidates].describe()
        
        # Transpose for better readability and add formatting
        summary_df = summary_stats.T.round(3)
        
        # Add meaningful column names and formatting
        summary_df.columns = ['Count', 'Mean', 'Std Dev', 'Min', '25%', '50% (Median)', '75%', 'Max']
        
        # Create two columns for better organization
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🎯 Key Construction Performance Metrics**")
            key_metrics = ['schedule_performance_index', 'cost_performance_index', 'equipment_utilization_rate', 'percent_complete']
            key_metrics_present = [m for m in key_metrics if m in summary_df.index]
            
            if key_metrics_present:
                for metric in key_metrics_present:
                    mean_val = summary_df.loc[metric, 'Mean']
                    min_val = summary_df.loc[metric, 'Min']
                    max_val = summary_df.loc[metric, 'Max']
                    
                    # Format based on metric type
                    if 'index' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.3f}",
                            help=f"Range: {min_val:.3f} - {max_val:.3f}"
                        )
                    elif 'rate' in metric.lower():
                        # For rates, assume they're already in decimal form (0.0-1.0)
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.1%}",
                            help=f"Range: {min_val:.1%} - {max_val:.1%}"
                        )
                    elif 'percent' in metric.lower():
                        # For percent fields, check if values are > 1 (likely stored as whole numbers)
                        if mean_val > 1:
                            # Values stored as whole numbers (e.g., 85 for 85%)
                            st.metric(
                                label=metric.replace('_', ' ').title(),
                                value=f"{mean_val:.1f}%",
                                help=f"Range: {min_val:.1f}% - {max_val:.1f}%"
                            )
                        else:
                            # Values stored as decimals (e.g., 0.85 for 85%)
                            st.metric(
                                label=metric.replace('_', ' ').title(),
                                value=f"{mean_val:.1%}",
                                help=f"Range: {min_val:.1%} - {max_val:.1%}"
                            )
                    else:
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.2f}",
                            help=f"Range: {min_val:.2f} - {max_val:.2f}"
                        )
        
        with col2:
            st.markdown("**📊 Construction Project Insights**")
            
            # Calculate and display key insights
            insights = []
            
            if 'schedule_performance_index' in summary_df.index:
                spi_mean = summary_df.loc['schedule_performance_index', 'Mean']
                spi_std = summary_df.loc['schedule_performance_index', 'Std Dev']
                insights.append(f"• **Schedule Performance Variability**: {spi_std:.3f} (σ)")
                
                if spi_mean >= 1.0:
                    insights.append(f"• **Ahead of schedule** (SPI: {spi_mean:.3f})")
                elif spi_mean >= 0.9:
                    insights.append(f"• **On track** (SPI: {spi_mean:.3f})")
                else:
                    insights.append(f"• **⚠️ Schedule delays detected** (SPI: {spi_mean:.3f})")
            
            if 'cost_performance_index' in summary_df.index:
                cpi_mean = summary_df.loc['cost_performance_index', 'Mean']
                if cpi_mean >= 1.0:
                    insights.append(f"• **Under budget** (CPI: {cpi_mean:.3f})")
                elif cpi_mean >= 0.9:
                    insights.append(f"• **On budget** (CPI: {cpi_mean:.3f})")
                else:
                    insights.append(f"• **⚠️ Budget overruns** (CPI: {cpi_mean:.3f})")
            
            if 'equipment_utilization_rate' in summary_df.index:
                eq_q75 = summary_df.loc['equipment_utilization_rate', '75%']
                eq_q25 = summary_df.loc['equipment_utilization_rate', '25%']
                eq_iqr = eq_q75 - eq_q25
                insights.append(f"• **Equipment Utilization IQR**: {eq_iqr:.1%}")
            
            if 'risk_score' in summary_df.index:
                risk_median = summary_df.loc['risk_score', '50% (Median)']
                insights.append(f"• **Median Risk Score**: {risk_median:.2f}")
                if risk_median > 7.0:
                    insights.append(f"• **⚠️ High risk projects**: {risk_median:.2f}")
            
            # Add categorical insights
            if 'project_id' in data.columns:
                unique_projects = data['project_id'].nunique()
                insights.append(f"• **Active Projects**: {unique_projects}")
            
            if 'task_status' in data.columns:
                completed_tasks = (data['task_status'] == 'Completed').sum()
                total_tasks = len(data)
                completion_rate = completed_tasks / total_tasks
                insights.append(f"• **Task Completion Rate**: {completion_rate:.1%}")
            
            if 'critical_path_flag' in data.columns:
                critical_tasks = data['critical_path_flag'].sum()
                insights.append(f"• **Critical Path Tasks**: {critical_tasks}")
            
            for insight in insights:
                st.markdown(insight)
        
        # Full detailed table (collapsible)
        with st.expander("📋 Detailed Statistics Table", expanded=False):
            st.dataframe(
                summary_df.style.format({
                    'Count': '{:.0f}',
                    'Mean': '{:.3f}',
                    'Std Dev': '{:.3f}',
                    'Min': '{:.3f}',
                    '25%': '{:.3f}',
                    '50% (Median)': '{:.3f}',
                    '75%': '{:.3f}',
                    'Max': '{:.3f}'
                }),
                use_container_width=True
            )

# AI Insights tab (SECONDARY - position 2)
with tabs[1]:
    st.subheader("✨ AI-Powered Construction Schedule Optimization with Agent Workflows")
    st.markdown("**Experience behind-the-scenes AI agent processing for each construction project management analysis focus area**")
    
    focus_area = st.radio("Focus Area", [
        "Overall Performance", 
        "Optimization Opportunities", 
        "Financial Impact", 
        "Strategic Recommendations"
    ])
    
    # Show business challenge and solution
    focus_info = get_focus_area_info(focus_area)
    if focus_info["challenge"]:
        st.markdown("#### Business Challenge")
        st.info(focus_info["challenge"])
        st.markdown("#### Agent Solution")
        st.success(focus_info["solution"])
    
    st.markdown("**Select Snowflake Cortex Model for Analysis:**")
    selected_model = st.selectbox("", MODELS, index=0, label_visibility="collapsed")

    # Agent control buttons and status
    col1, col2, col3 = st.columns([2, 1, 1])
    
    agent_running_key = f"{focus_area}_agent_running"
    if agent_running_key not in st.session_state:
        st.session_state[agent_running_key] = False
    
    with col1:
        if st.button("🚀 Start Construction Agent"):
            st.session_state[agent_running_key] = True
            st.rerun()
    
    with col2:
        if st.button("⏹ Stop Agent"):
            st.session_state[agent_running_key] = False
            st.rerun()
    
    with col3:
        st.markdown("**Status**")
        if st.session_state[agent_running_key]:
            st.markdown('<div class="agent-status-active">✅ Active</div>', unsafe_allow_html=True)
        else:
            st.markdown("⏸ Ready")

    # Progress placeholder
    progress_placeholder = st.empty()
    
    # Run agent if active
    if st.session_state[agent_running_key]:
        with st.spinner("Construction Agent Running..."):
            insights = generate_insights_with_agent_workflow(data, focus_area, selected_model, progress_placeholder)
            
            if insights:
                # Show completion message
                st.success(f"🎉 {focus_area} Construction Agent completed with real project management data analysis!")
                
                # Show report in expandable section
                with st.expander(f"📋 Generated {focus_area} Report (Real Construction Data)", expanded=True):
                    st.markdown(f"""
                    <div class="agent-report-header">
                        <strong>{focus_area} Report - AI-Generated Construction Schedule Optimization Analysis</strong><br>
                        <small>Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</small><br>
                        <small>Data Source: Live Snowflake Construction Project Management Analysis</small><br>
                        <small>AI Model: {selected_model}</small>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.markdown(insights)
                
                # Save to history
                timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
                st.session_state.insights_history.append({
                    "timestamp": timestamp,
                    "focus": focus_area,
                    "insights": insights,
                    "model": selected_model
                })
                
                # Download button
                st.download_button(
                    "📥 Download Construction Analysis Report", 
                    insights, 
                    file_name=f"{solution_name.replace(' ', '_').lower()}_{focus_area.lower().replace(' ', '_')}_report.md",
                    mime="text/markdown"
                )
                
                # Stop the agent after completion
                st.session_state[agent_running_key] = False

# Insights History tab placeholder - existing code will be inserted here
with tabs[2]:
    st.subheader("📁 Construction Insights History")
    if st.session_state.insights_history:
        for i, item in enumerate(reversed(st.session_state.insights_history)):
            with st.expander(f"{item['timestamp']} - {item['focus']} ({item['model']})", expanded=False):
                st.markdown(item["insights"])
    else:
        st.info("No insights generated yet. Go to the AI Agent Insights tab to generate construction analysis insights.")

# Data Explorer tab placeholder - existing code will be inserted here
with tabs[3]:
    st.subheader("🔍 Construction Data Explorer")
    rows_per_page = st.slider("Rows per page", 5, 50, 10)
    page = st.number_input("Page", min_value=1, value=1)
    start = (page - 1) * rows_per_page
    end = min(start + rows_per_page, len(data))
    st.dataframe(data.iloc[start:end], use_container_width=True)
    st.caption(f"Showing rows {start + 1}–{end} of {len(data)} construction project records")
```

</details>

## 3.2 Create and Deploy the Streamlit in Snowflake Gen AI Data App
1. Switch to **Chrome Tab 4 (Snowflake UI)**
2. Click on **Projects** in the left navigation panel
3. Click on **Streamlit**
4. Click the **+ Streamlit App** blue button in the upper right corner
5. Configure your app:
   - App title: `ProjectFlow AI`
   - Database: Select `SF_LABUSER#_DB` (only option available for your user)
   - Schema: Select `construction_con_connector` the schema created by your Fivetran connector (this should be the only schema available other than Public - do not select Public)
6. In the Streamlit Editor that appears (left side of the Streamlit UI), select all text (Command+A) and delete it
7. Paste the copied Streamlit application code into the empty editor (Command+V):
8. Click the blue **Run** button in the upper right corner
9. Close the editor by clicking the middle icon in the bottom left navigation

## 3.3 Explore the Streamlit in Snowflake Gen AI Data App
The ProjectFlow AI data app should now be running with the following sections:
- **Metrics**: Visual representation of key metrics including schedule performance index, cost performance index, resource utilization rates, and critical path optimization
- **AI Insights**: Generate in-depth analysis of overall performance, optimization opportunities, financial impact, and strategic recommendations
- **Insights History**: Access previously generated AI insights
- **Data Explorer**: Explore the underlying construction project management data with pagination controls.

## Done!
You've successfully:
1. Created a custom Fivetran connector using the Fivetran Connector SDK
2. Deployed the connector to sync construction project scheduling data into Snowflake
3. Built a Streamlit in Snowflake data app to visualize and analyze the data using Snowflake Cortex

## Next Steps
Consider how you might adapt this solution for your own use:
- Integration with other construction systems
- Adding real-time monitoring or projects
- Implementing machine learning models for more sophisticated results
- Customizing the Streamlit app for specific construction scheduling needs

## Resources
- Fivetran Connector SDK Documentation: [https://fivetran.com/docs/connectors/connector-sdk](https://fivetran.com/docs/connectors/connector-sdk)  
- Fivetran Connector SDK Examples: [https://fivetran.com/docs/connector-sdk/examples](https://fivetran.com/docs/connector-sdk/examples)
- API Connector Reference: [https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/con_api_spec](https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/con_api_spec)
- Snowflake Cortex Documentation: [https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- Snowflake Streamlit Documentation: [https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)