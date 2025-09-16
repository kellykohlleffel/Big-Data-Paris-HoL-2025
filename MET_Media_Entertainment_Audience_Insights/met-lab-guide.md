# Fivetran Connector SDK Hands-on Lab at Big Data London 2025: Media & Entertainment Audience Insights

## Overview
In this 20-minute hands-on lab, you'll build a **custom Fivetran connector** using the **Fivetran Connector SDK** and the **Anthropic Workbench** to integrate an industry-specific dataset from a custom REST API into Snowflake. You'll then create a **Streamlit (in Snowflake)** application with various tools and dashboards powering key metrics as well as a **Snowflake Cortex AI-driven** feature to drive even deeper analytics, descriptive, and prescriptive insights.

The Media Entertainment Audience Insights (MET) custom connector should fetch media records from a REST API and load them into a single table called `met_records` in your Snowflake database. The connector should deliver detailed information from social media APIs, customer relationship management systems, and digital analytics platforms. It should handle authentication, pagination, error handling, and maintain state between sync runs using a cursor-based approach.

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
Here is the API spec for this dataset: https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/met_api_spec

Provide a custom Fivetran connector for Media for the met_data endpoint. There is only one dataset called met_records.

Here is a sample record:
{
    "age_range": "35-44",
    "avg_session_duration": 5.36,
    "content_preferences": "[\"Action\", \"Adventure\", \"Thriller\"]",
    "conversion_rate": 9.04,
    "customer_id": "CUST_770487",
    "customer_segment": "New Customer",
    "email_address": "ava.lopez@gmail.com",
    "engagement_trend": "Mobile Dominant",
    "first_name": "Ava",
    "gender": "Prefer not to say",
    "last_name": "Lopez",
    "last_purchase_date": "2023-12-14T00:00:00",
    "last_updated_epoch": 1748797200,
    "lead_score": 83,
    "location_city": "San Jose",
    "location_country": "France",
    "predicted_churn_risk": 0.89,
    "purchase_frequency": 22,
    "recommended_content_type": "Family Movies",
    "record_id": "d19002cc-0879-4e6b-8724-d255852a529e",
    "record_timestamp": "2024-11-30T23:57:00",
    "social_engagement_score": 9.57,
    "social_media_followers": 4379,
    "total_purchase_value": 2202.85,
    "website_sessions": 19
}
```
</details>

3. Click the black **Run** button in the upper right
4. After Claude generates the connector.py code, you will see a response similar to the example connector, but updated for the media entertainment dataset.
5. Click [met_api_spec](https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/met_api_spec) if you'd like to see the API spec.

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

        - Verifies data loading and schema creation by simulating a full sync (in this case, upserting 600 records into met_records).

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
media_met_connector
```

12. Press Enter to deploy your new custom connector to Fivetran.

## Step 2: Start Data Sync in Fivetran (3 minutes)

1. Switch to **Chrome Tab 3 (Fivetran Automated Data Movement)**
2. Refresh the page and find your newly created connection named "media_met_connector" in the connections list
3. Click on the connection to open the **Status** page
4. Click the **Start Initial Sync** button
5. You should see a status message indicating that the sync is **Active** and that it is the first time syncing data for this connection.
6. Once your sync completes, you will see a message "Next sync will run in x hours" and if you click on the **1 HOUR** selection on the right side, you will see some sync metrics.
    * You may need to refresh the UI to see updated sync progress and logs in the UI. 

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
    page_title="audienceinsight_–_ai_driven_audience_profiling",
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

solution_name = '''Solution 3: AudienceInsight – AI-driven Audience Profiling'''
solution_name_clean = '''audienceinsight_–_ai_driven_audience_profiling'''
table_name = '''MET_RECORDS'''
table_description = '''Consolidated audience profiling data combining social media engagement, CRM customer data, and digital analytics for comprehensive audience understanding and targeting'''
solution_content = '''Solution 3: AudienceInsight – AI-driven Audience Profiling

### Business Challenge
The primary business challenge addressed by AudienceInsight is the need for media and entertainment companies to accurately understand their target audience's demographics, preferences, and behaviors. This challenge arises from the complexity of audience segmentation and the difficulty in gathering accurate data.

### Key Features
- Advanced audience profiling through machine learning and data analytics
- Integration with popular data sources (e.g., social media, CRM, and purchase history)
- Customizable dashboards for tracking audience metrics
- Predictive analytics for forecasting audience behavior

### Data Sources
- Social Media APIs: Twitter API, Facebook Graph API, Instagram API
- Customer Relationship Management (CRM) Systems: Salesforce, HubSpot, Zoho
- Purchase History Data: Google Analytics, Adobe Analytics, Mixpanel

### Competitive Advantage
AudienceInsight differentiates itself through its ability to provide detailed and accurate audience profiles, allowing media and entertainment companies to tailor their content and marketing strategies to their target audience.

### Key Stakeholders
- Content Creators
- Marketing Teams
- Sales Teams
- CMO (Chief Marketing Officer)

### Technical Approach
AudienceInsight utilizes a combination of machine learning algorithms and data analytics to create detailed audience profiles. It leverages generative AI to generate insights and recommendations based on the analyzed data.

### Expected Business Results
- **12% increase in sales through targeted marketing**
  **20,000 monthly visitors × 12% baseline conversion rate × 12% increase = 2,400 additional conversions/month**
- **20% improvement in customer satisfaction**
  **10,000 monthly customers × 40% baseline satisfaction rate × 20% improvement = 4,000 additional satisfied customers/month**
- **15% reduction in marketing costs**
  **$ 500,000 annual marketing costs × 15% reduction = $ 75,000 savings/year**
- **18% increase in audience engagement**
  **50,000 monthly viewers × 20% baseline engagement rate × 18% increase = 9,000 additional engagements/month**

### Success Metrics
- Sales conversions
- Customer satisfaction scores
- Marketing costs
- Audience engagement rates

### Risk Assessment
Potential challenges include data quality issues, algorithm bias, and the need for continuous model updates. Mitigation strategies include implementing data validation processes, regularly auditing the model for bias, and establishing a feedback loop for model improvement.

### Long-term Evolution
As generative AI advances, AudienceInsight could evolve to incorporate multimodal analysis (combining text, image, and video data) and predictive analytics to forecast audience behavior and preferences, enabling media and entertainment companies to proactively adjust their content and marketing strategies.'''

# Display logo and title inline
st.markdown(f'''
<div style="display:flex; align-items:center; margin-bottom:15px">
    <img src="https://i.imgur.com/Og6gFnB.png" width="100" style="margin-right:15px">
    <div>
        <h1 style="font-size:2.2rem; margin:0; padding:0">{solution_name_clean.replace('_', ' ').title()}</h1>
        <p style="font-size:1.1rem; color:gray; margin:0; padding:0">Fivetran and Cortex-powered Streamlit in Snowflake data application for Media and Entertainment Audience Analytics</p>
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
            "challenge": "Chief Marketing Officers manually review hundreds of audience engagement metrics, customer segment reports, and content performance data daily, spending 4+ hours analyzing audience behavior patterns, social media engagement effectiveness, and customer journey optimization to identify critical engagement issues and content strategy opportunities.",
            "solution": "Autonomous audience analytics workflow that analyzes customer engagement data, social media metrics, purchase behaviors, and content preferences to generate automated audience summaries, identify engagement bottlenecks, and produce prioritized audience insights with personalized content recommendations."
        },
        "Optimization Opportunities": {
            "challenge": "Marketing Directors spend 5+ hours daily manually identifying inefficiencies in audience targeting strategies, content personalization approaches, and customer engagement campaigns across multiple social media platforms and digital marketing channels.",
            "solution": "AI-powered audience optimization analysis that automatically detects customer engagement gaps, content performance inefficiencies, and targeting improvements with specific implementation recommendations for CRM and marketing automation system integration."
        },
        "Financial Impact": {
            "challenge": "VP of Marketing manually calculate complex ROI metrics across audience engagement activities and customer acquisition performance, requiring 4+ hours of cost modeling to assess marketing efficiency and customer lifetime value optimization across the customer portfolio.",
            "solution": "Automated marketing financial analysis that calculates comprehensive audience engagement ROI, identifies customer acquisition cost reduction opportunities across demographic segments, and projects engagement efficiency benefits with detailed marketing cost forecasting."
        },
        "Strategic Recommendations": {
            "challenge": "Chief Marketing Officers spend hours manually analyzing digital transformation opportunities and developing strategic audience intelligence roadmaps for personalized content advancement and AI-driven engagement implementation across customer touchpoints.",
            "solution": "Strategic audience intelligence workflow that analyzes competitive advantages against traditional demographic-based marketing approaches, identifies AI and personalization integration opportunities, and creates prioritized digital marketing transformation roadmaps."
        }
    }
    
    return focus_info.get(focus_area, {"challenge": "", "solution": ""})

def generate_insights_with_agent_workflow(data, focus_area, model_name, progress_placeholder=None):
    """Generate insights using AI agent workflow - Audience Analytics focused version"""
    
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
        key_metrics = ["social_engagement_score", "conversion_rate", "total_purchase_value", "predicted_churn_risk"]
        available_metrics = [col for col in key_metrics if col in data.columns]
        
        # Calculate enhanced audience analytics data insights
        avg_engagement_score = data['social_engagement_score'].mean() if 'social_engagement_score' in data.columns else 0
        avg_conversion_rate = data['conversion_rate'].mean() if 'conversion_rate' in data.columns else 0
        customer_segments = len(data['customer_segment'].unique()) if 'customer_segment' in data.columns else 0
        active_customers = len(data['customer_id'].unique()) if 'customer_id' in data.columns else 0
        avg_purchase_value = data['total_purchase_value'].mean() if 'total_purchase_value' in data.columns else 0
        avg_churn_risk = data['predicted_churn_risk'].mean() if 'predicted_churn_risk' in data.columns else 0
        
        # Define enhanced agent workflows for each focus area
        if focus_area == "Overall Performance":
            steps = [
                ("Audience Analytics Data Initialization", 15, f"Loading comprehensive audience profiling dataset with enhanced validation across {total_records} customer records and {customer_segments} audience segments", f"Connected to {len(available_metrics)} engagement metrics across {len(data.columns)} total audience data dimensions"),
                ("Customer Engagement Assessment", 35, f"Advanced calculation of audience indicators with engagement analysis (avg engagement score: {avg_engagement_score:.1f})", f"Computed audience metrics: {avg_engagement_score:.1f} engagement score, {avg_conversion_rate:.1f}% conversion rate, ${avg_purchase_value:.0f} avg purchase value"),
                ("Audience Pattern Recognition", 55, f"Sophisticated identification of customer behavior patterns with demographic correlation analysis across {customer_segments} audience segments", f"Detected significant patterns in {len(data['engagement_trend'].unique()) if 'engagement_trend' in data.columns else 'N/A'} engagement trend categories with demographic correlation analysis completed"),
                ("AI Audience Intelligence Processing", 75, f"Processing comprehensive audience data through {model_name} with advanced reasoning for engagement optimization insights", f"Enhanced AI analysis of audience profiling effectiveness across {total_records} customer records completed"),
                ("Audience Performance Report Compilation", 100, f"Professional audience analytics with evidence-based recommendations and actionable content strategy insights ready", f"Comprehensive audience performance report with {len(available_metrics)} engagement metrics analysis and customer retention recommendations generated")
            ]
            
        elif focus_area == "Optimization Opportunities":
            high_engagement_customers = len(data[data['engagement_trend'] == 'Increasing']) if 'engagement_trend' in data.columns else 0
            engagement_success_rate = (high_engagement_customers / total_records) * 100 if total_records > 0 else 0
            
            steps = [
                ("Audience Optimization Data Preparation", 12, f"Advanced loading of customer engagement data with enhanced validation across {total_records} records for targeting improvement identification", f"Prepared {customer_segments} customer segments, social media analytics for optimization with {engagement_success_rate:.1f}% high engagement rate"),
                ("Customer Engagement Inefficiency Detection", 28, f"Sophisticated analysis of targeting strategies and content performance with evidence-based inefficiency identification", f"Identified optimization opportunities across {customer_segments} audience segments with customer engagement and content performance gaps"),
                ("Audience Analytics Correlation Analysis", 45, f"Enhanced examination of relationships between customer segments, demographics, and engagement success rates", f"Analyzed correlations between audience characteristics and customer outcomes across {total_records} customer records"),
                ("CRM Integration Optimization", 65, f"Comprehensive evaluation of marketing operations integration with existing Salesforce, HubSpot, and analytics systems", f"Assessed integration opportunities across {len(data.columns)} data points and marketing automation system optimization needs"),
                ("AI Audience Intelligence", 85, f"Generating advanced audience optimization recommendations using {model_name} with marketing reasoning and implementation strategies", f"AI-powered audience targeting optimization strategy across {customer_segments} customer segments and engagement improvements completed"),
                ("Audience Strategy Finalization", 100, f"Professional audience optimization report with prioritized implementation roadmap and engagement impact analysis ready", f"Comprehensive optimization strategy with {len(available_metrics)} performance improvement areas and marketing operations implementation plan generated")
            ]
            
        elif focus_area == "Financial Impact":
            total_customers = data['customer_id'].nunique() if 'customer_id' in data.columns else 0
            avg_marketing_cost_estimate = 75000 * (avg_churn_risk) if avg_churn_risk > 0 else 0
            
            steps = [
                ("Marketing Financial Data Integration", 15, f"Advanced loading of audience analytics financial data and marketing cost metrics with enhanced validation across {total_records} customer records", f"Integrated marketing financial data: {avg_engagement_score:.1f} avg engagement score, {avg_churn_risk:.3f} churn risk across {active_customers} customers"),
                ("Marketing ROI Cost-Benefit Calculation", 30, f"Sophisticated ROI metrics calculation with customer acquisition analysis and audience engagement efficiency cost savings", f"Computed comprehensive cost analysis: acquisition expenses, churn costs, and ${avg_marketing_cost_estimate:,.0f} estimated marketing optimization potential"),
                ("Customer Lifetime Value Impact Assessment", 50, f"Enhanced analysis of audience engagement revenue impact with customer retention metrics and acquisition cost correlation analysis", f"Assessed marketing implications: {avg_churn_risk:.3f} churn risk with ${avg_purchase_value:.0f} average customer value requiring cost optimization"),
                ("Marketing Resource Efficiency Analysis", 70, f"Comprehensive evaluation of resource allocation efficiency across customer acquisition activities with campaign lifecycle cost optimization", f"Analyzed resource efficiency: {customer_segments} audience segments with customer acquisition cost reduction opportunities identified"),
                ("AI Marketing Financial Modeling", 90, f"Advanced audience analytics financial projections and marketing ROI calculations using {model_name} with comprehensive marketing cost-benefit analysis", f"Enhanced financial impact analysis and forecasting across {len(available_metrics)} marketing cost metrics completed"),
                ("Marketing Economics Report Generation", 100, f"Professional marketing financial impact analysis with detailed audience engagement ROI calculations and customer acquisition cost forecasting ready", f"Comprehensive marketing financial report with ${avg_marketing_cost_estimate:,.0f} cost optimization analysis and customer engagement efficiency strategy generated")
            ]
            
        elif focus_area == "Strategic Recommendations":
            audience_efficiency_score = (avg_engagement_score * 10) if avg_engagement_score > 0 else 0
            
            steps = [
                ("Marketing Technology Assessment", 15, f"Advanced loading of audience analytics digital context with competitive positioning analysis across {total_records} customer records and {customer_segments} audience segments", f"Analyzed marketing technology landscape: {customer_segments} audience segments, comprehensive audience digitization assessment completed"),
                ("Audience Competitive Advantage Analysis", 30, f"Sophisticated evaluation of competitive positioning against traditional demographic marketing with AI-powered audience optimization effectiveness", f"Assessed competitive advantages: {audience_efficiency_score:.1f} audience efficiency, {avg_engagement_score:.1f} engagement vs industry benchmarks"),
                ("Advanced Marketing Technology Integration", 50, f"Enhanced analysis of integration opportunities with personalized content, dynamic pricing, and AI-powered customer segmentation across {len(data.columns)} audience data dimensions", f"Identified strategic technology integration: personalized content delivery, automated segmentation, real-time engagement optimization opportunities"),
                ("Digital Marketing Strategy Development", 70, f"Comprehensive development of prioritized digital transformation roadmap with evidence-based marketing technology adoption strategies", f"Created sequenced implementation plan across {customer_segments} audience segments with advanced marketing technology integration opportunities"),
                ("AI Marketing Strategic Processing", 85, f"Advanced audience analytics strategic recommendations using {model_name} with long-term competitive positioning and marketing technology analysis", f"Enhanced strategic analysis with audience analytics competitive positioning and digital transformation roadmap completed"),
                ("Digital Marketing Report Generation", 100, f"Professional digital marketing transformation roadmap with competitive analysis and audience technology implementation plan ready for CMO executive review", f"Comprehensive strategic report with {customer_segments}-segment implementation plan and audience analytics competitive advantage analysis generated")
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
    # Key metrics from the MET dataset - only numeric columns
    key_metrics = ["social_media_followers", "social_engagement_score", "total_purchase_value", "purchase_frequency", "website_sessions", "avg_session_duration", "conversion_rate", "lead_score", "predicted_churn_risk"]
    
    # Filter to only columns that exist and are actually numeric
    available_metrics = []
    for col in key_metrics:
        if col in data.columns:
            try:
                # Test if the column is actually numeric by trying to calculate mean
                numeric_data = pd.to_numeric(data[col], errors='coerce')
                test_mean = numeric_data.mean()
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
    categorical_options = ["age_range", "gender", "location_city", "location_country", "content_preferences", "customer_segment", "recommended_content_type", "engagement_trend"]
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

    # Define specific instructions for each focus area
    focus_area_instructions = {
        "Overall Performance": """
        For the Overall Performance analysis of AudienceInsight:
        1. Provide a comprehensive analysis of the audience profiling system using social engagement, purchase behavior, and website analytics metrics
        2. Identify significant patterns in customer segments, content preferences, demographics, and engagement trends across media and entertainment operations
        3. Highlight 3-5 key audience metrics that best indicate audience insight effectiveness (engagement scores, conversion rates, purchase values)
        4. Discuss both strengths and areas for improvement in the AI-driven audience profiling process
        5. Include 3-5 actionable insights for improving audience targeting and content strategy based on customer data
        
        Structure your response with these media and entertainment focused sections:
        - Audience Insights (5 specific insights with supporting customer engagement and behavioral data)
        - Audience Performance Trends (3-4 significant trends in engagement scores and customer behavior)
        - Content Strategy Recommendations (3-5 data-backed recommendations for improving audience targeting and content delivery)
        - Implementation Steps (3-5 concrete next steps for marketing teams and content creators)
        """,
        
        "Optimization Opportunities": """
        For the Optimization Opportunities analysis of AudienceInsight:
        1. Focus specifically on areas where audience engagement, content personalization, and customer retention can be improved
        2. Identify inefficiencies in content delivery, audience segmentation, and customer journey optimization across media operations
        3. Analyze correlations between customer segments, content preferences, engagement scores, and purchase behaviors
        4. Prioritize optimization opportunities based on potential impact on audience engagement and revenue generation
        5. Suggest specific technical or process improvements for integration with existing CRM and analytics platforms
        
        Structure your response with these media and entertainment focused sections:
        - Audience Optimization Priorities (3-5 areas with highest engagement and retention improvement potential)
        - Content Impact Analysis (quantified benefits of addressing each opportunity in terms of engagement and conversion metrics)
        - CRM Integration Strategy (specific steps for marketing teams to implement each optimization)
        - Platform Integration Recommendations (specific technical changes needed for seamless integration with Salesforce, HubSpot, and analytics systems)
        - Marketing Operations Risk Assessment (potential challenges for content creators and marketing teams and how to mitigate them)
        """,
        
        "Financial Impact": """
        For the Financial Impact analysis of AudienceInsight:
        1. Focus on cost-benefit analysis and ROI in media and entertainment terms (marketing efficiency vs. audience engagement improvements)
        2. Quantify financial impacts through improved targeting, higher conversion rates, and increased customer lifetime value
        3. Identify cost savings opportunities across different customer segments and content distribution channels
        4. Analyze resource allocation efficiency across different marketing campaigns and content types
        5. Project future financial outcomes based on improved audience insights and expanding to predictive analytics
        
        Structure your response with these media and entertainment focused sections:
        - Marketing Cost Analysis (breakdown of marketing costs and potential savings by customer segment and content type)
        - Revenue Impact (how improved audience insights affect conversion rates and customer lifetime value)
        - Media ROI Calculation (specific calculations showing return on investment in terms of audience engagement improvement)
        - Marketing Cost Reduction Opportunities (specific areas to reduce customer acquisition and content production costs)
        - Revenue Forecasting (projections based on improved audience targeting and engagement metrics)
        """,
        
        "Strategic Recommendations": """
        For the Strategic Recommendations analysis of AudienceInsight:
        1. Focus on long-term strategic implications for digital transformation in media and entertainment marketing
        2. Identify competitive advantages against traditional demographic-based audience targeting approaches
        3. Suggest new directions for AI integration with emerging content technologies (e.g., personalized content, dynamic pricing)
        4. Connect recommendations to broader content strategy goals of improving audience satisfaction and reducing churn
        5. Provide a digital marketing roadmap with prioritized initiatives
        
        Structure your response with these media and entertainment focused sections:
        - Digital Marketing Context (how AudienceInsight fits into broader digital transformation in media and entertainment)
        - Content Competitive Advantage Analysis (how to maximize audience insights advantages compared to traditional marketing)
        - Content Strategy Strategic Priorities (3-5 high-impact strategic initiatives for improving audience engagement)
        - Advanced Content Technology Integration Vision (how to evolve AudienceInsight with personalized content and real-time recommendations over 1-3 years)
        - Marketing Operations Transformation Roadmap (sequenced steps for expanding to predictive analytics and automated content optimization)
        """
    }

    # Get the specific instructions for the selected focus area
    selected_focus_instructions = focus_area_instructions.get(focus_area, "")

    prompt = f'''
    You are an expert data analyst specializing in {focus_area.lower()} analysis for media and entertainment audience profiling operations.

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
    - Frame all insights in the context of media and entertainment audience profiling and marketing operations
    '''

    return call_cortex_model(prompt, model_name)

def create_metrics_charts(data):
    """Create metric visualizations for the media and entertainment audience data"""
    charts = []
    
    # Social Engagement Score Distribution
    if 'social_engagement_score' in data.columns:
        engagement_chart = alt.Chart(data).mark_bar().encode(
            alt.X('social_engagement_score:Q', bin=alt.Bin(maxbins=15), title='Social Engagement Score'),
            alt.Y('count()', title='Number of Customers'),
            color=alt.value('#1f77b4')
        ).properties(
            title='Social Engagement Score Distribution',
            width=380,
            height=340
        )
        charts.append(('Social Engagement Score Distribution', engagement_chart))
    
    # Purchase Value by Customer Segment
    if 'total_purchase_value' in data.columns and 'customer_segment' in data.columns:
        purchase_chart = alt.Chart(data).mark_boxplot().encode(
            alt.X('customer_segment:N', title='Customer Segment'),
            alt.Y('total_purchase_value:Q', title='Total Purchase Value ($)'),
            color=alt.Color('customer_segment:N', legend=None)
        ).properties(
            title='Purchase Value by Customer Segment',
            width=380,
            height=340
        )
        charts.append(('Purchase Value by Customer Segment', purchase_chart))
    
    # Conversion Rate vs Engagement Score
    if 'conversion_rate' in data.columns and 'social_engagement_score' in data.columns:
        conversion_chart = alt.Chart(data).mark_circle(size=60).encode(
            alt.X('social_engagement_score:Q', title='Social Engagement Score'),
            alt.Y('conversion_rate:Q', title='Conversion Rate (%)'),
            alt.Color('predicted_churn_risk:Q', title='Churn Risk', scale=alt.Scale(scheme='redyellowblue')),
            tooltip=['social_engagement_score:Q', 'conversion_rate:Q', 'predicted_churn_risk:Q']
        ).properties(
            title='Conversion Rate vs Social Engagement',
            width=380,
            height=340
        )
        charts.append(('Conversion Rate vs Social Engagement', conversion_chart))
    
    # Customer Segment Distribution
    if 'customer_segment' in data.columns:
        segment_chart = alt.Chart(data).mark_arc().encode(
            theta=alt.Theta('count():Q'),
            color=alt.Color('customer_segment:N', title='Segment'),
            tooltip=['customer_segment:N', 'count():Q']
        ).properties(
            title='Customer Segment Distribution',
            width=380,
            height=340
        )
        charts.append(('Customer Segment Distribution', segment_chart))
    
    # Age Range vs Average Purchase Value
    if 'age_range' in data.columns and 'total_purchase_value' in data.columns:
        age_purchase_chart = alt.Chart(data).mark_bar().encode(
            alt.X('age_range:O', title='Age Range', sort=['18-24', '25-34', '35-44', '45-54', '55-64', '65+']),
            alt.Y('mean(total_purchase_value):Q', title='Average Purchase Value ($)'),
            color=alt.Color('mean(total_purchase_value):Q', title='Avg Purchase', scale=alt.Scale(scheme='blues')),
            tooltip=['age_range:O', alt.Tooltip('mean(total_purchase_value):Q', format='.2f')]
        ).properties(
            title='Average Purchase Value by Age Range',
            width=380,
            height=340
        )
        charts.append(('Average Purchase Value by Age Range', age_purchase_chart))
    
    # Website Sessions vs Purchase Frequency
    if 'website_sessions' in data.columns and 'purchase_frequency' in data.columns:
        sessions_chart = alt.Chart(data).mark_circle(size=60).encode(
            alt.X('website_sessions:Q', title='Website Sessions'),
            alt.Y('purchase_frequency:Q', title='Purchase Frequency'),
            alt.Color('customer_segment:N', title='Segment'),
            tooltip=['website_sessions:Q', 'purchase_frequency:Q', 'customer_segment:N']
        ).properties(
            title='Website Sessions vs Purchase Frequency',
            width=380,
            height=340
        )
        charts.append(('Website Sessions vs Purchase Frequency', sessions_chart))
    
    # Engagement Trend Distribution
    if 'engagement_trend' in data.columns:
        trend_chart = alt.Chart(data).mark_bar().encode(
            alt.X('engagement_trend:N', title='Engagement Trend'),
            alt.Y('count():Q', title='Customer Count'),
            color=alt.Color('engagement_trend:N', legend=None),
            tooltip=['engagement_trend:N', 'count():Q']
        ).properties(
            title='Engagement Trend Distribution',
            width=380,
            height=340
        )
        charts.append(('Engagement Trend Distribution', trend_chart))
    
    # Churn Risk by Gender
    if 'predicted_churn_risk' in data.columns and 'gender' in data.columns:
        churn_gender_chart = alt.Chart(data).mark_boxplot().encode(
            alt.X('gender:N', title='Gender'),
            alt.Y('predicted_churn_risk:Q', title='Predicted Churn Risk'),
            color=alt.Color('gender:N', legend=None)
        ).properties(
            title='Churn Risk Distribution by Gender',
            width=380,
            height=340
        )
        charts.append(('Churn Risk Distribution by Gender', churn_gender_chart))
    
    return charts

# Load data and define variables BEFORE tabs to prevent NameError
data = load_data()
if data.empty:
    st.error("No data found.")
    st.stop()

# Define ALL required variables immediately after data loading
categorical_cols = [col for col in ["age_range", "gender", "location_city", "location_country", "content_preferences", "customer_segment", "recommended_content_type", "engagement_trend"] if col in data.columns]
numeric_cols = [col for col in ["social_media_followers", "social_engagement_score", "total_purchase_value", "purchase_frequency", "website_sessions", "avg_session_duration", "conversion_rate", "lead_score", "predicted_churn_risk"] if col in data.columns]
date_cols = [col for col in ["record_timestamp", "last_purchase_date"] if col in data.columns]

sample_cols = data.columns.tolist()
numeric_candidates = [col for col in sample_cols if data[col].dtype in ['float64', 'int64'] and 'id' not in col.lower()]
date_candidates = [col for col in sample_cols if 'date' in col.lower() or 'timestamp' in col.lower()]
cat_candidates = [col for col in sample_cols if data[col].dtype == 'object' and data[col].nunique() < 1000]

# Calculate audience engagement efficiency for Strategic Recommendations
audience_engagement_efficiency = 0
if 'social_engagement_score' in data.columns and 'predicted_churn_risk' in data.columns:
    avg_engagement = data['social_engagement_score'].mean()
    avg_churn_risk = data['predicted_churn_risk'].mean()
    audience_engagement_efficiency = (avg_engagement * (1 - avg_churn_risk)) * 10

# Calculate customer lifetime value efficiency
customer_lifetime_efficiency = 0
if 'total_purchase_value' in data.columns and 'purchase_frequency' in data.columns:
    avg_purchase_value = data['total_purchase_value'].mean()
    avg_purchase_frequency = data['purchase_frequency'].mean()
    customer_lifetime_efficiency = avg_purchase_value * avg_purchase_frequency

# Calculate content engagement efficiency
content_engagement_efficiency = 0
if 'conversion_rate' in data.columns and 'website_sessions' in data.columns:
    avg_conversion_rate = data['conversion_rate'].mean()
    avg_sessions = data['website_sessions'].mean()
    content_engagement_efficiency = (avg_conversion_rate / 100) * avg_sessions

# Four tabs - Metrics tab first, then AI Insights
tabs = st.tabs(["📊 Metrics", "✨ AI Insights", "📁 Insights History", "🔍 Data Explorer"])

# Metrics tab (PRIMARY position)
with tabs[0]:
    # Metrics tab placeholder - existing code will be inserted here
    st.subheader("📊 Key Performance Metrics")
    
    # Display key metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'social_engagement_score' in data.columns:
            avg_engagement = data['social_engagement_score'].mean()
            st.metric("Avg Engagement Score", f"{avg_engagement:.1f}", delta=f"{(avg_engagement - 5.0):.1f} vs benchmark")
    
    with col2:
        if 'conversion_rate' in data.columns:
            avg_conversion = data['conversion_rate'].mean()
            st.metric("Avg Conversion Rate", f"{avg_conversion:.1f}%", delta=f"{(avg_conversion - 3.0):.1f}% vs target")
    
    with col3:
        if 'predicted_churn_risk' in data.columns:
            avg_churn = data['predicted_churn_risk'].mean()
            st.metric("Avg Churn Risk", f"{avg_churn:.2f}", delta=f"{(0.30 - avg_churn):.2f} vs target")
    
    with col4:
        if 'total_purchase_value' in data.columns:
            avg_purchase = data['total_purchase_value'].mean()
            st.metric("Avg Purchase Value", f"${avg_purchase:.0f}", delta=f"${(avg_purchase - 1000):.0f} vs target")
    
    st.markdown("---")
    
    # Create and display charts
    charts = create_metrics_charts(data)
    
    if charts:
        st.subheader("📈 Audience Analytics Visualizations")
        
        # Display charts in a 2-column grid, ensuring all charts are shown
        num_charts = len(charts)
        for i in range(0, num_charts, 2):
            cols = st.columns(2)
            
            # Left column chart
            if i < num_charts:
                chart_title, chart = charts[i]
                with cols[0]:
                    st.altair_chart(chart, use_container_width=True)
            
            # Right column chart
            if i + 1 < num_charts:
                chart_title, chart = charts[i + 1]
                with cols[1]:
                    st.altair_chart(chart, use_container_width=True)
        
        # Display chart count for reference
        st.caption(f"Displaying {num_charts} audience analytics charts")
    else:
        st.info("No suitable data found for creating visualizations.")
    
    # Enhanced Summary statistics table
    st.subheader("📈 Summary Statistics")
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
            st.markdown("**🎯 Key Audience Metrics**")
            key_metrics = ['social_engagement_score', 'conversion_rate', 'total_purchase_value', 'predicted_churn_risk']
            key_metrics_present = [m for m in key_metrics if m in summary_df.index]
            
            if key_metrics_present:
                for metric in key_metrics_present:
                    mean_val = summary_df.loc[metric, 'Mean']
                    min_val = summary_df.loc[metric, 'Min']
                    max_val = summary_df.loc[metric, 'Max']
                    
                    # Format based on metric type
                    if 'score' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.2f}",
                            help=f"Range: {min_val:.2f} - {max_val:.2f}"
                        )
                    elif 'rate' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.2f}%",
                            help=f"Range: {min_val:.2f}% - {max_val:.2f}%"
                        )
                    elif 'value' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"${mean_val:.0f}",
                            help=f"Range: ${min_val:.0f} - ${max_val:.0f}"
                        )
                    elif 'risk' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.3f}",
                            help=f"Range: {min_val:.3f} - {max_val:.3f}"
                        )
        
        with col2:
            st.markdown("**📊 Audience Insights**")
            
            # Calculate and display key insights
            insights = []
            
            if 'social_engagement_score' in summary_df.index:
                engagement_mean = summary_df.loc['social_engagement_score', 'Mean']
                engagement_std = summary_df.loc['social_engagement_score', 'Std Dev']
                insights.append(f"• **Engagement Variability**: σ = {engagement_std:.2f}")
                
                if engagement_mean > 7:
                    insights.append(f"• **High audience engagement** (avg {engagement_mean:.2f})")
                elif engagement_mean < 4:
                    insights.append(f"• **Low audience engagement** (avg {engagement_mean:.2f})")
                else:
                    insights.append(f"• **Moderate audience engagement** (avg {engagement_mean:.2f})")
            
            if 'conversion_rate' in summary_df.index:
                cr_q75 = summary_df.loc['conversion_rate', '75%']
                cr_q25 = summary_df.loc['conversion_rate', '25%']
                cr_iqr = cr_q75 - cr_q25
                insights.append(f"• **Conversion Rate IQR**: {cr_iqr:.2f}%")
            
            if 'predicted_churn_risk' in summary_df.index:
                churn_median = summary_df.loc['predicted_churn_risk', '50% (Median)']
                churn_max = summary_df.loc['predicted_churn_risk', 'Max']
                insights.append(f"• **Median Churn Risk**: {churn_median:.3f}")
                if churn_max > 0.8:
                    insights.append(f"• **⚠️ High churn risk customers**: up to {churn_max:.3f}")
            
            # Add categorical insights
            if 'customer_segment' in data.columns:
                top_segment = data['customer_segment'].value_counts().index[0]
                segment_count = data['customer_segment'].value_counts().iloc[0]
                insights.append(f"• **Top Customer Segment**: {top_segment} ({segment_count} customers)")
            
            if 'engagement_trend' in data.columns:
                top_trend = data['engagement_trend'].value_counts().index[0]
                trend_count = data['engagement_trend'].value_counts().iloc[0]
                insights.append(f"• **Dominant Engagement**: {top_trend} ({trend_count} users)")
            
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

# AI Insights tab with Agent Workflows (SECONDARY position)
with tabs[1]:
    st.subheader("✨ AI-Powered Insights with Agent Workflows")
    st.markdown("**Experience behind-the-scenes AI agent processing for each audience analytics focus area**")
    
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
        if st.button("🚀 Start Agent"):
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
        with st.spinner("Agent Running..."):
            insights = generate_insights_with_agent_workflow(data, focus_area, selected_model, progress_placeholder)
            
            if insights:
                # Show completion message
                st.success(f"🎉 {focus_area} Agent completed with real audience analytics data analysis!")
                
                # Show report in expandable section
                with st.expander(f"📋 Generated {focus_area} Report (Real Audience Analytics Data)", expanded=True):
                    st.markdown(f"""
                    <div class="agent-report-header">
                        <strong>{focus_area} Report - AI-Generated Audience Analytics</strong><br>
                        <small>Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</small><br>
                        <small>Data Source: Live Snowflake Audience Analytics</small><br>
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
                
                # Stop the agent after completion
                st.session_state[agent_running_key] = False

# Insights History tab placeholder - existing code will be inserted here
with tabs[2]:
    st.subheader("📁 Insights History")
    if st.session_state.insights_history:
        for i, item in enumerate(reversed(st.session_state.insights_history)):
            with st.expander(f"{item['timestamp']} - {item['focus']} ({item['model']})", expanded=False):
                st.markdown(item["insights"])
    else:
        st.info("No insights generated yet. Go to the AI Insights tab to generate some insights.")

# Data Explorer tab placeholder - existing code will be inserted here
with tabs[3]:
    st.subheader("🔍 Data Explorer")
    rows_per_page = st.slider("Rows per page", 5, 50, 10)
    page = st.number_input("Page", min_value=1, value=1)
    start = (page - 1) * rows_per_page
    end = min(start + rows_per_page, len(data))
    st.dataframe(data.iloc[start:end], use_container_width=True)
    st.caption(f"Showing rows {start + 1}–{end} of {len(data)}")
```

</details>

## 3.2 Create and Deploy the Streamlit in Snowflake Gen AI Data App
1. Switch to **Chrome Tab 4 (Snowflake UI)**
2. Click on **Projects** in the left navigation panel
3. Click on **Streamlit**
4. Click the **+ Streamlit App** blue button in the upper right corner
5. Configure your app:
   - App title: `Audience Insight`
   - Database: Select `SF_LABUSER#_DB` (only option available for your user)
   - Schema: Select `media_met_connector` the schema created by your Fivetran connector (this should be the only schema available other than Public - do not select Public)
6. In the Streamlit Editor that appears (left side of the Streamlit UI), select all text (Command+A) and delete it
7. Paste the copied Streamlit application code into the empty editor (Command+V):
8. Click the blue **Run** button in the upper right corner
9. Close the editor by clicking the middle icon in the bottom left navigation

## 3.3 Explore the Streamlit in Snowflake Gen AI Data App
The Audience Insight data app should now be running with the following sections:
- **Metrics**: Key performance indicators, audience analytics, social engagement distribution, purchase value analysis, conversion vs engagement, customer segment distribution, age demographics, and website performance
- **AI Insights**: Generate AI-powered analysis of the media entertainment data across four focus areas (Overall Performance, Optimization Opportunities, Financial Impact, Strategic Recommendations)
- **Insights History**: Access previously generated AI insights for media entertainment analysis
- **Data Explorer**: Browse the underlying audience insight records

## Done!
You've successfully:
1. Created a custom Fivetran connector using the Fivetran Connector SDK
2. Deployed the connector to sync media entertainment audience data into Snowflake
3. Built a Streamlit in Snowflake data app to visualize and analyze the data using Snowflake Cortex

## Next Steps
Consider how you might adapt this solution for your own use:
- Integration with other media entertainment related systems
- Adding real-time monitoring or projects
- Implementing machine learning models for more sophisticated results
- Customizing the Streamlit app for specific construction scheduling needs

## Resources
- Fivetran Connector SDK Documentation: [https://fivetran.com/docs/connectors/connector-sdk](https://fivetran.com/docs/connectors/connector-sdk)  
- Fivetran Connector SDK Examples: [https://fivetran.com/docs/connector-sdk/examples](https://fivetran.com/docs/connector-sdk/examples)
- API Connector Reference: [https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/met_api_spec](https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/met_api_spec)
- Snowflake Cortex Documentation: [https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- Snowflake Streamlit Documentation: [https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
