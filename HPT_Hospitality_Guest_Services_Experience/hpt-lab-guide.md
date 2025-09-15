# Fivetran Connector SDK Hands on Lab at Big Data London 2025: Hospitality Guest Services Experience
## Overview
In this 20-minute hands on lab, you'll build a **custom Fivetran connector** using the **Fivetran Connector SDK** and the **Anthropic Workbench** to integrate an industry specific dataset from a custom REST API into Snowflake. You'll then create a **Streamlit (in Snowflake)** application with various tools and dashboards powering key metrics as well as a **Snowflake Cortex AI-driven** feature to drive even deeper analytics, descriptive, and prescriptive insights.

The Hospitality Guest Services Experience (HPT) custom connector should guest services experience records from a REST API and load them into a single table called `hpt_records` in your Snowflake database. The connector should deliver detailed information that provides a comprehensive view of guest preferences, local venue quality, real-time availability, and booking information for personalized concierge recommendation analytics. It should handle authentication, pagination, error handling, and maintain state between sync runs using a cursor-based approach.

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
Here is the API spec for this dataset: https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/hpt_api_spec

Provide a custom Fivetran connector for Hospitality Guest Service Experience for the hpt_data endpoint. There is only one dataset called hpt_records.

Here is a sample record:
{
    "activity_preferences": "Shopping",
    "event_availability_status": "Available",
    "event_category": "Sports",
    "event_id": "EVT_00000",
    "event_name": "Food Truck Festival",
    "event_start_datetime": "2024-09-19T15:45:00",
    "guest_id": "GUEST_00000",
    "guest_preference_score": 3.85,
    "guest_sentiment_rating": 8.18,
    "last_updated_epoch": 1748797200,
    "preferred_cuisine_types": "Mexican",
    "recommendation_timestamp": "2024-10-07T11:50:00",
    "record_id": "2041bd63-f4d5-4e2d-a984-a58550f6dff8",
    "temperature_fahrenheit": 84.4,
    "transportation_cost_estimate": 5.17,
    "transportation_eta_minutes": 10,
    "venue_id": "VEN_00000",
    "venue_latitude": 40.749234,
    "venue_longitude": -73.953105,
    "venue_name": "The Rooftop Bistro",
    "venue_price_level": 1,
    "venue_rating": 2.63,
    "weather_condition": "Heavy Rain"
}
```
</details>

3. Click the black **Run** button in the upper right
4. After Claude generates the connector.py code, you will see a response similar to the example connector, but updated for the hospitality dataset.
5. Click [hpt_api_spec](https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/hpt_api_spec) if you'd like to see the API spec.

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

        - Verifies data loading and schema creation by simulating a full sync (in this case, upserting 600 records into hpt_records).

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
hospitality_hpt_connector
```

12. Press Enter to deploy your new custom connector to Fivetran.

## Step 2: Start Data Sync in Fivetran (3 minutes)

1. Switch to **Chrome Tab 3 (Fivetran Automated Data Movement)**
2. Refresh the page and find your newly created connection named "hospitality_hpt_connector" in the connections list
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
    page_title="locallink_–_intelligent_guest_services_and_local_experience_curator",
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

solution_name = '''Solution 5: LocalLink – Intelligent Guest Services and Local Experience Curator'''
solution_name_clean = '''locallink_–_intelligent_guest_services_and_local_experience_curator'''
table_name = '''HPT_RECORDS'''
table_description = '''Consolidated table containing guest preferences, local venue data, real-time availability, and booking information for AI-powered concierge recommendations'''
solution_content = '''Solution 5: LocalLink – Intelligent Guest Services and Local Experience Curator**

**Business Challenge:** Hotels struggle to provide comprehensive local expertise and personalized recommendations at scale, leading to generic tourist suggestions that fail to create memorable experiences and missed opportunities for guest satisfaction and loyalty.

**Key Features:**
• Personalized local attraction and dining recommendations
• Real-time event and activity availability integration
• Multi-language conversational AI for guest inquiries
• Dynamic itinerary creation based on guest preferences
• Local vendor partnership optimization and booking integration

**Data Sources:**
• Guest Preference Systems: Revinate, TrustYou, Medallia
• Local Event APIs: Eventbrite, Facebook Events, Meetup API
• Restaurant and Attraction Data: Yelp Fusion API, Google Places API, TripAdvisor Content API
• Transportation Services: Uber API, Lyft API, local transit feeds
• Weather Services: OpenWeatherMap, AccuWeather, Weather Underground

**Competitive Advantage:** Unlike static concierge services or generic recommendation engines, this solution uses generative AI to create unique, contextual local experiences by synthesizing real-time availability, guest preferences, weather conditions, and local insights to craft personalized itineraries.

**Key Stakeholders:** Guest Services Managers, Concierge Staff, Guest Relations Directors, General Managers, Marketing Directors. **Top C-Level Executive:** Chief Experience Officer (CXO) or Chief Marketing Officer (CMO)

**Technical Approach:** Natural Language Processing models understand guest inquiries and preferences while generative AI creates personalized recommendations and detailed itineraries. Large Language Models generate conversational responses that feel natural and helpful while incorporating real-time local data.

**Expected Business Results:**

• $ 315,000 in additional commission revenue annually
**$ 2,100,000 annual guest spending × 15% increase = $ 315,000 additional revenue/year**

• 1,095 additional positive guest reviews annually
**7,300 annual guests × 15% baseline review rate × 10% improvement = 1,095 additional reviews/year**

• 2,190 fewer concierge service requests annually
**21,900 annual requests × 10% reduction through self-service = 2,190 fewer requests/year**

• 438 additional repeat bookings annually
**4,380 annual guests × 10% baseline repeat rate × 10% improvement = 438 additional bookings/year**

**Success Metrics:**
• Guest engagement with recommendations
• Local experience booking conversion rates
• Guest satisfaction scores for concierge services
• Revenue from local partnership commissions

**Risk Assessment:** Local vendor relationship management requires dedicated partnership coordination. Data accuracy maintained through continuous validation and updates. Guest privacy protected through secure data handling protocols.

**Long-term Evolution:** Integration with augmented reality for location-based experiences and expanded multi-language support for global hospitality operations.'''

# Display logo and title inline with hospitality industry specifics
st.markdown(f'''
<div style="display:flex; align-items:center; margin-bottom:15px">
    <img src="https://i.imgur.com/Og6gFnB.png" width="100" style="margin-right:15px">
    <div>
        <h1 style="font-size:2.2rem; margin:0; padding:0">{solution_name_clean.replace('_', ' ').title()}</h1>
        <p style="font-size:1.1rem; color:gray; margin:0; padding:0">Fivetran and Cortex-powered Streamlit in Snowflake data application for Hospitality Guest Experience</p>
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
            "challenge": "Guest Services Managers and Chief Experience Officers manually review hundreds of guest preference data points, local venue availability, and weather conditions daily, spending 4+ hours analyzing guest satisfaction patterns, venue recommendation trends, and local experience correlations to identify guest service optimization opportunities and concierge effectiveness.",
            "solution": "Autonomous guest experience workflow that analyzes guest preferences, venue ratings, local event data, and weather patterns to generate automated concierge summaries, identify guest satisfaction risks, and produce prioritized local experience insights with personalized recommendation strategies."
        },
        "Optimization Opportunities": {
            "challenge": "Concierge Staff and Guest Relations Directors spend 5+ hours daily manually identifying inefficiencies in local recommendations, venue partnerships, and guest activity matching across multiple locations and weather conditions.",
            "solution": "AI-powered guest experience optimization analysis that automatically detects concierge service gaps, local partnership opportunities, and guest satisfaction improvements with specific implementation recommendations for Revinate, TrustYou, and Medallia integration."
        },
        "Financial Impact": {
            "challenge": "Chief Marketing Officers manually calculate complex ROI metrics across local partnership commissions and guest experience activities, requiring 4+ hours of financial modeling to assess operational costs and guest satisfaction effectiveness across the hospitality portfolio.",
            "solution": "Automated guest experience financial analysis that calculates comprehensive concierge service ROI, identifies revenue opportunities across local partnerships, and projects guest satisfaction benefits with detailed commission forecasting."
        },
        "Strategic Recommendations": {
            "challenge": "Chief Experience Officers spend hours manually analyzing digital transformation opportunities and developing strategic guest experience roadmaps for local partnership integration and AI-powered concierge implementation across hospitality operations.",
            "solution": "Strategic guest experience intelligence workflow that analyzes competitive advantages against traditional concierge services, identifies AI and personalization integration opportunities, and creates prioritized digital hospitality transformation roadmaps."
        }
    }
    
    return focus_info.get(focus_area, {"challenge": "", "solution": ""})

def generate_insights_with_agent_workflow(data, focus_area, model_name, progress_placeholder=None):
    """Generate insights using AI agent workflow - Hospitality focused version"""
    
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
        key_metrics = ["guest_preference_score", "guest_sentiment_rating", "venue_rating", "venue_price_level"]
        available_metrics = [col for col in key_metrics if col in data.columns]
        
        # Calculate enhanced hospitality data insights
        avg_guest_satisfaction = data['guest_sentiment_rating'].mean() if 'guest_sentiment_rating' in data.columns else 0
        avg_venue_rating = data['venue_rating'].mean() if 'venue_rating' in data.columns else 0
        unique_guests = len(data['guest_id'].unique()) if 'guest_id' in data.columns else 0
        unique_venues = len(data['venue_id'].unique()) if 'venue_id' in data.columns else 0
        avg_preference_score = data['guest_preference_score'].mean() if 'guest_preference_score' in data.columns else 0
        total_events = len(data['event_id'].unique()) if 'event_id' in data.columns else 0
        
        # Define enhanced agent workflows for each focus area
        if focus_area == "Overall Performance":
            steps = [
                ("Guest Experience Data Initialization", 15, f"Loading comprehensive guest experience dataset with enhanced validation across {total_records} guest interactions and {unique_guests} active guests", f"Connected to {len(available_metrics)} hospitality metrics across {len(data.columns)} total guest experience dimensions"),
                ("Guest Satisfaction Assessment", 35, f"Advanced calculation of guest experience indicators with satisfaction analysis (avg: {avg_guest_satisfaction:.1f}/10)", f"Computed hospitality metrics: {avg_guest_satisfaction:.1f}/10 avg satisfaction, {avg_venue_rating:.1f}★ venue rating, {avg_preference_score:.1f} preference score"),
                ("Local Experience Pattern Recognition", 55, f"Sophisticated identification of guest patterns with preference correlation analysis across {unique_venues} local venues", f"Detected significant patterns in {len(data['weather_condition'].unique()) if 'weather_condition' in data.columns else 'N/A'} weather conditions with guest experience correlation analysis completed"),
                ("AI Guest Intelligence Processing", 75, f"Processing comprehensive guest data through {model_name} with advanced reasoning for hospitality optimization insights", f"Enhanced AI analysis of guest experience effectiveness across {total_records} guest interactions completed"),
                ("Guest Experience Report Compilation", 100, f"Professional hospitality analysis with evidence-based recommendations and actionable guest service insights ready", f"Comprehensive guest performance report with {len(available_metrics)} hospitality metrics analysis and local experience optimization recommendations generated")
            ]
            
        elif focus_area == "Optimization Opportunities":
            guest_satisfaction_rate = (avg_guest_satisfaction / 10) * 100 if avg_guest_satisfaction > 0 else 0
            venue_quality_score = (avg_venue_rating / 5) * 100 if avg_venue_rating > 0 else 0
            
            steps = [
                ("Guest Optimization Data Preparation", 12, f"Advanced loading of guest experience data with enhanced validation across {total_records} records for hospitality efficiency identification", f"Prepared {unique_guests} active guests, {unique_venues} venues for optimization analysis with {total_events} local events"),
                ("Concierge Service Inefficiency Detection", 28, f"Sophisticated analysis of guest experience strategies and concierge performance with evidence-based inefficiency identification", f"Identified optimization opportunities across {unique_venues} local venues with guest service and experience management gaps"),
                ("Guest Preference Correlation Analysis", 45, f"Enhanced examination of relationships between weather conditions, venue types, and guest satisfaction rates", f"Analyzed correlations between guest characteristics and experience outcomes across {total_records} guest interactions"),
                ("Guest Systems Integration Optimization", 65, f"Comprehensive evaluation of guest experience integration with existing Revinate, TrustYou, and Medallia systems", f"Assessed integration opportunities across {len(data.columns)} data points and guest experience system optimization needs"),
                ("AI Guest Intelligence", 85, f"Generating advanced guest optimization recommendations using {model_name} with hospitality reasoning and implementation strategies", f"AI-powered guest experience optimization strategy across {unique_venues} venues and efficiency improvements completed"),
                ("Hospitality Strategy Finalization", 100, f"Professional guest optimization report with prioritized implementation roadmap and hospitality impact analysis ready", f"Comprehensive optimization strategy with {len(available_metrics)} efficiency improvement areas and guest experience implementation plan generated")
            ]
            
        elif focus_area == "Financial Impact":
            total_guest_value = avg_preference_score * unique_guests * 50 if avg_preference_score > 0 and unique_guests > 0 else 0  # Assuming $50/guest interaction value
            commission_revenue = total_guest_value * 0.15 if total_guest_value > 0 else 0  # 15% commission potential
            
            steps = [
                ("Guest Financial Data Integration", 15, f"Advanced loading of guest financial data and hospitality revenue metrics with enhanced validation across {total_records} guest interactions", f"Integrated guest financial data: {avg_guest_satisfaction:.1f}/10 avg satisfaction, {avg_venue_rating:.1f}★ avg venue rating across {unique_guests} guests"),
                ("Guest Revenue Calculation", 30, f"Sophisticated ROI metrics calculation with guest analysis and hospitality efficiency commission savings", f"Computed comprehensive revenue analysis: guest expenses, venue commissions, and ${commission_revenue:,.0f} estimated guest optimization potential"),
                ("Local Partnership Impact Assessment", 50, f"Enhanced analysis of guest revenue impact with hospitality metrics and venue correlation analysis", f"Assessed hospitality implications: {total_events} total events with {unique_venues} venues requiring optimization"),
                ("Guest Operations Efficiency Analysis", 70, f"Comprehensive evaluation of operational cost efficiency across guest activities with hospitality lifecycle cost optimization", f"Analyzed operational efficiency: {unique_venues} local venues with guest revenue enhancement opportunities identified"),
                ("AI Guest Financial Modeling", 90, f"Advanced guest experience financial projections and ROI calculations using {model_name} with comprehensive hospitality cost-benefit analysis", f"Enhanced financial impact analysis and forecasting across {len(available_metrics)} hospitality revenue metrics completed"),
                ("Hospitality Economics Report Generation", 100, f"Professional guest financial impact analysis with detailed hospitality ROI calculations and guest revenue forecasting ready", f"Comprehensive hospitality financial report with ${commission_revenue:,.0f} revenue optimization analysis and guest satisfaction strategy generated")
            ]
            
        elif focus_area == "Strategic Recommendations":
            # Calculate guest experience efficiency score for Strategic Recommendations
            guest_experience_efficiency = (avg_guest_satisfaction * avg_venue_rating * 2) if avg_guest_satisfaction > 0 and avg_venue_rating > 0 else 0
            
            steps = [
                ("Hospitality Technology Assessment", 15, f"Advanced loading of guest experience digital context with competitive positioning analysis across {total_records} guest interactions and {unique_guests} active guests", f"Analyzed hospitality technology landscape: {unique_venues} venues, {unique_guests} guests, comprehensive guest experience digitization assessment completed"),
                ("Guest Competitive Advantage Analysis", 30, f"Sophisticated evaluation of competitive positioning against traditional concierge services with AI-powered guest optimization effectiveness", f"Assessed competitive advantages: {guest_experience_efficiency:.1f} guest experience efficiency, {avg_guest_satisfaction:.1f}/10 satisfaction vs industry benchmarks"),
                ("Advanced Hospitality Technology Integration", 50, f"Enhanced analysis of integration opportunities with local event data, real-time venue availability, and AI-powered guest preference sensing across {len(data.columns)} hospitality data dimensions", f"Identified strategic technology integration: real-time guest sensing, adaptive recommendation algorithms, automated concierge optimization opportunities"),
                ("Digital Guest Strategy Development", 70, f"Comprehensive development of prioritized digital transformation roadmap with evidence-based hospitality technology adoption strategies", f"Created sequenced implementation plan across {unique_venues} venues with advanced guest experience technology integration opportunities"),
                ("AI Guest Strategic Processing", 85, f"Advanced guest experience strategic recommendations using {model_name} with long-term competitive positioning and hospitality technology analysis", f"Enhanced strategic analysis with hospitality competitive positioning and digital transformation roadmap completed"),
                ("Digital Hospitality Report Generation", 100, f"Professional digital hospitality transformation roadmap with competitive analysis and guest technology implementation plan ready for CXO executive review", f"Comprehensive strategic report with {unique_guests}-guest implementation plan and hospitality competitive advantage analysis generated")
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
    # Define key hospitality concierge metrics that should be numeric
    key_metrics = ["guest_preference_score", "guest_sentiment_rating", "venue_rating", "venue_price_level", 
                   "venue_latitude", "venue_longitude", "transportation_eta_minutes", "transportation_cost_estimate", 
                   "temperature_fahrenheit"]
    
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
    categorical_options = ["guest_id", "preferred_cuisine_types", "activity_preferences", "event_id", "event_name", 
                          "event_category", "event_availability_status", "venue_id", "venue_name", "weather_condition"]
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
                correlation_info = "Top correlations between guest experience metrics:\n"
                for col1, col2, _, corr_value in corr_pairs[:3]:
                    correlation_info += f"- {col1} and {col2}: r = {corr_value:.2f}\n"
        except Exception as e:
            correlation_info = "Could not calculate correlations between guest experience metrics.\n"

    # Define specific instructions for each focus area tailored to hospitality concierge services
    focus_area_instructions = {
        "Overall Performance": """
        For the Overall Performance analysis of LocalLink in Hospitality Guest Services:
        1. Provide a comprehensive analysis of the guest concierge system using guest preferences, venue recommendations, event bookings, and local experience data
        2. Identify significant patterns in guest satisfaction ratings, venue preferences, activity choices, and booking patterns across different guest segments and preferences
        3. Highlight 3-5 key guest experience metrics that best indicate concierge service performance (guest satisfaction scores, venue rating quality, transportation efficiency, weather-activity correlations)
        4. Discuss both strengths and areas for improvement in the AI-powered guest recommendation process
        5. Include 3-5 actionable insights for improving guest experience and local recommendation accuracy based on preference and venue data
        
        Structure your response with these hospitality concierge focused sections:
        - Guest Experience Performance Insights (5 specific insights with supporting guest satisfaction and venue recommendation data)
        - Local Recommendation Trends (3-4 significant trends in venue preferences, activity bookings, and guest behavior patterns)
        - Concierge Service Recommendations (3-5 data-backed recommendations for improving guest services and local experience curation)
        - Implementation Steps (3-5 concrete next steps for guest services managers and concierge staff)
        """,
        
        "Optimization Opportunities": """
        For the Optimization Opportunities analysis of LocalLink in Hospitality Guest Services:
        1. Focus specifically on areas where guest recommendation accuracy, local experience curation, and concierge service efficiency can be improved
        2. Identify inefficiencies in venue recommendations, activity matching, transportation arrangements, and weather-based suggestions
        3. Analyze correlations between guest preferences, venue ratings, activity types, weather conditions, and guest satisfaction scores
        4. Prioritize optimization opportunities based on potential impact on guest satisfaction and revenue from local partnerships
        5. Suggest specific technical or process improvements for integration with existing guest preference systems (Revinate, TrustYou, Medallia)
        
        Structure your response with these hospitality concierge focused sections:
        - Guest Experience Optimization Priorities (3-5 areas with highest guest satisfaction and revenue improvement potential)
        - Local Partnership Impact Analysis (quantified benefits of addressing each opportunity in terms of commission revenue and guest engagement)
        - Concierge Integration Strategy (specific steps for guest services teams to implement each optimization)
        - System Enhancement Recommendations (specific technical changes needed for seamless integration with existing hospitality management systems)
        - Guest Services Risk Assessment (potential challenges for concierge staff and guest relations teams and how to mitigate them)
        """,
        
        "Financial Impact": """
        For the Financial Impact analysis of LocalLink in Hospitality Guest Services:
        1. Focus on cost-benefit analysis and ROI in guest experience terms (operational costs vs. guest satisfaction and revenue benefits)
        2. Quantify financial impacts through commission revenue from local partnerships, reduced concierge staff workload, and improved guest retention
        3. Identify revenue opportunities across different guest segments, venue partnerships, and seasonal activity patterns
        4. Analyze guest spending patterns and booking conversion rates across different recommendation types and weather conditions
        5. Project future financial outcomes based on improved guest satisfaction scores and increased local experience bookings
        
        Structure your response with these hospitality concierge focused sections:
        - Guest Revenue Analysis (breakdown of commission revenue and guest spending by venue type, activity category, and guest segment)
        - Concierge ROI Impact (how improved recommendations affect guest satisfaction scores and repeat bookings)
        - Hospitality Operations ROI Calculation (specific calculations showing return on investment in terms of reduced staff workload and increased revenue)
        - Revenue Optimization Opportunities (specific areas to increase commission income and improve guest experience monetization)
        - Financial Forecasting (projections based on improved guest satisfaction and local partnership performance metrics)
        """,
        
        "Strategic Recommendations": """
        For the Strategic Recommendations analysis of LocalLink in Hospitality Guest Services:
        1. Focus on long-term strategic implications for digital transformation in hospitality guest experience and local partnership management
        2. Identify competitive advantages against traditional concierge services and generic recommendation platforms
        3. Suggest new directions for AI integration with emerging hospitality technologies (e.g., mobile apps, AR/VR experiences, IoT integration, predictive guest preferences)
        4. Connect recommendations to broader business goals of improving guest loyalty and establishing local market leadership
        5. Provide a digital hospitality transformation roadmap with prioritized initiatives
        
        Structure your response with these hospitality concierge focused sections:
        - Guest Experience Digital Context (how LocalLink fits into broader digital transformation in hospitality operations)
        - Competitive Advantage Analysis (how to maximize guest experience advantages compared to traditional concierge approaches)
        - Hospitality Technology Strategic Priorities (3-5 high-impact strategic initiatives for improving guest services operations)
        - Advanced Guest Analytics Integration Vision (how to evolve LocalLink with AI and real-time local data over 1-3 years)
        - Hospitality Transformation Roadmap (sequenced steps for expanding to predictive guest preferences and autonomous local experience curation systems)
        """
    }

    # Get the specific instructions for the selected focus area
    selected_focus_instructions = focus_area_instructions.get(focus_area, "")

    prompt = f'''
    You are an expert data analyst specializing in {focus_area.lower()} analysis for hospitality guest services and local experience curation.

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
    - Frame all insights in the context of hospitality guest services and local experience curation
    '''

    return call_cortex_model(prompt, model_name)

def create_metrics_charts(data):
    """Create metric visualizations for the hospitality concierge data"""
    charts = []
    
    # Guest Satisfaction Distribution
    if 'guest_sentiment_rating' in data.columns:
        satisfaction_chart = alt.Chart(data).mark_bar().encode(
            alt.X('guest_sentiment_rating:Q', bin=alt.Bin(maxbins=15), title='Guest Sentiment Rating'),
            alt.Y('count()', title='Number of Guests'),
            color=alt.value('#2E86C1')
        ).properties(
            title='Guest Satisfaction Distribution',
            width=380,
            height=340
        )
        charts.append(('Guest Satisfaction Distribution', satisfaction_chart))
    
    # Venue Ratings by Price Level
    if 'venue_rating' in data.columns and 'venue_price_level' in data.columns:
        venue_rating_chart = alt.Chart(data).mark_boxplot().encode(
            alt.X('venue_price_level:O', title='Venue Price Level', axis=alt.Axis(labelAngle=0)),
            alt.Y('venue_rating:Q', title='Venue Rating'),
            color=alt.Color('venue_price_level:O', scale=alt.Scale(scheme='viridis'), legend=None)
        ).properties(
            title='Venue Ratings by Price Level',
            width=380,
            height=340
        )
        charts.append(('Venue Ratings by Price Level', venue_rating_chart))
    
    # Activity Preferences Distribution
    if 'activity_preferences' in data.columns:
        activity_chart = alt.Chart(data).mark_bar().encode(
            alt.X('activity_preferences:N', title='Activity Type'),
            alt.Y('count()', title='Number of Recommendations'),
            color=alt.Color('activity_preferences:N', scale=alt.Scale(scheme='category10'), legend=None),
            tooltip=['activity_preferences:N', 'count()']
        ).properties(
            title='Popular Activity Preferences',
            width=380,
            height=340
        )
        charts.append(('Popular Activities', activity_chart))
    
    # Cuisine Preferences Distribution
    if 'preferred_cuisine_types' in data.columns:
        cuisine_chart = alt.Chart(data).mark_bar().encode(
            alt.X('preferred_cuisine_types:N', title='Cuisine Type'),
            alt.Y('count()', title='Number of Preferences'),
            color=alt.Color('preferred_cuisine_types:N', scale=alt.Scale(scheme='set2'), legend=None),
            tooltip=['preferred_cuisine_types:N', 'count()']
        ).properties(
            title='Preferred Cuisine Types',
            width=380,
            height=340
        )
        charts.append(('Cuisine Preferences', cuisine_chart))
    
    # Weather Impact on Recommendations
    if 'weather_condition' in data.columns and 'guest_preference_score' in data.columns:
        weather_chart = alt.Chart(data).mark_bar().encode(
            alt.X('weather_condition:N', title='Weather Condition'),
            alt.Y('mean(guest_preference_score):Q', title='Average Guest Preference Score'),
            color=alt.Color('weather_condition:N', scale=alt.Scale(scheme='blues'), legend=None),
            tooltip=['weather_condition:N', 'mean(guest_preference_score):Q']
        ).properties(
            title='Weather Impact on Guest Preferences',
            width=380,
            height=340
        )
        charts.append(('Weather Impact', weather_chart))
    
    # Transportation Efficiency Analysis
    if 'transportation_eta_minutes' in data.columns and 'transportation_cost_estimate' in data.columns:
        transport_chart = alt.Chart(data).mark_point(size=60, opacity=0.7).encode(
            alt.X('transportation_eta_minutes:Q', title='ETA (Minutes)'),
            alt.Y('transportation_cost_estimate:Q', title='Cost Estimate ($)'),
            color=alt.Color('activity_preferences:N', title='Activity Type'),
            tooltip=['transportation_eta_minutes:Q', 'transportation_cost_estimate:Q', 'activity_preferences:N', 'venue_name:N']
        ).properties(
            title='Transportation Efficiency Analysis',
            width=380,
            height=340
        )
        charts.append(('Transportation Efficiency', transport_chart))
    
    # Event Availability Status - Modern Radial Progress Chart
    if 'event_availability_status' in data.columns:
        # Calculate counts and percentages
        availability_counts = data['event_availability_status'].value_counts()
        total_events = len(data)
        
        # Create data for radial chart
        status_data = []
        colors = {'Available': '#10B981', 'Sold Out': '#EF4444', 'Waitlist': '#F59E0B', 'Postponed': '#8B5CF6'}
        start_angle = 0
        
        for status in ['Available', 'Sold Out', 'Waitlist', 'Postponed']:
            if status in availability_counts.index:
                count = availability_counts[status]
                percentage = (count / total_events) * 100
                end_angle = start_angle + (count / total_events) * 360
                
                status_data.append({
                    'status': status,
                    'count': count,
                    'percentage': percentage,
                    'start_angle': start_angle,
                    'end_angle': end_angle,
                    'color': colors.get(status, '#6B7280')
                })
                start_angle = end_angle
        
        # Convert to DataFrame
        status_df = pd.DataFrame(status_data)
        
        if not status_df.empty:
            # Create the radial progress chart
            radial_chart = alt.Chart(status_df).mark_arc(
                innerRadius=70,
                outerRadius=110,
                stroke='white',
                strokeWidth=3,
                cornerRadius=5
            ).encode(
                theta=alt.Theta('count:Q', scale=alt.Scale(type='linear')),
                color=alt.Color(
                    'status:N',
                    scale=alt.Scale(
                        domain=list(colors.keys()),
                        range=list(colors.values())
                    ),
                    legend=alt.Legend(
                        orient='right',
                        titleFontSize=12,
                        labelFontSize=11,
                        symbolSize=100,
                        symbolType='circle'
                    )
                ),
                tooltip=[
                    alt.Tooltip('status:N', title='Status'),
                    alt.Tooltip('count:Q', title='Events'),
                    alt.Tooltip('percentage:Q', title='Percentage', format='.1f')
                ],
                order=alt.Order('count:Q', sort='descending')
            ).properties(
                title=alt.TitleParams(
                    text='Event Availability Status',
                    fontSize=16,
                    fontWeight='bold',
                    anchor='start'
                ),
                width=300,
                height=300
            )
            
            # Add center text with key metrics
            available_count = availability_counts.get('Available', 0)
            available_pct = (available_count / total_events * 100) if total_events > 0 else 0
            
            center_text = alt.Chart(pd.DataFrame([{
                'x': 0, 'y': 10, 
                'text': f'{available_count}',
                'subtext': f'{available_pct:.1f}% Available'
            }])).mark_text(
                align='center',
                baseline='middle',
                fontSize=24,
                fontWeight='bold',
                color='#10B981'
            ).encode(
                x=alt.value(150),  # Center of 300px width
                y=alt.value(140),   # Center of 300px height  
                text='text:N'
            )
            
            center_subtext = alt.Chart(pd.DataFrame([{
                'x': 0, 'y': -10,
                'text': f'{available_pct:.1f}% Available'
            }])).mark_text(
                align='center',
                baseline='middle',
                fontSize=12,
                color='#6B7280'
            ).encode(
                x=alt.value(150),
                y=alt.value(165),
                text='text:N'
            )
            
            # Combine all elements
            combined_chart = radial_chart + center_text + center_subtext
            
            charts.append(('Event Availability', combined_chart))
        else:
            # Fallback simple chart if data processing fails
            simple_chart = alt.Chart(data).mark_bar().encode(
                x=alt.X('count()', title='Count'),
                y=alt.Y('event_availability_status:N', title='Status'),
                color=alt.Color('event_availability_status:N', legend=None)
            ).properties(
                title='Event Availability Status',
                width=380,
                height=200
            )
            charts.append(('Event Availability', simple_chart))
    
    # Guest Preference vs Venue Rating Correlation
    if 'guest_preference_score' in data.columns and 'venue_rating' in data.columns:
        correlation_chart = alt.Chart(data).mark_point(size=60, opacity=0.7).encode(
            alt.X('guest_preference_score:Q', title='Guest Preference Score'),
            alt.Y('venue_rating:Q', title='Venue Rating'),
            color=alt.Color('preferred_cuisine_types:N', title='Cuisine Type'),
            tooltip=['guest_preference_score:Q', 'venue_rating:Q', 'venue_name:N', 'preferred_cuisine_types:N']
        ).properties(
            title='Guest Preferences vs Venue Quality',
            width=380,
            height=340
        )
        charts.append(('Preference-Quality Correlation', correlation_chart))
    
    return charts

data = load_data()
if data.empty:
    st.error("No data found.")
    st.stop()

# Identify column types based on actual data
categorical_cols = [col for col in ["guest_id", "preferred_cuisine_types", "activity_preferences", "event_id", "event_name", 
                                   "event_category", "event_availability_status", "venue_id", "venue_name", "weather_condition"] if col in data.columns]
numeric_cols = [col for col in ["guest_preference_score", "guest_sentiment_rating", "venue_rating", "venue_price_level", 
                               "venue_latitude", "venue_longitude", "transportation_eta_minutes", "transportation_cost_estimate", 
                               "temperature_fahrenheit"] if col in data.columns]
date_cols = [col for col in ["event_start_datetime", "recommendation_timestamp"] if col in data.columns]

sample_cols = data.columns.tolist()
numeric_candidates = [col for col in sample_cols if data[col].dtype in ['float64', 'int64'] and 'id' not in col.lower()]
date_candidates = [col for col in sample_cols if 'date' in col.lower() or 'timestamp' in col.lower()]
cat_candidates = [col for col in sample_cols if data[col].dtype == 'object' and data[col].nunique() < 1000]

# Calculate key variables that will be used throughout the application
if 'guest_sentiment_rating' in data.columns and 'venue_rating' in data.columns:
    # Calculate guest experience efficiency as satisfaction correlation with venue quality
    guest_satisfaction = data['guest_sentiment_rating'].mean()
    venue_quality = data['venue_rating'].mean()
    guest_experience_efficiency = (guest_satisfaction * venue_quality) / 10 * 20  # Scale to percentage
else:
    guest_experience_efficiency = 85.0  # Default assumption

# Four tabs - Metrics first, then AI Insights
tabs = st.tabs(["📊 Metrics", "✨ AI Insights", "📁 Insights History", "🔍 Data Explorer"])

# Metrics tab placeholder - existing code will be inserted here
with tabs[0]:
    st.subheader("📊 Key Hospitality Concierge Metrics")
    
    # Display key metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'guest_sentiment_rating' in data.columns:
            avg_satisfaction = data['guest_sentiment_rating'].mean()
            max_satisfaction = data['guest_sentiment_rating'].max()
            st.metric("Avg Guest Satisfaction", f"{avg_satisfaction:.1f}/10", delta=f"Peak: {max_satisfaction:.1f}")
    
    with col2:
        if 'venue_rating' in data.columns:
            avg_venue_rating = data['venue_rating'].mean()
            venues_above_4 = (data['venue_rating'] >= 4.0).sum()
            st.metric("Avg Venue Rating", f"{avg_venue_rating:.1f}★", delta=f"{venues_above_4} venues 4★+")
    
    with col3:
        if 'transportation_eta_minutes' in data.columns:
            avg_eta = data['transportation_eta_minutes'].mean()
            eta_std = data['transportation_eta_minutes'].std()
            st.metric("Avg Transportation ETA", f"{avg_eta:.1f} min", delta=f"±{eta_std:.1f} min variability")
    
    with col4:
        if 'transportation_cost_estimate' in data.columns:
            avg_cost = data['transportation_cost_estimate'].mean()
            total_transport_value = data['transportation_cost_estimate'].sum()
            st.metric("Avg Transport Cost", f"${avg_cost:.2f}", delta=f"${total_transport_value:,.0f} total")
    
    st.markdown("---")
    
    # Create and display charts
    charts = create_metrics_charts(data)
    
    # ---- Title clipping fix (Altair) ----
    # 1) Push the title down from the top edge using TitleParams(offset=...)
    # 2) Give the chart extra top padding so the title never clips in Snowflake Streamlit
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
        st.subheader("📈 Performance Visualizations")
        # Display in a 2-column grid (kept consistent with your Snowflake AGR structure)
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
            st.markdown("**🏨 Key Guest Experience Metrics**")
            key_metrics = ['guest_sentiment_rating', 'guest_preference_score', 'venue_rating', 'venue_price_level']
            key_metrics_present = [m for m in key_metrics if m in summary_df.index]
            
            if key_metrics_present:
                for metric in key_metrics_present:
                    mean_val = summary_df.loc[metric, 'Mean']
                    min_val = summary_df.loc[metric, 'Min']
                    max_val = summary_df.loc[metric, 'Max']
                    
                    # Format based on metric type
                    if 'sentiment' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.2f}/10",
                            help=f"Range: {min_val:.2f} - {max_val:.2f}"
                        )
                    elif 'rating' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.2f}★",
                            help=f"Range: {min_val:.2f} - {max_val:.2f}"
                        )
                    elif 'price' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"${mean_val:.0f}",
                            help=f"Range: ${min_val:.0f} - ${max_val:.0f}"
                        )
                    else:
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.2f}",
                            help=f"Range: {min_val:.2f} - {max_val:.2f}"
                        )
        
        with col2:
            st.markdown("**🚗 Transportation & Location Insights**")
            
            # Calculate and display key insights
            insights = []
            
            if 'temperature_fahrenheit' in summary_df.index:
                temp_mean = summary_df.loc['temperature_fahrenheit', 'Mean']
                temp_range = summary_df.loc['temperature_fahrenheit', 'Max'] - summary_df.loc['temperature_fahrenheit', 'Min']
                insights.append(f"• **Average Temperature**: {temp_mean:.1f}°F")
                insights.append(f"• **Temperature Range**: {temp_range:.1f}°F")
            
            if 'transportation_eta_minutes' in summary_df.index:
                eta_mean = summary_df.loc['transportation_eta_minutes', 'Mean']
                eta_median = summary_df.loc['transportation_eta_minutes', '50% (Median)']
                insights.append(f"• **Average ETA**: {eta_mean:.1f} minutes")
                insights.append(f"• **Typical ETA**: {eta_median:.1f} minutes")
            
            if 'transportation_cost_estimate' in summary_df.index:
                cost_mean = summary_df.loc['transportation_cost_estimate', 'Mean']
                cost_q75 = summary_df.loc['transportation_cost_estimate', '75%']
                insights.append(f"• **Average Transport Cost**: ${cost_mean:.2f}")
                insights.append(f"• **75% of rides under**: ${cost_q75:.2f}")
            
            if 'venue_latitude' in summary_df.index and 'venue_longitude' in summary_df.index:
                lat_range = summary_df.loc['venue_latitude', 'Max'] - summary_df.loc['venue_latitude', 'Min']
                lon_range = summary_df.loc['venue_longitude', 'Max'] - summary_df.loc['venue_longitude', 'Min']
                insights.append(f"• **Geographic Coverage**: {lat_range:.3f}° lat × {lon_range:.3f}° lon")
            
            # Add categorical insights
            if 'preferred_cuisine_types' in data.columns:
                cuisine_distribution = data['preferred_cuisine_types'].value_counts()
                top_cuisine = cuisine_distribution.index[0]
                top_count = cuisine_distribution.iloc[0]
                insights.append(f"• **Most Popular Cuisine**: {top_cuisine} ({top_count} preferences)")
            
            if 'activity_preferences' in data.columns:
                activity_distribution = data['activity_preferences'].value_counts()
                top_activity = activity_distribution.index[0]
                top_activity_count = activity_distribution.iloc[0]
                insights.append(f"• **Top Activity Type**: {top_activity} ({top_activity_count} bookings)")
            
            if 'weather_condition' in data.columns:
                weather_distribution = data['weather_condition'].value_counts()
                top_weather = weather_distribution.index[0]
                insights.append(f"• **Most Common Weather**: {top_weather}")
            
            if 'event_availability_status' in data.columns:
                available_events = (data['event_availability_status'] == 'Available').sum()
                total_events = len(data)
                availability_rate = (available_events / total_events) * 100
                insights.append(f"• **Event Availability Rate**: {availability_rate:.1f}%")
            
            # Calculate guest satisfaction insights
            if 'guest_sentiment_rating' in data.columns and 'venue_rating' in data.columns:
                high_satisfaction = (data['guest_sentiment_rating'] >= 8.0).sum()
                high_venue_quality = (data['venue_rating'] >= 4.0).sum()
                total_records = len(data)
                insights.append(f"• **High Guest Satisfaction**: {(high_satisfaction/total_records)*100:.1f}% (8+ rating)")
                insights.append(f"• **Quality Venues**: {(high_venue_quality/total_records)*100:.1f}% (4+ stars)")
            
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
    st.subheader("✨ AI-Powered Hospitality Insights with Agent Workflows")
    st.markdown("**Experience behind-the-scenes AI agent processing for each guest experience analysis focus area**")
    
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
        if st.button("🚀 Start Guest Experience Agent"):
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
        with st.spinner("Guest Experience Agent Running..."):
            insights = generate_insights_with_agent_workflow(data, focus_area, selected_model, progress_placeholder)
            
            if insights:
                # Show completion message
                st.success(f"🎉 {focus_area} Guest Experience Agent completed with real hospitality data analysis!")
                
                # Show report in expandable section
                with st.expander(f"📋 Generated {focus_area} Report (Real Hospitality Data)", expanded=True):
                    st.markdown(f"""
                    <div class="agent-report-header">
                        <strong>{focus_area} Report - AI-Generated Hospitality Analysis</strong><br>
                        <small>Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</small><br>
                        <small>Data Source: Live Snowflake Hospitality Guest Analysis</small><br>
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
                    "📥 Download Guest Experience Analysis Report", 
                    insights, 
                    file_name=f"{solution_name.replace(' ', '_').lower()}_{focus_area.lower().replace(' ', '_')}_report.md",
                    mime="text/markdown"
                )
                
                # Stop the agent after completion
                st.session_state[agent_running_key] = False

# Insights History tab placeholder - existing code will be inserted here
with tabs[2]:
    st.subheader("📁 Insights History")
    if st.session_state.insights_history:
        for i, item in enumerate(reversed(st.session_state.insights_history)):
            with st.expander(f"{item['timestamp']} - {item['focus']} ({item.get('model', 'Unknown')})", expanded=False):
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
   - App title: `LocalLink`
   - Database: Select `SF_LABUSER#_DB` (only option available for your user)
   - Schema: Select `hospitality_hpt_connector` the schema created by your Fivetran connector (this should be the only schema available other than Public - do not select Public)
6. In the Streamlit Editor that appears (left side of the Streamlit UI), select all text (Command+A) and delete it
7. Paste the copied Streamlit application code into the empty editor (Command+V):
8. Click the blue **Run** button in the upper right corner
9. Close the editor by clicking the middle icon in the bottom left navigation

## 3.3 Explore the Streamlit in Snowflake Gen AI Data App
The LocalLink data app should now be running with the following sections:
- **Metrics**: Key performance indicators, guest experience analytics, guest satisfaction distribution, venue ratings by price level, popular activity references, preferred cuisine types, weather impact on guest preferences, transportation efficiency analysis, event first availability status, guest preferences vs venue quality
- **AI Insights**: Generate in-depth analysis of overall performance, optimization opportunities, financial impact, and strategic recommendations
- **Insights History**: Access previously generated insights for reference and comparison across different time periods and focus areas.
- **Data Explorer**: Explore the underlying hospitality data with pagination controls to examine individual guest preferences and concierge recommendation metrics

## Done!
You've successfully:
1. Created a custom Fivetran connector using the Fivetran Connector SDK
2. Deployed the connector to sync hospitality data into Snowflake
3. Built a Streamlit in Snowflake data app to visualize and analyze the data using Snowflake Cortex

## Next Steps
Consider how you might adapt this solution for your own use:
- Integration with other hospitality systems
- Adding real-time monitoring or projects
- Implementing machine learning models for more sophisticated results
- Customizing the Streamlit app for specific hospitality needs

## Resources
- Fivetran Connector SDK Documentation: [https://fivetran.com/docs/connectors/connector-sdk](https://fivetran.com/docs/connectors/connector-sdk)  
- Fivetran Connector SDK Examples: [https://fivetran.com/docs/connector-sdk/examples](https://fivetran.com/docs/connector-sdk/examples)
- API Connector Reference: [https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/hpt_api_spec](https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/hpt_api_spec)
- Snowflake Cortex Documentation: [https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- Snowflake Streamlit Documentation: [https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)