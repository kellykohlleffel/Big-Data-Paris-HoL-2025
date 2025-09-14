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

# Four tabs - Metrics tab first, then AI Insights
tabs = st.tabs(["📊 Metrics", "✨ AI Insights", "📁 Insights History", "🔍 Data Explorer"])

# Metrics tab (PRIMARY - position 1)
with tabs[0]:
    st.subheader("📊 Key Performance Metrics")
    
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
    st.subheader("✨ AI-Powered Insights")
    focus_area = st.radio("Focus Area", [
        "Overall Performance", 
        "Optimization Opportunities", 
        "Financial Impact", 
        "Strategic Recommendations"
    ])
    selected_model = st.selectbox("Cortex Model", MODELS, index=0)

    if st.button("Generate Insights"):
        with st.spinner("Generating with Snowflake Cortex..."):
            insights = generate_insights(data, focus_area, selected_model)
            if insights:
                st.markdown(insights)
                timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
                st.session_state.insights_history.append({
                    "timestamp": timestamp,
                    "focus": focus_area,
                    "model": selected_model,
                    "insights": insights
                })
                st.download_button("Download Insights", insights, file_name=f"{solution_name.replace(' ', '_').lower()}_insights.md")
            else:
                st.error("No insights returned.")

# Insights History tab
with tabs[2]:
    st.subheader("📁 Insights History")
    if st.session_state.insights_history:
        for i, item in enumerate(reversed(st.session_state.insights_history)):
            with st.expander(f"{item['timestamp']} - {item['focus']} ({item['model']})", expanded=False):
                st.markdown(item["insights"])
    else:
        st.info("No insights generated yet. Go to the AI Insights tab to generate some insights.")

# Data Explorer tab
with tabs[3]:
    st.subheader("🔍 Data Explorer")
    rows_per_page = st.slider("Rows per page", 5, 50, 10)
    page = st.number_input("Page", min_value=1, value=1)
    start = (page - 1) * rows_per_page
    end = min(start + rows_per_page, len(data))
    st.dataframe(data.iloc[start:end], use_container_width=True)
    st.caption(f"Showing rows {start + 1}–{end} of {len(data)}")