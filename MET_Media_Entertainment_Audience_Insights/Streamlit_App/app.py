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
    page_icon="https://i.imgur.com/vAoVPLQ.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
    <img src="https://i.imgur.com/vAoVPLQ.png" width="100" style="margin-right:15px">
    <div>
        <h1 style="font-size:2.2rem; margin:0; padding:0">{solution_name_clean.replace('_', ' ').title()}</h1>
        <p style="font-size:1.1rem; color:gray; margin:0; padding:0">Fivetran and Cortex-powered Streamlit in Snowflake data application for Media and Entertainment Audience Analytics</p>
    </div>
</div>
''', unsafe_allow_html=True)

# Define available models as strings
MODELS = [
    "claude-4-sonnet", "claude-3-7-sonnet", "claude-3-5-sonnet", "llama3.1-8b", "llama3.1-70b", "llama4-maverick", "llama4-scout", "snowflake-llama-3.1-405b", "snowflake-llama-3.3-70b", "mistral-large2", "deepseek-r1"
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

data = load_data()
if data.empty:
    st.error("No data found.")
    st.stop()

categorical_cols = [col for col in ["age_range", "gender", "location_city", "location_country", "content_preferences", "customer_segment", "recommended_content_type", "engagement_trend"] if col in data.columns]
numeric_cols = [col for col in ["social_media_followers", "social_engagement_score", "total_purchase_value", "purchase_frequency", "website_sessions", "avg_session_duration", "conversion_rate", "lead_score", "predicted_churn_risk"] if col in data.columns]
date_cols = [col for col in ["record_timestamp", "last_purchase_date"] if col in data.columns]

sample_cols = data.columns.tolist()
numeric_candidates = [col for col in sample_cols if data[col].dtype in ['float64', 'int64'] and 'id' not in col.lower()]
date_candidates = [col for col in sample_cols if 'date' in col.lower() or 'timestamp' in col.lower()]
cat_candidates = [col for col in sample_cols if data[col].dtype == 'object' and data[col].nunique() < 1000]

# Four tabs - Metrics tab first, then AI Insights
tabs = st.tabs(["📊 Metrics", "✨ AI Insights", "📁 Insights History", "🔍 Data Explorer"])

# Metrics tab (PRIMARY position)
with tabs[0]:
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

# AI Insights tab (SECONDARY position)
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