import streamlit as st
import pandas as pd
import altair as alt
import time
import json
import re
from datetime import datetime
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="churnguard_–_ai_driven_customer_retention",
    page_icon="https://i.imgur.com/Og6gFnB.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

solution_name = '''Solution 2: ChurnGuard – AI-driven Customer Retention'''
solution_name_clean = '''churnguard_–_ai_driven_customer_retention'''
table_name = '''TLC_RECORDS'''
table_description = '''Consolidated customer data combining CRM interactions, usage analytics, and social sentiment for AI-driven churn prediction and retention strategies'''
solution_content = '''Solution 2: ChurnGuard – AI-driven Customer Retention**

- **Primary Business Challenge:** High customer churn rates leading to revenue loss and increased acquisition costs.

- **Key Features:**
  - Customer churn prediction
  - Personalized retention strategies
  - Real-time customer sentiment analysis
  - Targeted marketing campaigns

- **Data Sources:**
  - Customer Relationship Management (CRM): Salesforce, Zoho CRM
  - Customer Usage Data: Ericsson Expert Analytics, Nokia Customer Experience Management
  - Social Media Data: Twitter API, Facebook Graph API

- **Competitive Advantage:**
  - Proactive customer retention strategies reduce churn and improve customer loyalty.
  - Personalized marketing campaigns increase customer engagement and satisfaction.

- **Key Stakeholders:**
  - Marketing Managers, Customer Service Representatives, Sales Teams
  - Top C-level Executive: Chief Marketing Officer (CMO)

- **Technical Approach with Generative AI:**
  - Use generative models to predict customer churn based on historical data.
  - Implement natural language processing (NLP) for sentiment analysis.
  - Generate personalized retention strategies using customer segmentation.

- **Expected Business Results:**
  - "10% reduction in customer churn rate"
    **10,000 customers/year × 10% churn rate × 10% reduction = 100 fewer churned customers/year**
  - "$ 1,000,000 in revenue retention annually"
    **$ 10,000,000 annual revenue × 10% churn rate × 10% reduction = $ 1,000,000 retained revenue/year**
  - "15% increase in customer engagement"
    **60% baseline engagement rate × 15% improvement = 69% engagement rate**
  - "20% reduction in customer acquisition costs"
    **$ 5,000,000 annual acquisition costs × 20% reduction = $ 1,000,000 savings/year**

- **Success Metrics:**
  - Reduction in customer churn rate
  - Revenue retention
  - Increase in customer engagement
  - Reduction in customer acquisition costs

- **Risk Assessment:**
  - Potential implementation challenges: Data privacy concerns, model bias
  - Mitigation strategies: Compliance with data protection regulations, continuous model bias assessment

- **Long-term Evolution:**
  - Integration with IoT devices for real-time customer behavior analysis
  - Expansion to include predictive analytics for new customer acquisition

**'''

# Display logo and title inline
st.markdown(f'''
<div style="display:flex; align-items:center; margin-bottom:15px">
    <img src="https://i.imgur.com/Og6gFnB.png" width="100" style="margin-right:15px">
    <div>
        <h1 style="font-size:2.2rem; margin:0; padding:0">{solution_name_clean.replace('_', ' ').title()}</h1>
        <p style="font-size:1.1rem; color:gray; margin:0; padding:0">Fivetran and Cortex-powered Streamlit in Snowflake data application for Customer Retention</p>
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
    # Define key customer retention metrics that should be numeric
    key_metrics = ["total_contract_value", "support_tickets_count", "monthly_usage_minutes", 
                   "data_consumption_gb", "service_quality_score", "network_performance_rating", 
                   "social_sentiment_score", "social_mentions_count", "engagement_score", 
                   "churn_risk_probability"]
    
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
    categorical_options = ["customer_id", "account_name", "customer_tier", "usage_trend_30d", "payment_status"]
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
                correlation_info = "Top correlations between customer metrics:\n"
                for col1, col2, _, corr_value in corr_pairs[:3]:
                    correlation_info += f"- {col1} and {col2}: r = {corr_value:.2f}\n"
        except Exception as e:
            correlation_info = "Could not calculate correlations between customer metrics.\n"

    # Define specific instructions for each focus area tailored to customer retention
    focus_area_instructions = {
        "Overall Performance": """
        For the Overall Performance analysis of ChurnGuard in Customer Retention:
        1. Provide a comprehensive analysis of the customer retention system using churn risk probabilities, customer engagement scores, and service quality metrics
        2. Identify significant patterns in customer behavior, usage trends, support ticket volumes, and social sentiment across different customer tiers and payment statuses
        3. Highlight 3-5 key customer retention metrics that best indicate retention success (churn risk probability, engagement score, service quality score, social sentiment)
        4. Discuss both strengths and areas for improvement in the AI-powered customer retention process
        5. Include 3-5 actionable insights for improving customer retention based on the CRM and usage data
        
        Structure your response with these customer retention focused sections:
        - Customer Retention Performance Insights (5 specific insights with supporting engagement and churn data)
        - Customer Behavior Trends (3-4 significant trends in usage patterns and engagement)
        - Retention Strategy Recommendations (3-5 data-backed recommendations for improving customer retention operations)
        - Implementation Steps (3-5 concrete next steps for marketing managers and customer service teams)
        """,
        
        "Optimization Opportunities": """
        For the Optimization Opportunities analysis of ChurnGuard in Customer Retention:
        1. Focus specifically on areas where customer retention rates, engagement levels, and churn prediction accuracy can be improved
        2. Identify inefficiencies in customer service delivery, engagement campaigns, support ticket resolution, and social sentiment management
        3. Analyze correlations between customer usage patterns, support interactions, payment status, and churn risk probability
        4. Prioritize optimization opportunities based on potential impact on reducing customer churn and improving retention
        5. Suggest specific technical or process improvements for integration with existing CRM systems (Salesforce, Zoho CRM)
        
        Structure your response with these customer retention focused sections:
        - Customer Retention Optimization Priorities (3-5 areas with highest churn reduction and engagement improvement potential)
        - Customer Experience Impact Analysis (quantified benefits of addressing each opportunity in terms of retention rate improvement)
        - CRM Integration Strategy (specific steps for customer service teams to implement each optimization)
        - System Integration Recommendations (specific technical changes needed for seamless integration with Salesforce and Zoho CRM systems)
        - Customer Retention Risk Assessment (potential challenges for marketing managers and how to mitigate them)
        """,
        
        "Financial Impact": """
        For the Financial Impact analysis of ChurnGuard in Customer Retention:
        1. Focus on cost-benefit analysis and ROI in customer retention terms (customer acquisition costs vs. retention campaign effectiveness)
        2. Quantify financial impacts through churn reduction, increased customer lifetime value, and improved customer engagement
        3. Identify cost savings opportunities across different customer tiers and retention strategies
        4. Analyze customer contract values and support costs across different engagement levels and churn risk categories
        5. Project future financial outcomes based on improved customer retention rates and reduced churn
        
        Structure your response with these customer retention focused sections:
        - Customer Retention Cost Analysis (breakdown of acquisition costs and potential savings by customer tier and engagement level)
        - Churn Reduction Impact (how improved customer retention affects revenue and customer lifetime value)
        - Customer Retention ROI Calculation (specific calculations showing return on investment in terms of churn reduction and revenue retention)
        - Cost Reduction Opportunities (specific areas to reduce customer acquisition costs and improve retention efficiency)
        - Financial Forecasting (projections based on improved customer retention performance metrics)
        """,
        
        "Strategic Recommendations": """
        For the Strategic Recommendations analysis of ChurnGuard in Customer Retention:
        1. Focus on long-term strategic implications for digital transformation in customer retention and engagement management
        2. Identify competitive advantages against traditional customer retention approaches
        3. Suggest new directions for AI integration with emerging customer engagement technologies (e.g., real-time sentiment analysis, predictive customer behavior modeling)
        4. Connect recommendations to broader business goals of improving customer lifetime value and reducing acquisition costs
        5. Provide a digital customer retention roadmap with prioritized initiatives
        
        Structure your response with these customer retention focused sections:
        - Digital Customer Experience Context (how ChurnGuard fits into broader digital transformation in customer retention management)
        - Competitive Advantage Analysis (how to maximize retention advantages compared to traditional customer service approaches)
        - Customer Retention Technology Strategic Priorities (3-5 high-impact strategic initiatives for improving customer engagement operations)
        - Advanced Customer Analytics Integration Vision (how to evolve ChurnGuard with AI and real-time data over 1-3 years)
        - Customer Retention Transformation Roadmap (sequenced steps for expanding to predictive customer behavior analysis and autonomous retention campaigns)
        """
    }

    # Get the specific instructions for the selected focus area
    selected_focus_instructions = focus_area_instructions.get(focus_area, "")

    prompt = f'''
    You are an expert data analyst specializing in {focus_area.lower()} analysis for customer retention and churn prevention.

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
    - Frame all insights in the context of customer retention and churn prevention
    '''

    return call_cortex_model(prompt, model_name)

def create_metrics_charts(data):
    """Create metric visualizations for the customer retention data"""
    charts = []
    
    # Churn Risk Probability Distribution
    if 'churn_risk_probability' in data.columns:
        churn_chart = alt.Chart(data).mark_bar().encode(
            alt.X('churn_risk_probability:Q', bin=alt.Bin(maxbins=15), title='Churn Risk Probability'),
            alt.Y('count()', title='Number of Customers'),
            color=alt.value('#ff7f0e')
        ).properties(
            title='Customer Churn Risk Distribution',
            width=380,
            height=340
        )
        charts.append(('Churn Risk Distribution', churn_chart))
    
    # Customer Engagement by Tier
    if 'engagement_score' in data.columns and 'customer_tier' in data.columns:
        engagement_chart = alt.Chart(data).mark_boxplot().encode(
            alt.X('customer_tier:N', title='Customer Tier'),
            alt.Y('engagement_score:Q', title='Engagement Score'),
            color=alt.Color('customer_tier:N', legend=None)
        ).properties(
            title='Customer Engagement by Tier',
            width=380,
            height=340
        )
        charts.append(('Engagement by Tier', engagement_chart))
    
    # Service Quality vs Churn Risk
    if 'service_quality_score' in data.columns and 'churn_risk_probability' in data.columns:
        quality_chart = alt.Chart(data).mark_point(size=60, opacity=0.7).encode(
            alt.X('service_quality_score:Q', title='Service Quality Score'),
            alt.Y('churn_risk_probability:Q', title='Churn Risk Probability'),
            color=alt.Color('customer_tier:N', title='Customer Tier'),
            tooltip=['service_quality_score:Q', 'churn_risk_probability:Q', 'customer_tier:N', 'account_name:N']
        ).properties(
            title='Service Quality vs Churn Risk',
            width=380,
            height=340
        )
        charts.append(('Quality vs Churn Risk', quality_chart))
    
    # Payment Status Distribution
    if 'payment_status' in data.columns:
        payment_chart = alt.Chart(data).mark_bar().encode(
            alt.X('payment_status:N', title='Payment Status'),
            alt.Y('count()', title='Number of Customers'),
            color=alt.Color('payment_status:N', legend=None),
            tooltip=['payment_status:N', 'count()']
        ).properties(
            title='Customer Payment Status Distribution',
            width=380,
            height=340
        )
        charts.append(('Payment Status Distribution', payment_chart))
    
    # Social Sentiment Analysis
    if 'social_sentiment_score' in data.columns and 'social_mentions_count' in data.columns:
        sentiment_chart = alt.Chart(data).mark_point(size=60, opacity=0.7).encode(
            alt.X('social_sentiment_score:Q', title='Social Sentiment Score'),
            alt.Y('social_mentions_count:Q', title='Social Mentions Count'),
            color=alt.Color('churn_risk_probability:Q', title='Churn Risk', scale=alt.Scale(scheme='reds')),
            tooltip=['social_sentiment_score:Q', 'social_mentions_count:Q', 'churn_risk_probability:Q', 'account_name:N']
        ).properties(
            title='Social Sentiment vs Mentions',
            width=380,
            height=340
        )
        charts.append(('Social Sentiment Analysis', sentiment_chart))
    
    # Contract Value by Usage Trend
    if 'total_contract_value' in data.columns and 'usage_trend_30d' in data.columns:
        contract_chart = alt.Chart(data).mark_bar().encode(
            alt.X('usage_trend_30d:N', title='Usage Trend (30 days)'),
            alt.Y('mean(total_contract_value):Q', title='Average Contract Value ($)'),
            color=alt.Color('usage_trend_30d:N', legend=None),
            tooltip=['usage_trend_30d:N', 'mean(total_contract_value):Q']
        ).properties(
            title='Contract Value by Usage Trend',
            width=380,
            height=340
        )
        charts.append(('Contract Value by Trend', contract_chart))
    
    # Support Tickets vs Churn Risk
    if 'support_tickets_count' in data.columns and 'churn_risk_probability' in data.columns:
        support_chart = alt.Chart(data).mark_point(size=60, opacity=0.7).encode(
            alt.X('support_tickets_count:Q', title='Support Tickets Count'),
            alt.Y('churn_risk_probability:Q', title='Churn Risk Probability'),
            color=alt.Color('service_quality_score:Q', title='Service Quality', scale=alt.Scale(scheme='viridis')),
            tooltip=['support_tickets_count:Q', 'churn_risk_probability:Q', 'service_quality_score:Q', 'account_name:N']
        ).properties(
            title='Support Tickets vs Churn Risk',
            width=380,
            height=340
        )
        charts.append(('Support vs Churn Risk', support_chart))
    
    # Data Usage Analysis
    if 'data_consumption_gb' in data.columns and 'monthly_usage_minutes' in data.columns:
        usage_chart = alt.Chart(data).mark_point(size=60, opacity=0.7).encode(
            alt.X('monthly_usage_minutes:Q', title='Monthly Usage (minutes)'),
            alt.Y('data_consumption_gb:Q', title='Data Consumption (GB)'),
            color=alt.Color('customer_tier:N', title='Customer Tier'),
            tooltip=['monthly_usage_minutes:Q', 'data_consumption_gb:Q', 'customer_tier:N', 'account_name:N']
        ).properties(
            title='Usage Patterns by Customer Tier',
            width=380,
            height=340
        )
        charts.append(('Usage Patterns', usage_chart))
    
    return charts

data = load_data()
if data.empty:
    st.error("No data found.")
    st.stop()

# Identify column types based on actual data
categorical_cols = [col for col in ["customer_id", "account_name", "customer_tier", "usage_trend_30d", "payment_status"] if col in data.columns]
numeric_cols = [col for col in ["total_contract_value", "support_tickets_count", "monthly_usage_minutes", "data_consumption_gb", "service_quality_score", "network_performance_rating", "social_sentiment_score", "social_mentions_count", "engagement_score", "churn_risk_probability"] if col in data.columns]
date_cols = [col for col in ["account_created_date", "last_interaction_date", "last_updated_timestamp"] if col in data.columns]

sample_cols = data.columns.tolist()
numeric_candidates = [col for col in sample_cols if data[col].dtype in ['float64', 'int64'] and 'id' not in col.lower()]
date_candidates = [col for col in sample_cols if 'date' in col.lower() or 'timestamp' in col.lower()]
cat_candidates = [col for col in sample_cols if data[col].dtype == 'object' and data[col].nunique() < 1000]

# Four tabs - Metrics tab first (PRIMARY), then AI Insights (SECONDARY)
tabs = st.tabs(["📊 Metrics", "✨ AI Insights", "📁 Insights History", "🔍 Data Explorer"])

# Metrics tab (PRIMARY - position 1)
with tabs[0]:
    st.subheader("📊 Key Customer Retention Metrics")
    
    # Display key metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'churn_risk_probability' in data.columns:
            avg_churn_risk = data['churn_risk_probability'].mean()
            st.metric("Avg Churn Risk Probability", f"{avg_churn_risk:.1%}", delta=f"{(0.25 - avg_churn_risk):.1%} vs target")
    
    with col2:
        if 'engagement_score' in data.columns:
            avg_engagement = data['engagement_score'].mean()
            st.metric("Avg Customer Engagement", f"{avg_engagement:.1f}", delta=f"{(avg_engagement - 75.0):.1f} vs target")
    
    with col3:
        if 'service_quality_score' in data.columns:
            avg_quality = data['service_quality_score'].mean()
            st.metric("Avg Service Quality", f"{avg_quality:.2f}/10", delta=f"{(avg_quality - 8.0):.2f} vs target")
    
    with col4:
        if 'retention_campaign_active' in data.columns:
            active_campaigns = data['retention_campaign_active'].sum()
            total_customers = len(data)
            campaign_rate = active_campaigns / total_customers
            st.metric("Active Retention Campaigns", f"{campaign_rate:.1%}", delta=f"{active_campaigns} customers")
    
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
            st.markdown("**🎯 Key Customer Retention Metrics**")
            key_metrics = ['churn_risk_probability', 'engagement_score', 'service_quality_score', 'total_contract_value']
            key_metrics_present = [m for m in key_metrics if m in summary_df.index]
            
            if key_metrics_present:
                for metric in key_metrics_present:
                    mean_val = summary_df.loc[metric, 'Mean']
                    min_val = summary_df.loc[metric, 'Min']
                    max_val = summary_df.loc[metric, 'Max']
                    
                    # Format based on metric type
                    if 'probability' in metric.lower() or 'risk' in metric.lower():
                        # For probabilities, assume they're in decimal form (0.0-1.0)
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.1%}",
                            help=f"Range: {min_val:.1%} - {max_val:.1%}"
                        )
                    elif 'score' in metric.lower():
                        # For scores, display as decimal with appropriate range
                        if max_val <= 10:  # Likely a 1-10 scale
                            st.metric(
                                label=metric.replace('_', ' ').title(),
                                value=f"{mean_val:.2f}/10",
                                help=f"Range: {min_val:.2f} - {max_val:.2f}"
                            )
                        elif max_val <= 100:  # Likely a 0-100 scale
                            st.metric(
                                label=metric.replace('_', ' ').title(),
                                value=f"{mean_val:.1f}/100",
                                help=f"Range: {min_val:.1f} - {max_val:.1f}"
                            )
                        else:
                            st.metric(
                                label=metric.replace('_', ' ').title(),
                                value=f"{mean_val:.2f}",
                                help=f"Range: {min_val:.2f} - {max_val:.2f}"
                            )
                    elif 'value' in metric.lower():
                        # For contract values, format as currency
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"${mean_val:,.0f}",
                            help=f"Range: ${min_val:,.0f} - ${max_val:,.0f}"
                        )
                    else:
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.2f}",
                            help=f"Range: {min_val:.2f} - {max_val:.2f}"
                        )
        
        with col2:
            st.markdown("**📊 Customer Retention Insights**")
            
            # Calculate and display key insights
            insights = []
            
            if 'churn_risk_probability' in summary_df.index:
                churn_mean = summary_df.loc['churn_risk_probability', 'Mean']
                churn_std = summary_df.loc['churn_risk_probability', 'Std Dev']
                insights.append(f"• **Churn Risk Variability**: {churn_std:.3f} (σ)")
                
                if churn_mean <= 0.20:
                    insights.append(f"• **Low churn risk** (Avg: {churn_mean:.1%})")
                elif churn_mean <= 0.35:
                    insights.append(f"• **Moderate churn risk** (Avg: {churn_mean:.1%})")
                else:
                    insights.append(f"• **⚠️ High churn risk detected** (Avg: {churn_mean:.1%})")
            
            if 'engagement_score' in summary_df.index:
                engagement_mean = summary_df.loc['engagement_score', 'Mean']
                if engagement_mean >= 80.0:
                    insights.append(f"• **High customer engagement** (Avg: {engagement_mean:.1f})")
                elif engagement_mean >= 70.0:
                    insights.append(f"• **Good customer engagement** (Avg: {engagement_mean:.1f})")
                else:
                    insights.append(f"• **⚠️ Low customer engagement** (Avg: {engagement_mean:.1f})")
            
            if 'service_quality_score' in summary_df.index:
                quality_q75 = summary_df.loc['service_quality_score', '75%']
                quality_q25 = summary_df.loc['service_quality_score', '25%']
                quality_iqr = quality_q75 - quality_q25
                insights.append(f"• **Service Quality IQR**: {quality_iqr:.2f}")
            
            if 'social_sentiment_score' in summary_df.index:
                sentiment_median = summary_df.loc['social_sentiment_score', '50% (Median)']
                insights.append(f"• **Median Social Sentiment**: {sentiment_median:.2f}")
                if sentiment_median > 0.3:
                    insights.append(f"• **Positive customer sentiment**: {sentiment_median:.2f}")
                elif sentiment_median < -0.3:
                    insights.append(f"• **⚠️ Negative customer sentiment**: {sentiment_median:.2f}")
            
            # Add categorical insights
            if 'customer_tier' in data.columns:
                tier_distribution = data['customer_tier'].value_counts()
                top_tier = tier_distribution.index[0]
                top_count = tier_distribution.iloc[0]
                insights.append(f"• **Top Customer Tier**: {top_tier} ({top_count} customers)")
            
            if 'payment_status' in data.columns:
                current_payments = (data['payment_status'] == 'Current').sum()
                total_customers = len(data)
                current_rate = current_payments / total_customers
                insights.append(f"• **Current Payment Rate**: {current_rate:.1%}")
            
            if 'usage_trend_30d' in data.columns:
                increasing_trend = (data['usage_trend_30d'] == 'Increasing').sum()
                insights.append(f"• **Customers with Increasing Usage**: {increasing_trend}")
            
            if 'retention_campaign_active' in data.columns:
                active_campaigns = data['retention_campaign_active'].sum()
                insights.append(f"• **Active Retention Campaigns**: {active_campaigns}")
            
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
            with st.expander(f"{item['timestamp']} - {item['focus']} ({item.get('model', 'Unknown')})", expanded=False):
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