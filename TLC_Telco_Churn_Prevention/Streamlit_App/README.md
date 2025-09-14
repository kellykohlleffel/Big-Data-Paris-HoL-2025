# ChurnGuard – AI-driven Customer Retention

![ChurnGuard](images/ChurnGuard.png)

A Fivetran and Cortex-powered Streamlit in Snowflake data application for AI-driven Customer Retention and Churn Prevention.

## Overview

ChurnGuard is an intelligent customer retention optimization system that helps telecommunications companies proactively identify at-risk customers and implement personalized retention strategies to reduce churn and maximize customer lifetime value. This Streamlit in Snowflake data application helps Chief Marketing Officers, Customer Service Representatives, and Marketing Managers reduce customer churn rates, increase customer engagement, and optimize retention campaign effectiveness through real-time analysis of CRM interactions, usage analytics, and social sentiment data.

The application utilizes a comprehensive customer dataset that consolidates data from customer relationship management platforms, usage analytics systems, and social media monitoring tools. This data is moved into Snowflake using Fivetran connectors, enabling reliable and efficient data pipelines for AI-driven customer retention analytics and personalized engagement strategies.

## Data Sources

The application is designed to work with data from major customer relationship management and telecommunications systems:

### Customer Retention Data Sources
- **Customer Relationship Management (CRM)**: 
  - Salesforce
  - Zoho CRM
  - HubSpot
- **Customer Usage Data**: 
  - Ericsson Expert Analytics
  - Nokia Customer Experience Management
  - Cisco Customer Experience Analytics
- **Social Media Data**: 
  - Twitter API
  - Facebook Graph API
  - Instagram Business API
- **Payment Processing**: 
  - Stripe
  - PayPal
  - Square
- **Customer Support**: 
  - Zendesk
  - ServiceNow
  - Freshdesk

The application consolidates these disparate data sources into a unified `TLC_RECORDS` table that provides a 360-degree view of customer behavior, engagement patterns, and churn risk indicators for comprehensive retention analytics.

## Key Features

- **AI-powered churn prediction**: Leverages generative AI models to analyze customer patterns and predict churn risk probability with high accuracy
- **Integration with comprehensive customer data**: Consolidates data from CRM systems, usage analytics, and social sentiment monitoring platforms
- **Comprehensive customer retention analytics**: Visual representation of key metrics including churn risk probability, customer engagement scores, service quality ratings, and retention campaign effectiveness
- **AI-powered insights**: Generate in-depth analysis of overall performance, optimization opportunities, financial impact, and strategic recommendations
- **Fivetran integration**: Utilizes Fivetran connectors to reliably move data from multiple customer touchpoints to Snowflake

## Streamlit Data App Sections

### 📊 Metrics
- **Key Performance Indicators**: Track average churn risk probability (32.4%), customer engagement scores (77.2/100), service quality ratings (8.4/10), and active retention campaigns
- **Customer Retention Analytics**: Monitor customer tier distribution, payment status analysis, and contract value optimization
- **Churn Risk Distribution**: Visualize churn risk probability across customer base to identify high-risk segments
- **Customer Engagement by Tier**: Analyze engagement scores across Basic, Standard, Business, Enterprise, and Premium customer tiers
- **Service Quality vs Churn Risk**: Correlation analysis between service quality scores and churn probability
- **Payment Status Distribution**: Monitor current (69.7%), late (15.3%), and overdue (6.3%) payment statuses
- **Social Sentiment Analysis**: Track social sentiment scores and mention volumes to gauge customer satisfaction
- **Contract Value Analysis**: Analyze contract values by usage trends and customer behavior patterns
- **Usage Pattern Analytics**: Review monthly usage minutes and data consumption patterns by customer tier

### ✨ AI Insights
Generate AI-powered insights with different focus areas:
- **Overall Performance**: Comprehensive analysis of the customer retention system using churn risk probabilities, engagement scores, and service quality metrics
- **Optimization Opportunities**: Areas where customer retention rates, engagement levels, and churn prediction accuracy can be improved
- **Financial Impact**: Cost-benefit analysis and ROI in customer retention terms (customer acquisition costs vs. retention campaign effectiveness)
- **Strategic Recommendations**: Long-term strategic implications for digital transformation in customer retention and engagement management

### 📁 Insights History
Access previously generated insights for reference and comparison across different time periods and focus areas.

### 🔍 Data Explorer
Explore the underlying customer data with pagination controls to examine individual customer records and retention metrics.

## Setup Instructions

1. Within Snowflake, click on **Projects**
2. Click on **Streamlit**
3. Click the blue box in the upper right to create a new Streamlit application
4. On the next page:
   - Name your application: `churnguard_ai_driven_customer_retention`
   - **IMPORTANT:** Set the database context
   - **IMPORTANT:** Set the schema context

### Fivetran Data Movement Setup

1. Configure Fivetran connectors for your customer data sources:
   - **Salesforce Connector**: Connect to Salesforce CRM for customer interaction data
   - **Zendesk Connector**: Connect to support ticket and service quality data
   - **Custom API Connectors**: Set up connectors for usage analytics and social media data
2. Start the Fivetran syncs in the Fivetran UI to move data into a `TLC_RECORDS` table in your Snowflake instance
3. Verify data is being loaded correctly by checking the table structure and sample records in Snowflake

## Data Flow

1. **Customer Data Collection**: Real-time customer data is collected from multiple touchpoints:
   - **CRM Systems**: Salesforce, Zoho CRM for customer interactions and account information
   - **Usage Analytics**: Ericsson Expert Analytics, Nokia Customer Experience Management for service usage patterns
   - **Social Media**: Twitter API, Facebook Graph API for sentiment analysis and brand mentions

2. **Automated Data Integration**: Fivetran connectors manage the orchestration and scheduling of data movement from various customer touchpoints into Snowflake

3. **Data Consolidation**: Customer data is consolidated into Snowflake as a unified `TLC_RECORDS` table combining CRM interactions, usage analytics, and social sentiment

4. **AI-Powered Analysis**: Snowpark for Python and Snowflake Cortex analyze the consolidated customer data to generate predictive insights and retention recommendations

5. **Interactive Visualization**: Streamlit in Snowflake presents the analyzed data in an interactive application for marketing managers and customer service teams

## Data Requirements

The application expects a table named `TLC_RECORDS` which contains consolidated customer data combining CRM interactions, usage analytics, and social sentiment for AI-driven churn prediction and retention strategies:

### Customer Identifiers
- `RECORD_ID`
- `CUSTOMER_ID`
- `ACCOUNT_NAME`

### Customer Information
- `CUSTOMER_TIER` (Basic, Standard, Business, Enterprise, Premium)
- `ACCOUNT_CREATED_DATE`
- `LAST_INTERACTION_DATE`
- `TOTAL_CONTRACT_VALUE`
- `PAYMENT_STATUS`

### Usage and Engagement Data
- `MONTHLY_USAGE_MINUTES`
- `DATA_CONSUMPTION_GB`
- `USAGE_TREND_30D`
- `ENGAGEMENT_SCORE`

### Service Quality Metrics
- `SUPPORT_TICKETS_COUNT`
- `SERVICE_QUALITY_SCORE`
- `NETWORK_PERFORMANCE_RATING`

### Social Media and Sentiment Data
- `SOCIAL_SENTIMENT_SCORE`
- `SOCIAL_MENTIONS_COUNT`

### Retention Analytics
- `CHURN_RISK_PROBABILITY`
- `RETENTION_CAMPAIGN_ACTIVE`
- `LAST_UPDATED_TIMESTAMP`

## Benefits

- **10% reduction in customer churn rate**: 10,000 customers/year × 10% churn rate × 10% reduction = 100 fewer churned customers/year
- **$1,000,000 in revenue retention annually**: $10,000,000 annual revenue × 10% churn rate × 10% reduction = $1,000,000 retained revenue/year
- **15% increase in customer engagement**: 60% baseline engagement rate × 15% improvement = 69% engagement rate
- **20% reduction in customer acquisition costs**: $5,000,000 annual acquisition costs × 20% reduction = $1,000,000 savings/year

## Technical Details

This application uses:
- Streamlit in Snowflake for the user interface
- Snowflake Cortex for AI-powered insights generation with multiple models:
  - Claude 4 Sonnet, Claude 3.5 Sonnet
  - OpenAI GPT models (GPT-4.1, GPT-5 series)
  - Llama 3.1/3.3 (8B, 70B, 405B variants)
  - Mistral Large2, DeepSeek R1, Snowflake Arctic
- Snowpark for Python for data processing and analytics
- **Fivetran connectors** for automated, reliable data movement from CRM, usage analytics, and social media platforms into Snowflake
- **Altair** for interactive data visualizations with enhanced title positioning and padding for Snowflake environments

## Success Metrics

- Reduction in customer churn rate from current 32.4% average risk
- Revenue retention through proactive engagement strategies
- Increase in customer engagement scores above current 77.2 average
- Reduction in customer acquisition costs through improved retention
- Enhanced customer satisfaction measured through social sentiment scores

## Key Stakeholders

- **Primary**: Marketing Managers, Customer Service Representatives, Sales Teams
- **Secondary**: Customer Success Managers, Business Analysts, CRM Administrators
- **Tertiary**: Data Scientists, IT Operations, Customer Experience Teams
- **Top C-Level Executive**: Chief Marketing Officer (CMO)

## Competitive Advantage

ChurnGuard provides proactive customer retention strategies that reduce churn and improve customer loyalty through AI-driven personalization. Unlike reactive traditional customer service approaches, the system enables personalized marketing campaigns that increase customer engagement and satisfaction, giving businesses a significant competitive edge in customer retention and lifetime value optimization.

The technical approach leverages generative models to predict customer churn based on historical data, implements natural language processing (NLP) for sentiment analysis, and generates personalized retention strategies using advanced customer segmentation techniques.

## Risk Assessment

**Potential Implementation Challenges**: Data privacy concerns regarding customer information, model bias in churn prediction algorithms, integration complexity across multiple customer touchpoints

**Mitigation Strategies**: Strict compliance with data protection regulations (GDPR, CCPA), continuous model bias assessment and fairness testing, phased implementation approach starting with pilot customer segments, comprehensive staff training on AI-driven retention strategies

## Long-term Evolution

**Near-term (6-12 months)**: Integration with IoT devices for real-time customer behavior analysis, expansion of social media monitoring to include more platforms and sentiment sources

**Medium-term (1-2 years)**: Expansion to include predictive analytics for new customer acquisition, development of autonomous retention campaign optimization, integration with customer journey mapping tools

**Long-term (3-5 years)**: Evolution toward fully autonomous customer experience management with real-time personalization, integration with emerging customer engagement technologies, expansion to cross-industry customer retention analytics platforms