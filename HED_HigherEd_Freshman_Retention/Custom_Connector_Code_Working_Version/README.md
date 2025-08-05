# HED Higher Education Fivetran Custom Connector

This connector uses the Fivetran Connector SDK to extract Higher Education Data (HED) from an API and load it into Snowflake via Fivetran, powering advanced student success and retention analytics applications.

## Overview

The HED Higher Education connector fetches student academic and engagement records from a REST API and loads them into a single table called `hed_records` in your Snowflake database. The connector retrieves detailed information about student performance, engagement metrics, academic standing, and intervention data. It handles authentication, pagination, error handling, and maintains state between sync runs using a cursor-based approach.

## Features

- **Incremental syncs**: Uses cursor-based pagination to efficiently process data in chunks
- **State management**: Tracks sync progress and saves checkpoints every 100 records
- **Error handling**: Gracefully handles API and runtime errors with detailed logging
- **Configuration management**: Customizable API endpoint, page size, and authentication
- **Minimal permissions**: Requires only an API key to access the HED data
- **Data enrichment**: Extracts over 22 metrics and attributes related to student success and retention
- **Efficient processing**: Handles large datasets (600+ records) with minimal resource usage
- **Higher education-specific**: Designed to handle student data common in higher education operations

## Project Structure

```
hed/
├── configuration.json     # Configuration parameters for the connector
├── connector.py           # Main connector implementation
├── debug_and_reset.sh     # Script for testing and resetting the connector
├── deploy.sh              # Script for deploying the connector to Fivetran
└── requirements.txt       # Python dependencies (if any)
```

## Configuration Parameters

The connector uses the following configuration parameters:

| Parameter    | Description                                            | Default Value                                             |
|--------------|--------------------------------------------------------|-----------------------------------------------------------|
| `api_key`    | API key for authentication to the HED API              | Required                                                  |
| `base_url`   | Base URL for the HED API                               | `https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com` |
| `page_size`  | Number of records to fetch per API request             | `100`                                                     |

## HED API Details

### Endpoint

The connector uses the following REST API endpoint to access higher education student data:

- `GET /hed_data`: Retrieves a paginated list of Higher Education student records

### Query Parameters

- `cursor`: The cursor for the next page of results (used for pagination)
- `page_size`: The number of records to return per page (1-200, defaults to 100)

### Authentication

The API uses API key authentication, with the key passed in the request header:

```
x-api-key: YOUR_API_KEY
```

### Data Schema

The HED API returns records with the following structure:

| Field                    | Type      | Description                                   |
|--------------------------|-----------|-----------------------------------------------|
| `record_id`              | string    | Unique identifier for the record (primary key) |
| `student_id`             | string    | Unique identifier for the student             |
| `enrollment_date`        | timestamp | Date when the student enrolled                |
| `academic_standing`      | string    | Current academic standing of the student      |
| `current_gpa`            | number    | Student's current GPA                         |
| `credit_hours_attempted` | number    | Total credit hours attempted                  |
| `credit_hours_earned`    | number    | Total credit hours earned                     |
| `major_code`             | string    | Student's major code                          |
| `advisor_id`             | string    | Unique identifier for the student's advisor   |
| `financial_aid_amount`   | number    | Amount of financial aid received              |
| `last_login_date`        | timestamp | Date of student's last system login          |
| `total_course_views`     | number    | Total number of course page views             |
| `assignment_submissions` | number    | Number of assignments submitted               |
| `discussion_posts`       | number    | Number of discussion forum posts              |
| `avg_assignment_score`   | number    | Average score on assignments                  |
| `course_completion_rate` | number    | Rate of course completion                     |
| `plagiarism_incidents`   | number    | Number of plagiarism incidents                |
| `writing_quality_score`  | number    | Score indicating writing quality              |
| `engagement_score`       | number    | Overall student engagement score              |
| `at_risk_flag`           | boolean   | Flag indicating if student is at risk         |
| `intervention_count`     | number    | Number of interventions performed             |
| `last_updated`           | timestamp | Date when the record was last updated         |

## Implementation Details

### Schema Definition

The connector defines a simple schema with a single table `hed_records` and specifies `record_id` as the primary key.

```python
def schema(configuration: dict):
    # Return minimal schema with ONLY table name and primary key
    return [
        {
            "table": "hed_records",
            "primary_key": ["record_id"]
        }
    ]
```

### Data Extraction

The connector follows a structured approach to extract data:

1. **Configuration validation**: Retrieves and validates the API key, base URL and page size
2. **Session setup**: Creates an authenticated session with the API key in the header
3. **State management**: Loads previous sync state to determine where to resume from
4. **Pagination handling**: Implements cursor-based pagination to fetch data in batches
5. **Data processing**: Yields upsert operations for each record to be loaded into Fivetran
6. **Checkpoint creation**: Saves checkpoints after every 100 records to ensure reliability
7. **Error handling**: Catches and logs API exceptions and unexpected errors

The connector uses this workflow to efficiently process large datasets while maintaining resiliency against failures.

## Setup and Usage

### Prerequisites

1. Python 3.7+ installed
2. The Fivetran Connector SDK installed:
   ```bash
   pip install fivetran-connector-sdk
   ```
3. DuckDB command-line tool (optional, for viewing debug output)
4. Basic understanding of the Fivetran platform

### Local Development and Testing

1. Clone or create the project directory:
   ```bash
   mkdir hed && cd hed
   ```

2. Create the required files:
   - `configuration.json`: Configure your API credentials and settings
   - `connector.py`: Implement the connector using the Fivetran SDK
   - `debug_and_reset.sh`: Script for testing
   - `deploy.sh`: Script for deployment

3. Make the scripts executable and run the debug script:
   ```bash
   chmod +x debug_and_reset.sh
   ./debug_and_reset.sh
   ```

   This will:
   - Reset the connector state
   - Run the connector in debug mode
   - Display sample data from the extracted records
   - Show operation statistics

### Debug Script Details

The `debug_and_reset.sh` script includes the following steps:

1. **Validation**: Checks for required files and extracts the table name
2. **Reset**: Clears previous state and data using `fivetran reset`
3. **Debug**: Runs the connector in debug mode with the specified configuration
4. **Query**: Executes a sample query against the extracted data using DuckDB
5. **Summary**: Displays a summary of operations performed (upserts, checkpoints, etc.)

When you run the debug script, you'll see output similar to the following:

```
✓ Detected table name: hed_records
===========================================================
         Fivetran Connector Debug & Reset Script          
===========================================================

This will reset your connector, delete current state and warehouse.db files.
Do you want to continue? (Y/n): y

Step 1: Resetting Fivetran connector...
✓ Reset successful

Step 2: Running debug with configuration...
(Real-time output will be displayed below)

Aug 05, 2025 08:28:51 AM INFO Fivetran-Connector-SDK: Debugging connector at: /path/to/connector
Aug 05, 2025 08:28:51 AM INFO Fivetran-Connector-SDK: Running connector tester...
Aug 05, 2025 08:28:53 AM INFO Fivetran-Connector-SDK: Initiating the 'schema' method call...
Aug 05, 2025 08:28:53 AM: INFO Fivetran-Tester-Process: [SchemaChange]: tester.hed_records 
Aug 05, 2025 08:28:53 AM INFO Fivetran-Connector-SDK: Initiating the 'update' method call...

Aug 05, 2025 08:28:53 AM INFO: Starting initial sync
Aug 05, 2025 08:28:53 AM INFO: Checkpoint at 100 records, cursor: gAAAAABokgcV1bApjxUCN5dMNXBfxvISrvX0SwHNbKJRml1qJaWPygw6VAZP4pONODYRg0TrV2CjUqxrJMlWg8KbZvpmO-gjb7H6FwSGrbiVymHzjJPkKZziHYULzBs2ydxPwzDSzWd2
Aug 05, 2025 08:28:53 AM INFO: Processed batch: 100 records, has_more: True
...
Aug 05, 2025 08:28:53 AM INFO: Final checkpoint: 600 total records, cursor: gAAAAABokgcVcNdfS1HrwnEMBZipc_HM9pMPN0wj7kv3VPUKXTXipraKaP0kRz4CQnsCFxhIMb3KoUc0m96TODUgyLVMCn_dZA%3D%3D

✓ Debug completed

Step 3: Querying sample data from DuckDB...
Running query: SELECT * FROM tester.hed_records ORDER BY record_id LIMIT 5;

┌──────────────────────┬──────────────────────┬───┬──────────────────────┬────────────────────┬─────────────────────┐
│      record_id       │ plagiarism_incidents │ … │ credit_hours_attem…  │  enrollment_date   │ credit_hours_earned │
│       varchar        │        int64         │   │        int64         │      varchar       │        int64        │
├──────────────────────┼──────────────────────┼───┼──────────────────────┼────────────────────┼─────────────────────┤
│ 003888f5-c339-4ab6…  │                    0 │ … │                   12 │ 2024-08-04 0:00:00 │                   8 │
│ 0052be6a-72eb-4835…  │                    1 │ … │                   13 │ 2024-08-09 0:00:00 │                  12 │
│ 00d602fd-dc15-4610…  │                    2 │ … │                   15 │ 2024-08-07 0:00:00 │                  11 │
│ 0181408b-20ab-4c1b…  │                    8 │ … │                   14 │ 2024-08-03 0:00:00 │                  12 │
│ 01b31fa2-9816-4c28…  │                    0 │ … │                   15 │ 2024-08-01 0:00:00 │                  14 │
├──────────────────────┴──────────────────────┴───┴──────────────────────┴────────────────────┴─────────────────────┤
│ 5 rows                                                                                       25 columns (5 shown) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

==================== OPERATION SUMMARY ====================
  Operation       | Calls     
  ----------------+------------
  Upserts         | 600       
  Updates         | 0         
  Deletes         | 0         
  Truncates       | 0         
  SchemaChanges   | 1         
  Checkpoints     | 8         
====================================================================

✓ Debug and reset operations completed.
Next sync state: {"next_cursor": "gAAAAABokgcVcNdfS1HrwnEMBZipc_HM9pMPN0wj7kv3VPUKXTXipraKaP0kRz4CQnsCFxhIMb3KoUc0m96TODUgyLVMCn_dZA%3D%3D"}
```

In this output, you can observe:

1. **Initial Setup**: The script detects the table name automatically from your connector code
2. **Reset Process**: Confirms reset was successful after user confirmation
3. **Debug Logging**: Detailed logs of the SDK initializing and making method calls
4. **Data Fetching**: Shows each API request with pagination parameters:
   - Starting with no cursor (first page)
   - Continuing with cursors for subsequent pages
   - Creating checkpoints after every 100 records
   - Terminating when no more pages are available
5. **Sample Data**: Displays the first 5 records from the extracted dataset to verify content
6. **Operation Summary**: Shows that 600 records were processed through:
   - 600 upsert operations
   - 1 schema change
   - 8 checkpoint operations (one per page of 100 records)
7. **Final State**: The cursor value that will be used in the next sync to retrieve only new records

### Deploying to Fivetran

1. Ensure you have a Fivetran account and destination configured

2. Make the deploy script executable and run it:
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

3. When prompted, enter:
   - Your Fivetran account name (default: MDS_SNOWFLAKE_HOL)
   - Your Fivetran destination name (default: NEW_SALES_ENG_HANDS_ON_LAB)
   - A unique name for your connector (e.g., hed_new_custom_connector)

4. The script will package and upload your connector to Fivetran

### Deploy Script Details

The `deploy.sh` script performs the following actions:

1. **Configuration Check**: Validates that the necessary configuration files are present
2. **Account Information**: Prompts for Fivetran account details and retrieves API key
3. **Deployment**: Packages and uploads the connector to your Fivetran account
4. **Confirmation**: Displays the connector ID and a link to access it in the Fivetran dashboard

Sample output from a successful deployment:
```
Deploying connector...
✓ Packaging your project for upload...
✓ Uploading your project...
The connection 'hed_new_custom_connector' has been created successfully.
Python Version: 3.12.8
Connection ID: student_retention_analysis
Visit the Fivetran dashboard to start the initial sync: https://fivetran.com/dashboard/connectors/student_retention_analysis/status
```

### What to Expect After Deployment

After successful deployment:

1. The connector will be created in your Fivetran account with a randomly generated Connection ID (e.g., `student_retention_analysis`)
2. You'll be provided a link to access the connector in the Fivetran dashboard
3. The connector will be ready for its initial sync

In the Fivetran dashboard, you will be able to:
- View your connector's sync status
- Configure sync frequency (hourly, daily, etc.)
- Monitor for any errors or warnings
- View the destination schema with the `hed_records` table in Snowflake
- Track the data volume and record count
- Configure schema and field mapping if needed

The first sync will extract all available data, while subsequent syncs will be incremental, only fetching new or changed records based on the stored cursor position.

## Maintenance and Troubleshooting

### Common Issues

- **API Key Issues**: Ensure your API key is valid and has the correct permissions
- **Network Connectivity**: Check that your network can reach the HED API endpoint
- **Schema Changes**: If the API changes its schema, you may need to update the connector
- **Cursor Issues**: If the cursor becomes invalid, reset the connector to perform a full sync
- **Memory Limitations**: For very large datasets, consider decreasing the page size

### Monitoring

- Check the Fivetran dashboard for sync status and errors
- Review logs in the Fivetran dashboard for detailed error information
- Use the debug script locally to test changes before deployment
- Monitor the timestamps of the most recent records to ensure data freshness

### Debug and Reset

If you encounter issues with the connector, you can use the debug_and_reset.sh script to:

1. Reset the connector state (start fresh)
2. Run in debug mode to see detailed logs
3. Inspect the sample data to verify correct data extraction
4. Check operation statistics to ensure proper functioning

The script outputs detailed information that can help diagnose issues:
- API request parameters and response details
- Error messages and stack traces
- Operation counts (upserts, updates, deletes, etc.)
- Checkpoint state information

## Advanced Customization

To extend or modify this connector:

### Adding New Fields

1. If the HED API adds new fields, you can capture them automatically. Since the connector uses the entire record for upserts, new fields will be included without schema changes.

2. To add transformations or derived fields:
   ```python
   # Example of adding a derived field in the update function
   for record in records:
       # Add a derived field based on existing data
       record['retention_risk_score'] = calculate_risk_score(record['current_gpa'], record['engagement_score'], record['at_risk_flag'])
       yield op.upsert("hed_records", record)
   ```

### Supporting Multiple Tables

1. Update the schema function to define additional tables:
   ```python
   def schema(configuration: dict):
       return [
           {
               "table": "hed_records",
               "primary_key": ["record_id"]
           },
           {
               "table": "hed_students",
               "primary_key": ["student_id"]
           }
       ]
   ```

2. Modify the update function to yield operations for multiple tables:
   ```python
   # Example: Creating student records from HED data
   student_records = {}
   for record in records:
       # Extract student info for separate table
       student_id = record['student_id']
       if student_id not in student_records:
           student_records[student_id] = {
               'student_id': student_id,
               'academic_standing': record['academic_standing'],
               'current_gpa': record['current_gpa'],
               'major_code': record['major_code'],
               'advisor_id': record['advisor_id']
           }
       # Yield the original record
       yield op.upsert("hed_records", record)
   
   # Yield all unique student records
   for student in student_records.values():
       yield op.upsert("hed_students", student)
   ```

### Dependencies Management

To add external dependencies:

1. Create a `requirements.txt` file in your project directory
2. Add your dependencies, for example:
   ```
   requests>=2.25.0
   python-dateutil>=2.8.1
   ```
3. The Fivetran deployment process will automatically install these dependencies in the connector environment

## Downstream Application: StudentSuccess

The data extracted by this connector powers StudentSuccess, an AI-driven freshman retention insights system designed for higher education institutions. The application is built as a Streamlit in Snowflake data application, utilizing both Fivetran and Cortex for data integration and analysis in higher education.

### Application Purpose

StudentSuccess helps higher education institutions:
- Improve freshman retention rates through predictive analytics
- Reduce time to identify at-risk students
- Minimize intervention costs through targeted support
- Increase student success through data-driven insights
- Transform reactive academic advising into proactive student support

### Target Users

- Academic Advisors
- Student Success Coordinators
- Enrollment Management Teams
- Provost and Vice President of Student Affairs
- Academic Success Analysts

### Data Flow Architecture

1. **Data Sources** → 2. **HED API** → 3. **Fivetran Custom Connector** → 4. **Snowflake Data Warehouse** → 5. **Streamlit in Snowflake Application**

### Simulated Higher Education Data Sources

The HED API provides synthetic data that simulates information from various higher education systems:

- **Student Information Systems**: 
  - Banner
  - PeopleSoft
  - Colleague
- **Learning Management Systems**: 
  - Canvas
  - Blackboard
  - Moodle
- **Academic Integrity Systems**: 
  - Turnitin
  - SafeAssign
- **Engagement Analytics**: 
  - BrightBytes
  - Civitas Learning

This approach allows for realistic higher education analytics without using protected student information, making it ideal for demonstration, development, and training purposes.

### Analytics Capabilities

The StudentSuccess application provides:
- Real-time analysis of student performance and engagement metrics
- AI-driven student success predictions and retention analytics
- Early intervention identification and recommendation systems
- Comprehensive dashboards for academic performance indicators
- Predictive analytics for student outcome forecasting
- Advanced agent workflows for transparent student success analysis

## Notes

- This connector creates a single table named `hed_records` in your Snowflake database
- The connector uses checkpoints to track sync progress, enabling resumable syncs
- The connector is configured to use cursor-based pagination for efficient data extraction
- During initial sync, all records will be loaded; subsequent syncs will only fetch new or updated records
- The HED API returns approximately 600 higher education student records in a complete dataset
- The connector handles paginated requests with a default of 100 records per page
- The data is optimized for use with Snowflake and the StudentSuccess higher education application
- While this connector uses synthetic data, the approach mirrors real-world higher education data extraction patterns