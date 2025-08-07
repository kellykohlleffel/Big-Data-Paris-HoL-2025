# PHR Pharma Fivetran Custom Connector

This connector uses the Fivetran Connector SDK to extract Pharmaceutical Research (PHR) data from an API and load it into Snowflake via Fivetran, powering advanced clinical trial optimization and pharmaceutical research analytics applications.

## Overview

The PHR Pharma connector fetches clinical trial records from a REST API and loads them into a single table called `phr_records` in your Snowflake database. The connector retrieves detailed information about clinical trials, patient enrollment, protocol data, and regulatory status. It handles authentication, pagination, error handling, and maintains state between sync runs using a cursor-based approach.

## Features

- **Incremental syncs**: Uses cursor-based pagination to efficiently process data in chunks
- **State management**: Tracks sync progress and saves checkpoints every 100 records
- **Error handling**: Gracefully handles API and runtime errors with detailed logging
- **Configuration management**: Customizable API endpoint, page size, and authentication
- **Minimal permissions**: Requires only an API key to access the PHR data
- **Data enrichment**: Extracts over 17 metrics and attributes related to clinical trials and patient data
- **Efficient processing**: Handles large datasets (600+ records) with minimal resource usage
- **Pharmaceutical-specific**: Designed to handle clinical trial data common in pharmaceutical operations
- **Robust retry logic**: Implements exponential backoff and rate limiting handling
- **Safety limits**: Maximum iteration protection to prevent infinite loops

## Project Structure

```
phr/
├── configuration.json     # Configuration parameters for the connector
├── connector.py           # Main connector implementation
├── debug_and_validate.sh  # Script for testing and validating the connector
├── deploy.sh              # Script for deploying the connector to Fivetran
└── requirements.txt       # Python dependencies (if any)
```

## Configuration Parameters

The connector uses the following configuration parameters:

| Parameter    | Description                                            | Default Value                                             |
|--------------|--------------------------------------------------------|-----------------------------------------------------------|
| `api_key`    | API key for authentication to the PHR API              | Required                                                  |
| `base_url`   | Base URL for the PHR API                               | Required (no default)                                     |
| `page_size`  | Number of records to fetch per API request             | `100`                                                     |

## PHR API Details

### Endpoint

The connector uses the following REST API endpoint to access pharmaceutical clinical trial data:

- `GET /phr_data`: Retrieves a paginated list of clinical trial records

### Query Parameters

- `cursor`: The cursor for the next page of results (used for pagination)
- `page_size`: The number of records to return per page (1-200, defaults to 100)

### Authentication

The API uses API key authentication, with the key passed in the request header:

```
x-api-key: YOUR_API_KEY
```

### Data Schema

The PHR API returns records with the following structure:

| Field                        | Type      | Description                                   |
|------------------------------|-----------|-----------------------------------------------|
| `record_id`                  | string    | Unique identifier for the record (primary key) |
| `trial_id`                   | string    | Unique identifier for the clinical trial      |
| `protocol_id`                | string    | Protocol identifier for the trial            |
| `trial_name`                 | string    | Descriptive name of the clinical trial       |
| `sponsor_name`               | string    | Name of the trial sponsor organization        |
| `disease_area`               | string    | Medical specialty (e.g., Dermatology, Ophthalmology, Hematology) |
| `patient_id`                 | string    | Unique identifier for the patient             |
| `patient_age`                | integer   | Patient's age at enrollment                   |
| `patient_gender`             | string    | Patient's gender                              |
| `enrollment_date`            | string    | Date when patient was enrolled in the trial  |
| `site_id`                    | string    | Unique identifier for the clinical trial site |
| `site_name`                  | string    | Name of the clinical trial site              |
| `regulatory_approval_status` | string    | Current regulatory approval status            |
| `trial_status`               | string    | Current status of the clinical trial         |
| `protocol_amendment_date`    | string    | Date of the most recent protocol amendment   |
| `enrollment_rate`            | float     | Patient enrollment rate for the trial        |
| `dropout_rate`               | float     | Patient dropout rate for the trial           |

## Implementation Details

### Schema Definition

The connector defines a simple schema with a single table `phr_records` and specifies `record_id` as the primary key.

```python
def schema(configuration: dict):
    # Return minimal schema with ONLY table name and primary key
    return [
        {
            "table": "phr_records",
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
6. **Checkpoint creation**: Saves checkpoints after every pagination batch to ensure reliability
7. **Error handling**: Catches and logs API exceptions and unexpected errors
8. **Safety mechanisms**: Includes maximum iteration limits and exponential backoff retry logic

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
   mkdir phr && cd phr
   ```

2. Create the required files:
   - `configuration.json`: Configure your API credentials and settings
   - `connector.py`: Implement the connector using the Fivetran SDK
   - `debug_and_validate.sh`: Script for testing
   - `deploy.sh`: Script for deployment

3. Make the scripts executable and run the debug script:
   ```bash
   chmod +x debug_and_validate.sh
   ./debug_and_validate.sh
   ```

   This will:
   - Reset the connector state
   - Run the connector in debug mode
   - Display sample data from the extracted records
   - Show operation statistics

### Debug Script Details

The `debug_and_validate.sh` script includes the following steps:

1. **Validation**: Checks for required files and extracts the table name
2. **Reset**: Clears previous state and data using `fivetran reset`
3. **Debug**: Runs the connector in debug mode with the specified configuration
4. **Query**: Executes a sample query against the extracted data using DuckDB
5. **Summary**: Displays a summary of operations performed (upserts, checkpoints, etc.)

When you run the debug script, you'll see output similar to the following:

```
✓ Detected table name: phr_records
===========================================================
         Fivetran Connector Debug & Reset Script          
===========================================================

This will reset your connector, delete current state and warehouse.db files.
Do you want to continue? (Y/N): y

Step 1: Resetting Fivetran connector...
This will delete your current state and `warehouse.db` files. Do you want to continue? (Y/n): Aug 07, 2025 07:45:10 AM INFO Fivetran-Connector-SDK: Reset Successful 

✓ Reset successful
Step 2: Running debug with configuration...
(Real-time output will be displayed below)

Aug 07, 2025 07:45:11 AM INFO Fivetran-Connector-SDK: Debugging connector at: /Users/kelly.kohlleffel/Documents/GitHub/Fivetran_Connector_SDK/examples/quick_start_examples/phr 
Aug 07, 2025 07:45:11 AM INFO Fivetran-Connector-SDK: Running connector tester... 
Aug 07, 2025 07:45:11 AM: INFO Fivetran-Tester-Process: Version: 2.25.0701.001 
Aug 07, 2025 07:45:11 AM: INFO Fivetran-Tester-Process: Destination schema: /Users/kelly.kohlleffel/Documents/GitHub/Fivetran_Connector_SDK/examples/quick_start_examples/phr/files/warehouse.db/tester 
Aug 07, 2025 7:45:12 AM com.fivetran.partner_sdk.v2.client.connector.SdkConnectorClient configurationForm
INFO: Fetching configuration form from partner
Aug 07, 2025 07:45:12 AM: INFO Fivetran-Tester-Process: Previous state:
{} 
Aug 07, 2025 07:45:12 AM INFO Fivetran-Connector-SDK: Initiating the 'schema' method call... 
Aug 07, 2025 07:45:12 AM: INFO Fivetran-Tester-Process: [SchemaChange]: tester.phr_records 
Aug 07, 2025 07:45:12 AM INFO Fivetran-Connector-SDK: Initiating the 'update' method call... 
Aug 07, 2025 07:45:12 AM INFO: Starting initial sync 
Aug 07, 2025 07:45:13 AM INFO: Checkpoint at 100 records, cursor: gAAAAABolJ_YXFITnueF9jOeGtpKqWOuGIuJMoELHYFMBtcg74CLIg5FmhH8rTU8esCiS-i58ZErX3TXdj0_kc-C3jRH09D-ft513Lmgc9KUuDx8pdKD4yF05xXDkMjJ6BojfjaOVV_V 
Aug 07, 2025 07:45:13 AM INFO: Processed batch: 100 records, has_more: True 
Aug 07, 2025 07:45:13 AM INFO: Checkpoint at 200 records, cursor: gAAAAABolJ_ZELo6y_PmVp0wN7f7oF1jfPykIJ7RipWZmvzW1TiQr9TW5Mbhi9hEjUtVzzS4Y5faJ3QMgtJoAK8qN9brTPMcB-FuSjYLyWaE2FGH49OuXbVl9M2APd6UiehUT_FDFOhk 
Aug 07, 2025 07:45:13 AM INFO: Processed batch: 100 records, has_more: True 
Aug 07, 2025 07:45:13 AM INFO: Checkpoint at 300 records, cursor: gAAAAABolJ_ZVg01CUaDnkRupT1TKbEDydnQJmsmx6cM4VjnVIWRRPGBtYmDMm-QMRbg069dPIlNIhQATdH7LVQDu-4H0W2fRqaPVSWc6pWUnF6kLnsm3M03ne0WDtu39cX9lW18DC5R 
Aug 07, 2025 07:45:13 AM INFO: Processed batch: 100 records, has_more: True 
Aug 07, 2025 07:45:13 AM INFO: Checkpoint at 400 records, cursor: gAAAAABolJ_ZCPByBd-a1wGc6rfhR2GKSbfq40YaqoWtyWzvBJ-vuaGtNYaM0dCnpn-Fgbp_EKsjKtfqjnXK0vmn-uAI0Gzcjgpq6GsstOOgLUZLs-lDebT8WzkdYg7HIAfuNNI69a6c 
Aug 07, 2025 07:45:13 AM INFO: Processed batch: 100 records, has_more: True 
Aug 07, 2025 07:45:13 AM INFO: Checkpoint at 500 records, cursor: gAAAAABolJ_Zyji1ueqNrfmkAqjzVA6oJW9ZnzOnPz3YdcDCpEoVsReBMsfKviX-udR_S0ZuqiyEniRtM6BYnYxIi75a5_AN7AXNs1qStE7rCYL6oXzqa4ZOMMibenmEI7QfJxfYHXrj 
Aug 07, 2025 07:45:13 AM INFO: Processed batch: 100 records, has_more: True 
Aug 07, 2025 07:45:13 AM INFO: Checkpoint at 600 records, cursor: gAAAAABolJ_Z-A6RWK5IE-EynNE3YH8TF3eYvSyxiJqxPEzR2F9Rz8R25SNv0_MqZoIKw6Zbi5deFFb8pA4B9gFDza2qYhJKTmDtvlQXUgW2nGjuQM37SmBAQCsoOrIi-hw4hNFsl2kY 
Aug 07, 2025 07:45:13 AM INFO: Processed batch: 100 records, has_more: True 
Aug 07, 2025 07:45:13 AM INFO: Checkpoint at 600 records, cursor: gAAAAABolJ_ZcIMsGrFepiHC6FBt8yEP8V15MFzJNS4QKUgCBAnMGwXAlHlsg-Rdb3gKmRuAtoJd1ossPehgV0R9jMxXSWWLtw%3D%3D 
Aug 07, 2025 07:45:13 AM INFO: Processed batch: 0 records, has_more: False 
Aug 07, 2025 07:45:13 AM INFO: Final checkpoint: 600 total records, cursor: gAAAAABolJ_ZcIMsGrFepiHC6FBt8yEP8V15MFzJNS4QKUgCBAnMGwXAlHlsg-Rdb3gKmRuAtoJd1ossPehgV0R9jMxXSWWLtw%3D%3D 
Aug 07, 2025 07:45:35 AM: INFO Fivetran-Tester-Process: Checkpoint: {"next_cursor": "gAAAAABolJ_YXFITnueF9jOeGtpKqWOuGIuJMoELHYFMBtcg74CLIg5FmhH8rTU8esCiS-i58ZErX3TXdj0_kc-C3jRH09D-ft513Lmgc9KUuDx8pdKD4yF05xXDkMjJ6BojfjaOVV_V"} 
Aug 07, 2025 07:45:57 AM: INFO Fivetran-Tester-Process: Checkpoint: {"next_cursor": "gAAAAABolJ_ZELo6y_PmVp0wN7f7oF1jfPykIJ7RipWZmvzW1TiQr9TW5Mbhi9hEjUtVzzS4Y5faJ3QMgtJoAK8qN9brTPMcB-FuSjYLyWaE2FGH49OuXbVl9M2APd6UiehUT_FDFOhk"} 
Aug 07, 2025 07:46:19 AM: INFO Fivetran-Tester-Process: Checkpoint: {"next_cursor": "gAAAAABolJ_ZVg01CUaDnkRupT1TKbEDydnQJmsmx6cM4VjnVIWRRPGBtYmDMm-QMRbg069dPIlNIhQATdH7LVQDu-4H0W2fRqaPVSWc6pWUnF6kLnsm3M03ne0WDtu39cX9lW18DC5R"} 
Aug 07, 2025 07:46:41 AM: INFO Fivetran-Tester-Process: Checkpoint: {"next_cursor": "gAAAAABolJ_ZCPByBd-a1wGc6rfhR2GKSbfq40YaqoWtyWzvBJ-vuaGtNYaM0dCnpn-Fgbp_EKsjKtfqjnXK0vmn-uAI0Gzcjgpq6GsstOOgLUZLs-lDebT8WzkdYg7HIAfuNNI69a6c"} 
Aug 07, 2025 07:47:03 AM: INFO Fivetran-Tester-Process: Checkpoint: {"next_cursor": "gAAAAABolJ_Zyji1ueqNrfmkAqjzVA6oJW9ZnzOnPz3YdcDCpEoVsReBMsfKviX-udR_S0ZuqiyEniRtM6BYnYxIi75a5_AN7AXNs1qStE7rCYL6oXzqa4ZOMMibenmEI7QfJxfYHXrj"} 
Aug 07, 2025 07:47:25 AM: INFO Fivetran-Tester-Process: Checkpoint: {"next_cursor": "gAAAAABolJ_Z-A6RWK5IE-EynNE3YH8TF3eYvSyxiJqxPEzR2F9Rz8R25SNv0_MqZoIKw6Zbi5deFFb8pA4B9gFDza2qYhJKTmDtvlQXUgW2nGjuQM37SmBAQCsoOrIi-hw4hNFsl2kY"} 
Aug 07, 2025 07:47:25 AM: INFO Fivetran-Tester-Process: Checkpoint: {"next_cursor": "gAAAAABolJ_ZcIMsGrFepiHC6FBt8yEP8V15MFzJNS4QKUgCBAnMGwXAlHlsg-Rdb3gKmRuAtoJd1ossPehgV0R9jMxXSWWLtw%3D%3D"} 
Aug 07, 2025 07:47:25 AM: INFO Fivetran-Tester-Process: Checkpoint: {"next_cursor": "gAAAAABolJ_ZcIMsGrFepiHC6FBt8yEP8V15MFzJNS4QKUgCBAnMGwXAlHlsg-Rdb3gKmRuAtoJd1ossPehgV0R9jMxXSWWLtw%3D%3D"} 
Aug 07, 2025 07:47:25 AM: INFO Fivetran-Tester-Process: SYNC PROGRESS:
Operation       | Calls     
----------------+------------
Upserts         | 600       
Updates         | 0         
Deletes         | 0         
Truncates       | 0         
SchemaChanges   | 1         
Checkpoints     | 8         
 
Aug 07, 2025 07:47:25 AM: INFO Fivetran-Tester-Process: Sync SUCCEEDED 

✓ Debug completed
Step 3: Querying sample data from DuckDB...

Running query: SELECT * FROM tester.phr_records ORDER BY record_id LIMIT 5;

┌──────────────────────┬───────────────┬──────────────┬───┬─────────────┬──────────────────────┬─────────────────┐
│      record_id       │ disease_area  │   trial_id   │ … │ patient_age │     sponsor_name     │ enrollment_date │
│       varchar        │    varchar    │   varchar    │   │    int64    │       varchar        │     varchar     │
├──────────────────────┼───────────────┼──────────────┼───┼─────────────┼──────────────────────┼─────────────────┤
│ 0076f67a-51c0-46ed…  │ Dermatology   │ TRIAL_000368 │ … │          44 │ BioTech Inc.         │ 2025-05-09      │
│ 00868cd9-2997-4b6a…  │ Ophthalmology │ TRIAL_000237 │ … │          38 │ Medical Device Man…  │ 2025-06-11      │
│ 00d2cc1c-fb7a-446d…  │ Hematology    │ TRIAL_000112 │ … │          37 │ Government Agency    │ 2025-02-28      │
│ 00ff9150-c9fb-4220…  │ Ophthalmology │ TRIAL_000406 │ … │          78 │ Contract Research …  │ 2024-09-01      │
│ 0175c7c4-a744-4515…  │ Orthopedics   │ TRIAL_000490 │ … │          49 │ Research Institute   │ 2025-07-22      │
├──────────────────────┴───────────────┴──────────────┴───┴─────────────┴──────────────────────┴─────────────────┤
│ 5 rows                                                                                    20 columns (6 shown) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

==================== OPERATION SUMMARY ====================
  Aug 07, 2025 07:47:25 AM: INFO Fivetran-Tester-Process: SYNC PROGRESS:
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
Next sync state: {"next_cursor": "gAAAAABolJ_ZcIMsGrFepiHC6FBt8yEP8V15MFzJNS4QKUgCBAnMGwXAlHlsg-Rdb3gKmRuAtoJd1ossPehgV0R9jMxXSWWLtw%3D%3D"} 
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
   - A unique name for your connector (e.g., phr_0807_claude_4_workbench)

4. The script will package and upload your connector to Fivetran

### Deploy Script Details

The `deploy.sh` script performs the following actions:

1. **Configuration Check**: Validates that the necessary configuration files are present
2. **Account Information**: Prompts for Fivetran account details and retrieves API key
3. **Deployment**: Packages and uploads the connector to your Fivetran account
4. **Confirmation**: Displays the connector ID and a link to access it in the Fivetran dashboard

Sample output from a successful deployment:
```
Enter your Fivetran Account Name [MDS_SNOWFLAKE_HOL]: 
Enter your Fivetran Destination Name [NEW_SALES_ENG_HANDS_ON_LAB]: 
Enter a unique Fivetran Connector Name [my_new_fivetran_custom_connector]: phr_0807_claude_4_workbench
Deploying connector...
Aug 07, 2025 07:49:18 AM INFO Fivetran-Connector-SDK: Deploying with parameters: Fivetran deploy --destination NEW_SALES_ENG_HANDS_ON_LAB --connection phr_0807_claude_4_workbench --api-key dmNmWU5W******** --configuration configuration.json  
Aug 07, 2025 07:49:18 AM INFO Fivetran-Connector-SDK: We support only `.py` files and a `requirements.txt` file as part of the code upload. *No other code files* are supported or uploaded during the deployment process. Ensure that your code is structured accordingly and all dependencies are listed in `requirements.txt` 
Aug 07, 2025 07:49:19 AM INFO Fivetran-Connector-SDK: Validation of requirements.txt completed. 
Aug 07, 2025 07:49:19 AM INFO Fivetran-Connector-SDK: Deploying '/Users/kelly.kohlleffel/Documents/GitHub/Fivetran_Connector_SDK/examples/quick_start_examples/phr' to connection 'phr_0807_claude_4_workbench' in destination 'NEW_SALES_ENG_HANDS_ON_LAB'.
 
Aug 07, 2025 07:49:19 AM INFO Fivetran-Connector-SDK: Packaging your project for upload... 
✓
Aug 07, 2025 07:49:19 AM INFO Fivetran-Connector-SDK: Uploading your project... 
✓
Aug 07, 2025 07:49:21 AM INFO Fivetran-Connector-SDK: The connection 'phr_0807_claude_4_workbench' has been created successfully.
 
Aug 07, 2025 07:49:21 AM INFO Fivetran-Connector-SDK: Python version 3.12 to be used at runtime. 
Aug 07, 2025 07:49:21 AM INFO Fivetran-Connector-SDK: Connection ID: huddling_comfy 
Aug 07, 2025 07:49:21 AM INFO Fivetran-Connector-SDK: Visit the Fivetran dashboard to start the initial sync: https://fivetran.com/dashboard/connectors/huddling_comfy/status 
```

### What to Expect After Deployment

After successful deployment:

1. The connector will be created in your Fivetran account with a randomly generated Connection ID (e.g., `huddling_comfy`)
2. You'll be provided a link to access the connector in the Fivetran dashboard
3. The connector will be ready for its initial sync

In the Fivetran dashboard, you will be able to:
- View your connector's sync status
- Configure sync frequency (hourly, daily, etc.)
- Monitor for any errors or warnings
- View the destination schema with the `phr_records` table in Snowflake
- Track the data volume and record count
- Configure schema and field mapping if needed

The first sync will extract all available data, while subsequent syncs will be incremental, only fetching new or changed records based on the stored cursor position.

## Maintenance and Troubleshooting

### Common Issues

#### API Authentication Errors (401)
**Symptoms:**
```
Aug 07, 2025 07:45:13 AM SEVERE: Authentication failed - check API key
```
**Solutions:**
- Verify your API key is correct in the `configuration.json` file
- Ensure the API key hasn't expired
- Check that the API key has the necessary permissions
- Contact your PHR API administrator

#### API Permission Errors (403)
**Symptoms:**
```
Aug 07, 2025 07:45:13 AM SEVERE: Access forbidden - check API permissions
```
**Solutions:**
- Verify your account has access to the PHR data endpoints
- Check with your API administrator about permission levels
- Ensure your organization has active access to the PHR system

#### Rate Limiting (429)
**Symptoms:**
```
Aug 07, 2025 07:45:13 AM WARNING: Rate limit hit, waiting 60 seconds
```
**Solutions:**
- The connector automatically handles rate limiting with a 60-second backoff
- Consider reducing the `page_size` parameter to make smaller requests
- Check if multiple connectors are accessing the same API simultaneously

#### Network Connectivity Issues
**Symptoms:**
```
Aug 07, 2025 07:45:13 AM SEVERE: API request failed: Connection timeout
```
**Solutions:**
- Verify network connectivity to the PHR API endpoint
- Check firewall settings that might block outbound API requests
- Validate the `base_url` configuration parameter
- Test the API endpoint manually using curl or similar tools

#### Data Quality Issues
**Symptoms:**
```
Aug 07, 2025 07:45:13 AM WARNING: Skipping record without record_id: {...}
```
**Solutions:**
- Some records may be missing the required `record_id` field
- This is handled gracefully by skipping the problematic records
- Monitor logs to track frequency of these issues
- Contact PHR support if data quality issues persist

#### Infinite Loop Prevention
**Symptoms:**
```
Aug 07, 2025 07:45:13 AM WARNING: Reached maximum iteration limit (200). Saving checkpoint and exiting.
```
**Solutions:**
- The connector has built-in safety limits to prevent infinite loops
- This typically indicates an issue with cursor pagination
- Check the API documentation for cursor behavior
- Reset the connector state if the issue persists

### Monitoring

- Check the Fivetran dashboard for sync status and errors
- Review logs in the Fivetran dashboard for detailed error information
- Use the debug script locally to test changes before deployment
- Monitor the timestamps of the most recent records to ensure data freshness
- Track the cursor progression to ensure proper incremental syncing

### Debug and Reset

If you encounter issues with the connector, you can use the `debug_and_validate.sh` script to:

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

1. If the PHR API adds new fields, you can capture them automatically. Since the connector uses the entire record for upserts, new fields will be included without schema changes.

2. To add transformations or derived fields:
   ```python
   # Example of adding a derived field in the update function
   for record in records:
       # Add a derived field based on existing data
       record['trial_risk_score'] = calculate_trial_risk(
           record['enrollment_rate'], 
           record['dropout_rate'], 
           record['protocol_amendment_date']
       )
       # Add enrollment efficiency metric
       if record['enrollment_rate'] and record['dropout_rate']:
           record['net_enrollment_efficiency'] = record['enrollment_rate'] - record['dropout_rate']
       
       yield op.upsert("phr_records", record)
   ```

### Supporting Multiple Tables

1. Update the schema function to define additional tables:
   ```python
   def schema(configuration: dict):
       return [
           {
               "table": "phr_records",
               "primary_key": ["record_id"]
           },
           {
               "table": "phr_trials",
               "primary_key": ["trial_id"]
           },
           {
               "table": "phr_sites",
               "primary_key": ["site_id"]
           }
       ]
   ```

2. Modify the update function to yield operations for multiple tables:
   ```python
   # Example: Creating trial and site records from PHR data
   trial_records = {}
   site_records = {}
   
   for record in records:
       # Extract trial info for separate table
       trial_id = record['trial_id']
       if trial_id not in trial_records:
           trial_records[trial_id] = {
               'trial_id': trial_id,
               'trial_name': record['trial_name'],
               'protocol_id': record['protocol_id'],
               'sponsor_name': record['sponsor_name'],
               'disease_area': record['disease_area'],
               'trial_status': record['trial_status'],
               'regulatory_approval_status': record['regulatory_approval_status']
           }
       
       # Extract site info for separate table
       site_id = record['site_id']
       if site_id not in site_records:
           site_records[site_id] = {
               'site_id': site_id,
               'site_name': record['site_name']
           }
       
       # Yield the original record
       yield op.upsert("phr_records", record)
   
   # Yield all unique trial records
   for trial in trial_records.values():
       yield op.upsert("phr_trials", trial)
   
   # Yield all unique site records
   for site in site_records.values():
       yield op.upsert("phr_sites", site)
   ```

### Dependencies Management

To add external dependencies:

1. Create a `requirements.txt` file in your project directory
2. Add your dependencies, for example:
   ```
   requests>=2.25.0
   python-dateutil>=2.8.1
   pandas>=1.3.0
   ```
3. The Fivetran deployment process will automatically install these dependencies in the connector environment

### Custom Error Handling

To add custom error handling for specific business logic:

```python
def validate_clinical_trial_data(record):
    """Custom validation for clinical trial records"""
    errors = []
    
    # Validate patient age is reasonable
    if record.get('patient_age', 0) < 0 or record.get('patient_age', 0) > 120:
        errors.append(f"Invalid patient age: {record.get('patient_age')}")
    
    # Validate enrollment date format
    try:
        from datetime import datetime
        datetime.strptime(record.get('enrollment_date', ''), '%Y-%m-%d')
    except ValueError:
        errors.append(f"Invalid enrollment date format: {record.get('enrollment_date')}")
    
    # Validate enrollment and dropout rates
    enrollment_rate = record.get('enrollment_rate', 0)
    dropout_rate = record.get('dropout_rate', 0)
    if enrollment_rate < 0 or enrollment_rate > 100:
        errors.append(f"Invalid enrollment rate: {enrollment_rate}")
    if dropout_rate < 0 or dropout_rate > 100:
        errors.append(f"Invalid dropout rate: {dropout_rate}")
    
    return errors

# Use in the update function
for record in records:
    validation_errors = validate_clinical_trial_data(record)
    if validation_errors:
        log.warning(f"Data quality issues in record {record.get('record_id')}: {validation_errors}")
    
    if 'record_id' in record:
        yield op.upsert("phr_records", record)
```

## Downstream Application: TrialGenius

The data extracted by this connector powers TrialGenius, an AI-powered clinical trial design and optimization system designed for pharmaceutical companies. The application is built as a Streamlit in Snowflake data application, utilizing both Fivetran and Cortex for data integration and analysis in pharmaceutical research.

### Application Purpose

TrialGenius helps pharmaceutical companies:
- Reduce clinical trial costs and timelines through AI-powered optimization
- Improve patient recruitment and retention rates
- Optimize protocol design based on historical trial data
- Minimize trial failure risk through predictive analytics
- Transform reactive trial management into proactive optimization
- Generate synthetic control arms for rare disease studies

### Business Challenge

Pharmaceutical companies face mounting pressure to reduce clinical trial costs and timelines while improving success rates. Traditional clinical trial design relies heavily on historical precedent and expert intuition, leading to suboptimal patient stratification, inefficient site selection, and protocol designs that fail to account for real-world variability. This results in 90% of clinical trials failing to meet enrollment timelines and $2.6 billion average cost per approved drug, with clinical trials representing 60-70% of total development costs.

### Target Users

- **Primary**: Chief Medical Officer, VP of Clinical Development, Clinical Operations Directors
- **Secondary**: Biostatisticians, Regulatory Affairs Directors, Clinical Data Managers
- **Tertiary**: Site Investigators, Clinical Research Associates, Patient Recruitment Specialists
- **Top C-Level Executive**: Chief Medical Officer (CMO)

### Key TrialGenius Features

- Intelligent protocol generation with AI-powered inclusion/exclusion criteria optimization
- Predictive patient recruitment modeling with site-specific enrollment forecasting
- Adaptive trial design recommendations based on interim data analysis
- Synthetic control arm generation for rare disease studies
- Real-time protocol amendment suggestions based on emerging data patterns
- Multi-scenario simulation engine for risk assessment and contingency planning

### Data Flow Architecture

1. **Data Sources** → 2. **PHR API** → 3. **Fivetran Custom Connector** → 4. **Snowflake Data Warehouse** → 5. **Streamlit in Snowflake Application**

### Simulated Pharmaceutical Data Sources

The PHR API provides synthetic data that simulates information from various pharmaceutical and clinical trial systems:

- **Clinical Trial Management Systems (CTMS)**:
  - Veeva Vault CTMS
  - Oracle Clinical One
  - Medidata Rave
- **Electronic Data Capture (EDC)**:
  - Medidata Rave EDC
  - Oracle Clinical One Data Collection
  - Veeva Vault EDC
- **Real-World Evidence Databases**:
  - Flatiron Health
  - IQVIA Real-World Data
  - Optum Clinformatics
- **Regulatory Databases**:
  - FDA Orange Book
  - EMA Clinical Data Publication Policy
  - ClinicalTrials.gov
- **Patient Registries**:
  - TriNetX
  - IBM Watson Health
  - Syapse
- **Genomic Databases**:
  - gnomAD
  - UK Biobank
  - All of Us Research Program

This approach allows for realistic pharmaceutical analytics without using protected patient information, making it ideal for demonstration, development, and training purposes.

### Expected Business Results

TrialGenius powered by this connector delivers:

- **40% reduction in trial failure risk**
  - 100 trials/year × 90% baseline failure rate × 40% reduction = 36 fewer failed trials/year
- **$520 million in development cost savings annually**
  - $2.6 billion average cost per drug × 20% cost reduction = $520 million savings per approved drug
- **30% improvement in patient enrollment timelines**
  - 18 months average enrollment time × 30% improvement = 5.4 months faster enrollment
- **50% reduction in protocol amendment frequency**
  - Average 3 amendments per trial × 50% reduction = 1.5 fewer amendments per trial

### Analytics Capabilities

The TrialGenius application provides:
- Real-time analysis of clinical trial performance and enrollment metrics
- AI-driven trial optimization and protocol design recommendations
- Predictive analytics for patient recruitment and retention forecasting
- Comprehensive dashboards for clinical trial performance indicators
- Advanced simulation capabilities for trial scenario modeling
- Intelligent protocol amendment suggestions based on emerging data patterns

### Competitive Advantage

TrialGenius differentiates from traditional clinical trial optimization through its generative AI capability to create novel trial designs rather than simply analyzing existing ones. Unlike conventional statistical modeling tools, it generates synthetic patient populations and simulates thousands of trial scenarios simultaneously, enabling pharmaceutical companies to identify optimal trial parameters before patient enrollment begins. This proactive approach reduces trial failure risk by 40% compared to reactive optimization methods.

## Performance Optimization

### Recommended Settings
- **Page Size**: 100-500 records per page (default: 100)
- **Sync Frequency**: Every 1-4 hours for active clinical trials
- **Checkpointing**: Automatic after each page of results
- **Maximum Iterations**: 200 iterations per sync (built-in safety limit)

### Monitoring Metrics
The connector provides detailed logging for:
- Sync progress and record counts
- Checkpoint positions and cursor values
- API response times and error rates
- Data quality issues (missing record IDs)
- Retry attempts and backoff timing
- Memory usage and processing efficiency

### Performance Considerations

- The connector processes approximately 600 records in a complete dataset
- Each sync creates 8 checkpoints for 600 records (100 records per checkpoint)
- Exponential backoff retry logic prevents API overload
- Cursor-based pagination ensures efficient incremental syncing
- Built-in rate limiting handling maintains API compliance

## Error Handling and Recovery

### Automatic Recovery Features

The connector includes several automatic recovery mechanisms:

1. **Exponential Backoff**: Retries failed requests with increasing delays (2^attempt seconds)
2. **Rate Limit Handling**: Automatically waits 60 seconds when hitting rate limits
3. **Checkpoint Recovery**: Resumes from last successful checkpoint on restart
4. **Request Retry Logic**: Attempts each request up to 3 times before failing
5. **Iteration Limits**: Prevents infinite loops with maximum 200 iterations per sync

### Manual Recovery Options

If automatic recovery fails:

1. **Reset Connector State**: Use the debug script to clear state and start fresh
2. **Reduce Page Size**: Lower the `page_size` parameter to reduce request load
3. **Check Configuration**: Verify all configuration parameters are correct
4. **Contact Support**: Reach out to PHR API support for persistent issues

## Security Considerations

### API Key Management
- Store API keys securely in the Fivetran configuration
- Rotate API keys regularly according to your organization's security policy
- Never commit API keys to version control systems
- Use environment-specific API keys for development vs production

### Data Privacy
- The connector handles clinical trial data which may contain sensitive information
- Ensure compliance with relevant regulations (HIPAA, GDPR, etc.)
- Implement appropriate data governance policies
- Monitor data access and usage patterns

### Network Security
- Use HTTPS endpoints for all API communications
- Implement network security controls as required by your organization
- Consider using VPN or private network connections for sensitive data
- Monitor network traffic for unusual patterns

## Business Use Cases

This connector enables several key pharmaceutical analytics use cases:

### Clinical Trial Optimization
- **Patient Recruitment Analysis**: Identify optimal sites and patient populations based on historical enrollment data
- **Protocol Design**: Analyze historical trial data to improve future protocol designs and reduce amendment frequency
- **Risk Assessment**: Predict trial failure risks based on enrollment patterns, dropout rates, and site performance
- **Site Selection**: Evaluate site performance metrics to optimize future trial site selection

### Regulatory Compliance
- **Audit Trails**: Complete history of protocol amendments and regulatory approvals
- **Reporting**: Automated regulatory reporting and compliance monitoring
- **Data Integrity**: Validated data pipelines for regulatory submissions
- **Amendment Tracking**: Monitor protocol amendment frequency and timing patterns

### Operational Analytics
- **Enrollment Forecasting**: Predict patient enrollment timelines based on historical patterns
- **Cost Optimization**: Analyze trial costs and identify opportunities for efficiency improvements
- **Resource Planning**: Optimize resource allocation across multiple concurrent trials
- **Performance Benchmarking**: Compare trial performance against industry standards

### Real-World Evidence Integration
- **Outcomes Analysis**: Compare trial results with real-world patient outcomes
- **Safety Monitoring**: Post-market surveillance and adverse event tracking
- **Market Access**: Evidence generation for payer negotiations and market access strategies
- **Comparative Effectiveness**: Analyze trial data against real-world treatment patterns

## Notes

- This connector creates a single table named `phr_records` in your Snowflake database
- The connector uses checkpoints to track sync progress, enabling resumable syncs
- The connector is configured to use cursor-based pagination for efficient data extraction
- During initial sync, all records will be loaded; subsequent syncs will only fetch new or updated records
- The PHR API returns approximately 600 clinical trial records in a complete dataset
- The connector handles paginated requests with a default of 100 records per page
- The data is optimized for use with Snowflake and the TrialGenius pharmaceutical application
- While this connector uses synthetic data, the approach mirrors real-world pharmaceutical data extraction patterns
- The connector includes comprehensive error handling for common pharmaceutical API scenarios
- Built-in safety mechanisms prevent infinite loops and handle rate limiting automatically
- Supports Python 3.12 runtime environment in Fivetran cloud infrastructure

## Version History

- **v1.0.0**: Initial release with basic PHR clinical trial data extraction
- **Future Enhancements**: 
  - Enhanced error handling for specific pharmaceutical data scenarios
  - Additional data sources integration (genomic databases, patient registries)
  - Real-time streaming capabilities for active trial monitoring
  - Advanced data transformation and enrichment features

## Support and Resources

For technical support:
- **Connector Issues**: Contact Fivetran Support
- **API Issues**: Contact PHR Support team
- **Data Questions**: Consult PHR API documentation
- **Business Logic**: Review TrialGenius application documentation

### Additional Resources
- Fivetran Connector SDK Documentation
- PHR API Reference Guide
- Clinical Trial Data Management Best Practices
- Pharmaceutical Data Governance Guidelines