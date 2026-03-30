# AeroInsights Project

A scalable data ingestion pipeline for aviation and weather analytics using AWS serverless technologies.

## Overview

The AeroInsights project is designed to collect, process, and store real-time flight and weather data from external APIs. It leverages AWS Lambda, Step Functions, EventBridge Scheduler, and S3 to create a robust data lake architecture with bronze and silver layers.

### Key Features
- **Flight Data Ingestion**: Fetch live flight information using the Aviationstack API.
- **Weather Data Collection**: Retrieve weather forecasts from Open-Meteo for specified locations.
- **Serverless Architecture**: Fully managed AWS services for scalability and cost-efficiency.
- **Data Lake Structure**: Organized S3 storage with bronze (raw) and silver (processed) layers.

## Architecture

### Components
1. **AWS Lambda**: Serverless function to fetch and process data from APIs.
2. **Step Functions**: Orchestrates the ingestion workflow.
3. **EventBridge Scheduler**: Triggers the pipeline on a scheduled basis.
4. **S3 Buckets**: Stores raw (bronze) and processed (silver) data.
5. **Secrets Manager**: Securely stores API keys.

### Data Flow
1. The EventBridge Scheduler triggers the Step Function at specified intervals.
2. The Step Function invokes the Lambda function to fetch flight and weather data.
3. Data is uploaded to the bronze S3 bucket in a structured format.
4. (Future) Processed data will be stored in the silver S3 bucket.

## Setup Instructions

### Prerequisites
- AWS account with appropriate permissions.
- Terraform installed on your local machine.
- Python 3.12 for local testing.

### Deployment
1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/aeroinsights.git
   cd aeroinsights
   ```

2. Configure Terraform:
   - Copy the `terraform.tfvars.example` file to `terraform.tvfars`.
   - Update the variables in `terraform.tfvars` with your configuration.

3. Deploy the infrastructure:
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

### Configuration
- **Environment Variables**: Configured via Terraform variables in `main.tf`.
- **API Keys**: Store sensitive data like API keys in AWS Secrets Manager.

## Testing the Solution

### Local Testing
1. Install dependencies:
   ```bash
   pip install requests boto3
   ```

2. Run the Lambda function locally:
   - Update environment variables in `handler.py` to match your local setup.
   - Test with sample data:
     ```python
     event = {
         "locations": [
             {"name": "Zurich", "lat": 47.3667, "lon": 8.5500}
         ],
         "fetch_flights": True,
         "max_pages": 1
     }

     from handler import lambda_handler
     response = lambda_handler(event, None)
     print(response)
     ```

### Mock Data Testing
- Use the `use_mock` flag in the event to test without hitting API rate limits:
  ```python
  event = {"use_mock": True}
  ```

### Integration Testing
1. Deploy the infrastructure using Terraform.
2. Manually trigger the Step Function via AWS Console or CLI.
3. Verify data in S3 buckets and CloudWatch logs.

## Triggering the Pipeline Manually

### Triggering the Lambda Function Directly
You can manually invoke the Lambda function using the AWS CLI:

```bash
aws lambda invoke  \
--function-name airoinsights-ingestor  \
--payload '{"fetch_flights": true, "use_mock": false, "locations": [{"name": "zurich", "lat": 47.3769, "lon": 8.5417}, {"name": "london", "lat": 51.5074, "lon": -0.1278}]}'  \
--cli-binary-format raw-in-base64-out response.json && cat response.json
```

### Triggering the Step Function Manually
You can manually start the Step Function execution using the AWS CLI:

```bash
aws stepfunctions start-execution \
    --state-machine-arn arn:aws:states:REGION:ACCOUNT_ID:stateMachine:airoinsights-ingestor \
    --input '{
        "locations": [
            {"name": "Zurich", "lat": 47.3667, "lon": 8.5500},
            {"name": "London", "lat": 51.5074, "lon": -0.1278}
        ],
        "fetch_flights": true,
        "max_pages": 1
    }'
```

## Data Structure

### S3 Paths
- **Bronze Layer**: `s3://{bronze-bucket}/raw/{source}/city={city}/date={YYYY-MM-DD}/{source}_{timestamp}.json`
- **Silver Layer**: (Future) Processed and enriched data.

### Sample Data Format
#### Flight Data
```json
{
  "source": "aviationstack_flights",
  "ingested_at_utc": "2024-01-01T12:00:00Z",
  "metadata": {
    "city": "zurich",
    "flight_count": 5,
    "pages_fetched": 1
  },
  "payload": [
    {
      "departure": {"iata": "ZRH", "airport": "Zurich"},
      "arrival": {"iata": "LHR", "airport": "Heathrow"},
      "flight": {"iata": "LX316"},
      "flight_status": "active"
    }
  ]
}
```

#### Weather Data
```json
{
  "source": "open_meteo_weather",
  "ingested_at_utc": "2024-01-01T12:00:00Z",
  "metadata": {
    "location": "Zurich",
    "lat": 47.3667,
    "lon": 8.5500
  },
  "payload": {
    "latitude": 47.3667,
    "longitude": 8.5500,
    "hourly": {
      "temperature_2m": [10.5, 10.8],
      "relative_humidity_2m": [68, 69]
    }
  }
}
```

## Monitoring and Logging
- **CloudWatch Logs**: Lambda function logs are stored in CloudWatch.
- **S3 Access Logs**: Enable S3 access logging for audit purposes.

## Troubleshooting
### Common Issues
1. **API Rate Limits**: Use mock data or reduce `max_pages` in the event.
2. **Permission Errors**: Ensure the Lambda role has permissions to write to S3 and read from Secrets Manager.
3. **Timeouts**: Increase the Lambda timeout if API responses are slow.

### Debugging Tips
- Check CloudWatch logs for detailed error messages.
- Verify S3 bucket names and paths in Terraform variables.
- Ensure API keys are correctly stored in Secrets Manager.