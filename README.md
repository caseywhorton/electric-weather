# heat-warning

Create an isolated environment

Requirements
```
Python 3.9.13
Flask 2.2.4
Werkzeug 2.2.3
```

## Project Planning

+ Identify data sources
+ Model data
+ Environment Set up
+ Data Preprocessing
+ Target identification
+ Model fitting
+ Hosting with web API
+ Model serving

## Data Source: Weather API

https://api.weather.gov
https://www.weather.gov/documentation/services-web-api

## Data Source: Electricity

https://www.eia.gov/opendata/browser/electricity

## CURL requests

Forecast: https://api.weather.gov/zones/Feature/OHZ055/forecast
Current Measurements: https://api.weather.gov/zones/forecast/OHZ055/observations
Electricity Price: "https://api.eia.gov/v2/electricity/retail-sales/data?api_key=<api_key>&frequency=monthly&data[0]=customers&data[1]=price&data[2]=revenue&data[3]=sales&facets[stateid][]=OH&start=2023-01&end=2023-05&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

# Services

+ AWS Secrets Manager
    + Maintains the API key secret
    + Get's rotated by AWS Lambda
+ AWS Lambda
    + Executes python script to get data from open APIs
    + Saves artifacts
+ AWS S3 or AWS DynamoDB
    + Storage for data
+ AWS Sagemaker
    + Studio
    + Pipelines
+ AWS SAM
+ AWS CI/CD tools
    + Code pipeline

## S3 directories

- cw-weather-data
    - observations
    - forecasts
- cw-sagemaker-domain-1

## EventBridge

## SNS Subscriptions

## CloudWatch Alarms

## Elastic Container Registry (ECR)

## Sagemaker

### Domain
### User Profiles

+ Data Scientist
+ ML Ops Engineer
