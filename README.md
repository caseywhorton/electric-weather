# heat-warning

Create an isolated environment

Requirements
```
Python 3.9.13
Flask 2.2.4
Werkzeug 2.2.3
```

## Docker

`~ % docker build -t <> -f ./ml_preprocessing_dockerfile . --platform=linux/amd64`

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

- Forecast: https://api.weather.gov/zones/Feature/OHZ055/forecast
- Current Measurements: https://api.weather.gov/zones/forecast/OHZ055/observations
- Electricity Price: "https://api.eia.gov/v2/electricity/retail-sales/data?api_key=<api_key>&frequency=monthly&data[0]=customers&data[1]=price&data[2]=revenue&data[3]=sales&facets[stateid][]=OH&start=2023-01&end=2023-05&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

# ETL

1. Get data from API (source data), save to S3 (lambda)
2. Preprocess files on S3, save to `raw` data folder on S3 (lambda)
3. Preprocess raw data on S3 and split into train and test data
4.
5.

# Services

+ AWS Secrets Manager
    + Maintains the API key secret
    + Gets rotated by AWS Lambda
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
        - <zone>_<date>.json
- cw-sagemaker-domain-1
    - deep_ar
        - data
            - raw
        - output
            - model
                - output
                    - <model artifact>
                    - predictions
- cw-sagemaker-domain-2
    - deep_ar
        - data
            - train
            - test

## EventBridge

## SNS Subscriptions

Email subscription for budget.


## CloudWatch Alarms

Lambda function alarm for function error.

## Lambda Function

+ Need to add a Layer for the pandas function.
+ Each function is triggered by the previous function and writes logs to cloudwatch.

get_weather_data -> preprocess_weather_data -> train_test_split_weather_data

**get_weather_data** : Gets the data from the API request.
**preprocess_weather_data** : Preprocessing the API request JSON data into just feature data.
**train_test_split_weather_data** : Gathers preprocessed data and creates a training and test dataset.
Each training set needs >= 300 observations
Each observation is roughtly 1 hour, so we keep 24 observations as a hold-out set for testing.


## Elastic Container Registry (ECR)

## Sagemaker

### Domain

A single domain with Sagemaker Studio.

### User Profiles

+ Data Scientist
+ ML Ops Engineer

# Data Model

**predictions**
| column/attribute | data type | description | units |
|------------------|-----------|-------------|-------|
| id | hash | unique id for predictions | None |


**humidity_data**

| column/attribute | data type | description | units |
|------------------|-----------|-------------|-------|
| measurement_dte | date | The date of the measurement. | None |
| measurement_stp | timestamp | The time of the measurement. | None |
| relative_humidity | decimal (4,2) | The relative humidity. | |
| wind_speed | <need this> | Wind speed. | |
| elevation | add this | The elevation of the station taking the measurement. | |
| precipitation_last_hr | integer | The amount of precipitation in the last hour from the measurement time. | |
| temperature | decimal (5,2) | The temperature of the air. | |
| cloud_layers | char(3) | The type of cloud layers. | |

# Model Training

JSON Document Format for Deep AR:

{"start":<timestamp>, "relative_humidity": [...]}
{"start":<timestamp>, "wind_speed": [...]}

In order to train the deep AR model, we need at least 300 observations. A single day of observations from a single station has about 72 observations. So, we need several days of observations.

# Deployment

For deployment, I am working with two options to explore how each operate:

+ Batch Transform
+ Real Time Endpoint

## Batch Transform

For this deployment, an inference pipeline will be created to run 24 hours of predictions from a trained model.

### CI/CD

- change
- Add a feature to the model
    - Retrain the model and use new model artifact
    -

### Model Evaluation

The documentatio on the DeepAR input/output reveals the metric used to evaluate the model during training. The root mean squared error (RMSE) is calculated over all of the series that are being evaluated, and the formula is a little different than the usual RMSE calculated over a single set of predictions:

$ RMSE = \sqrt{\frac{1}{nT}*\sum_{i,t}(\hat{y}_{i,t}-y_{i,t})^2} $

## Real Time Endpoint

For this deployment, an endpoint will be made prevalent and available for a 24 hour period for making predictions. Each prediction will predict the next hour's target value.

- Import Docker Image
- Create Model Endpoint Configuration
- Create Model Endpoint ([Single Model)](https://docs.aws.amazon.com/sagemaker/latest/dg/realtime-endpoints-deployment.html))
    - Capture Data (input)
- Run inference on 1 hour basis
- Capture Data (output)
- Delete Model Endpoint

### CI/CD

- Change the S3 location of saved data
- Add a feature to the input dataset
    - Retrain the model and switch to new model artifact
