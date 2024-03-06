import json
import urllib.parse
import boto3
import pandas as pd
from utils.preprocessing import *
import datetime
from io import StringIO  # python3; python2: BytesIO
import io
from datetime import datetime, timezone, timedelta, date
import os

print("Loading function")

def lambda_handler(event, context):
    try:
        # Initialize AWS clients
        client = boto3.client("sagemaker")
        s3 = boto3.client("s3")

        # Get model name from environment variable
        model_name = os.environ.get("MODEL_NAME")
        print("Model name : {}".format(model_name))

        # Define S3 bucket and prefix
        bucket = "cw-sagemaker-domain-1"
        prefix = "deep_ar/data/raw"

        # Get current and lagged timestamps
        today = datetime.now(timezone.utc)
        lag_365 = datetime.now(timezone.utc) + timedelta(days=-365)

        # List objects in the S3 bucket
        objects = s3.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]

        # Read data files modified within the last year
        df_list = []
        for o in objects:
            if lag_365 <= o["LastModified"] <= today:
                obj = s3.get_object(Bucket=bucket, Key=o["Key"])
                df_list.append(pd.read_csv(obj["Body"]))

        # Concatenate dataframes, drop duplicates, and reset index
        preprocessed_df = pd.concat(df_list).drop_duplicates().reset_index()

        # Get start timestamp and its string representation
        start = getStart(preprocessed_df)
        start_str = getStartString(preprocessed_df)

        # Define features and preprocess them
        features = ["properties.relativeHumidity.value", "properties.temperature.value"]
        mylist = []
        for feature in features:
            mylist.append(
                featureDict(
                    start_str,
                    columnNameReformat(feature, "properties.relativeHumidity.value"),
                    preprocessQuant(preprocessed_df[feature]),
                )
            )

        # Convert feature dictionaries to time series
        time_series = [dict_to_series(i) for i in mylist]

        # Write time series data to a JSON lines file
        encoding = "utf-8"
        file_name = "serving.json"
        with open("/tmp/" + file_name, "wb") as f:
            for i, ts in enumerate(time_series):
                f.write(
                    series_to_jsonline(
                        ts, feature_name=list(mylist[i].keys())[1]
                    ).encode(encoding)
                )
                f.write("\n".encode(encoding))

        # Copy the JSON lines file to S3
        copy_to_s3(
            "/tmp/" + file_name,
            "s3://cw-weather-data-deployment/serving/" + file_name,
            override=True,
        )

        # Create a timestamp for the transform job
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        # Create the TransformJobName with a timestamp
        transform_job_name = f"WeatherBatchTransform-{timestamp}"

        # Create a dictionary containing start and start_str values
        metadata_dict = {
            "start": start.strftime("%Y-%m-%d %H:%M:%S"),
            "start_str": start_str
        }

        # Convert dictionary to JSON format
        metadata_json = json.dumps(metadata_dict)

        # Write metadata JSON to a file
        metadata_file_name = "metadata.json"
        with open("/tmp/" + metadata_file_name, "w") as f:
            f.write(metadata_json)

        # Copy the metadata file to S3
        copy_to_s3(
            "/tmp/" + metadata_file_name,
            "s3://cw-weather-data-deployment/metadata/" + metadata_file_name,
            override=True,
        )

        # Create a SageMaker transform job
        response = client.create_transform_job(
            TransformJobName=transform_job_name,
            ModelName=model_name,
            MaxConcurrentTransforms=1,
            ModelClientConfig={
                "InvocationsTimeoutInSeconds": 600,
                "InvocationsMaxRetries": 3,
            },
            MaxPayloadInMB=30,
            BatchStrategy="SingleRecord",
            TransformInput={
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": "s3://cw-weather-data-deployment/serving/serving.json",
                    }
                },
                "ContentType": "application/jsonlines",
                "CompressionType": "None",
                "SplitType": "None",
            },
            TransformOutput={
                "S3OutputPath": "s3://cw-weather-data-deployment/forecasts/",
                "Accept": "application/jsonlines",
                "AssembleWith": "Line",
            },
            TransformResources={
                "InstanceType": "ml.m5.xlarge",
                "InstanceCount": 1,
            },
        )

    except Exception as e:
        print(e)
        raise e

    return {"statusCode": 200, "body": "Lambda execution completed"}
