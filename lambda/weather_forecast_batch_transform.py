import json
import urllib.parse
import boto3
import pandas as pd
from utils.preprocessing import *
import datetime
from io import StringIO  # python3; python2: BytesIO
import io

print("Loading function")

s3 = boto3.client("s3")


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    print("bucket, ", bucket)
    print("key, ", key)
    features = ["properties.temperature.value", "properties.relativeHumidity.value"]

    try:
        s3 = boto3.client("s3")

        bucket = "cw-sagemaker-domain-1"
        prefix = "deep_ar/data/raw"

        today = datetime.now(timezone.utc)
        lag_365 = datetime.now(timezone.utc) + timedelta(days=-365)

        objects = s3.list_objects(Bucket=bucket, Prefix=prefix)

        df_list = []
        for o in objects["Contents"]:
            if o["LastModified"] <= today and o["LastModified"] >= lag_365:
                print(o["Key"])
                obj = s3.get_object(Bucket=bucket, Key=o["Key"])
                df_list.append(pd.read_csv(obj["Body"]))

        # drop the duplicates from the dataframe
        preprocessed_df = pd.concat(df_list).drop_duplicates().reset_index()

        # process the features in the columns of the dataframe
        start = getStart(preprocessed_df)
        start_str = getStartString(preprocessed_df)
        features = ["properties.relativeHumidity.value", "properties.temperature.value"]

        # preprocess the feature data
        mylist = list()

        for feature in features:
            mylist.append(
                featureDict(
                    start_str,
                    columnNameReformat(feature, "properties.relativeHumidity.value"),
                    # feature,
                    preprocessQuant(preprocessed_df[feature]),
                )
            )

        # create the individual time series for each feature
        time_series = []
        for i in mylist:
            time_series.append(dict_to_series(i))

        encoding = "utf-8"
        file_name = "serving.json"
        i = 0
        with open("/tmp/" + file_name, "wb") as f:
            for ts in time_series:
                f.write(
                    series_to_jsonline(
                        ts, feature_name=list(mylist[i].keys())[1]
                    ).encode(encoding)
                )
                f.write("\n".encode(encoding))
                i += 1

        copy_to_s3(
            "/tmp/" + file_name,
            "s3://cw-weather-data-deployment/serving/" + "serving.json",
            override=True,
        )

        client = boto3.client("sagemaker")

        response = client.create_transform_job(
            TransformJobName="weather-forecast-model-transform",
            ModelName="pipelines-j2qojlehpzpz-weather-forecast-mod-fzXXv3RR41",
            MaxConcurrentTransforms=1,
            ModelClientConfig={
                "InvocationsTimeoutInSeconds": 600,
                "InvocationsMaxRetries": 5,
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
                "InstanceType": "ml.m4.xlarge",
                "InstanceCount": 1,
            },
            ExperimentConfig={"ExperimentName": "weather-forecast-model"},
        )

    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e

    return None  # ['ContentType']
