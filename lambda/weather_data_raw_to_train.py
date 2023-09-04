import json
import urllib.parse
import boto3
import datetime
import pandas as pd
from utils.preprocessing import *

print("Loading function")

s3 = boto3.client("s3")


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    # get the data from s3
    # get the file that triggered the lambda
    # s3 = boto3.client("s3")
    # fc = s3.get_object(Bucket=bucket, Key=key)
    # file_content = fc["Body"]
    # raw_json = json.load(file_content)

    # print("Received event: " + json.dumps(event, indent=2))

    features = [
        # "properties.windSpeed.value",
        "properties.temperature.value",
        "properties.relativeHumidity.value"
        #'properties.cloudLayers'
    ]

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"]
        raw_json = json.load(file_content)
        df = pd.json_normalize(raw_json, record_path="features")
        df = df[df["properties.station"] == "https://api.weather.gov/stations/KCVG"]
        df.index = df["properties.timestamp"]
        df.sort_index(inplace=True)
        df = df[["properties.timestamp"] + features]
        df = df.drop_duplicates()

        preprocessed_df = preprocessDataFrame(df)
        start = getStart(preprocessed_df)
        start_str = getStartString(preprocessed_df)

        mylist = list()

        for feature in features:
            mylist.append(
                featureDict(
                    start_str,
                    # columnNameReformat(feature),
                    "target",
                    preprocessQuant(preprocessed_df[feature]),
                )
            )

        # print(bucket)
        # print(key)
        # print("CONTENT TYPE: " + response['ContentType'])
        print(mylist)

        bucket = "cw-sagemaker-domain-1"
        key_prefix = "deep_ar/data/raw/"
        file_name = (
            key_prefix
            + key[key.find("/") + 1 : key.find(".json")]
            + "_preprocessed.json"
        )
        print(file_name)

        response = s3.put_object(
            Body=json.dumps(mylist), Bucket="cw-sagemaker-domain-1", Key=file_name
        )

        return response  # ['ContentType']
    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e
