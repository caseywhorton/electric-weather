import json
import pandas as pd
import boto3
import urllib.parse
from utils.preprocessing import *
import boto3
from datetime import datetime, timezone, timedelta, date



def lambda_handler(event, context):
    s3_train_path = "s3://cw-sagemaker-domain-2/deep_ar/data/train/"
    s3_test_path = "s3://cw-sagemaker-domain-2/deep_ar/data/test/"
    s3_metadata_path = "s3://cw-sagemaker-domain-2/deep_ar/data/metadata/"

    day_window = 180
    today = datetime.now(timezone.utc)
    lag_days = datetime.now(timezone.utc) + timedelta(days=-day_window)

    bucket_weather_data = "cw-sagemaker-domain-1"
    bucket_electric_data = "cw-electric-demand-hourly-preprocessed"

    prefix = "deep_ar/data/raw"

    electricity_features = ['value_demand']
    weather_features = ['temperature_value', 'relativehumidity_value']

    encoding = 'utf-8'

    s3 = boto3.client("s3")

    # get preprocessed weather data
    objects = s3.list_objects(Bucket=bucket_weather_data, Prefix=prefix)

    df_list = []
    for o in objects["Contents"]:
        if o["LastModified"] <= today and o["LastModified"] >= lag_days:
            obj = s3.get_object(Bucket=bucket_weather_data, Key=o["Key"])
            df_list.append(pd.read_csv(obj["Body"]))

    weather_df = pd.concat(df_list).drop_duplicates().reset_index()
    weather_df.columns = [columnNameReformat(x) for x in weather_df.columns]
    weather_df.timestamp = weather_df.timestamp.apply(
        lambda x: roundUpHour(x))
    weather_df.index = weather_df.timestamp
    weather_df.drop(['index', 'timestamp'], axis=1, inplace=True)

    # get preprocessed electric data

    objects = s3.list_objects(Bucket=bucket_electric_data, Prefix=prefix)
    df_list = []
    for o in objects["Contents"]:
        if o["LastModified"] <= today and o["LastModified"] >= lag_days:
            obj = s3.get_object(Bucket=bucket_electric_data, Key=o["Key"])
            df_list.append(pd.read_csv(obj["Body"]))

    electricity_df = pd.concat(df_list).drop_duplicates().reset_index()
    electricity_df.columns = [columnNameReformat(
        x) for x in electricity_df.columns]
    electricity_df.rename(columns={'period': 'timestamp'}, inplace=True)
    electricity_df.index = electricity_df.timestamp
    electricity_df.drop(['index', 'timestamp'], axis=1, inplace=True)

    X = weather_df[weather_features].merge(
        electricity_df[electricity_features], how='inner', left_index=True, right_index=True)
    # get the starting timestamp from the joined data
    start = start_str = getStart(X)
    print(start)

    # preprocess the feature data
    mylist = list()

    # should only be the target columns
    for feature in X.columns:
        mylist.append(
            featureDict(
                start_datetime=start_str,
                feature_data=preprocessQuant(X[feature]),
            )
        )

    # create the individual time series for each feature
    time_series = []
    for i in mylist:
        time_series.append(dictToSeries(i))

    # Create the feature set list
    feature_dict = {'features': weather_features + electricity_features}
    file_name = 'feature_names.json'
    with open("/tmp/" + file_name, "w") as f:
        json.dump(feature_dict, f)

    print(f"Writing Feature List to {s3_metadata_path}")
    copyToS3(
        "/tmp/" + file_name,
        s3_metadata_path
        + date.today().strftime("%Y-%m-%d")
        + "_feature_names.json",
        override=True,
    )

    # Create test data
    file_name = "test.json"
    i = 0
    with open("/tmp/" + file_name, "wb") as f:
        for ts in time_series:
            f.write(seriesToJSONline(ts, feature_name=list(
                mylist[i].keys())[1]).encode(encoding))
            f.write("\n".encode(encoding))
            i += 1

    # copy test data to s3
    print(f"Writing Testing Data to {s3_test_path}")
    copyToS3(
        "/tmp/" + file_name,
        s3_test_path
        + "test.json",
        override=True,
    )

    time_series_training = []
    for ts in time_series:
        time_series_training.append(ts[:-24])

    file_name = "train.json"
    i = 0
    with open("/tmp/" + file_name, "wb") as f:
        for ts in time_series:
            f.write(seriesToJSONline(ts, feature_name=list(
                mylist[i].keys())[1]).encode(encoding))
            f.write("\n".encode(encoding))
            i += 1

    print(f"Writing Training Data to {s3_train_path}")
    # copy training data to S3
    copyToS3(
        "/tmp/" + file_name,
        s3_train_path
        + "train.json",
        override=True,
    )

    # TODO implement
    return {
        "statusCode": 200,
        "body": json.dumps("Training and Testing Data Loaded"),
        "train_path": s3_train_path
    }
