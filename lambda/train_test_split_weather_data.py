import json
import pandas as pd
import boto3
import urllib.parse
import datetime
from utils.train_test_split import *
from datetime import datetime, timezone, timedelta, date

s3 = boto3.client("s3")


def lambda_handler(event, context):
    # Get the object from the event and show its content type
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

    # Create the feature set list

    feature_dict = {"features": features}
    encoding = "utf-8"
    file_name = "feature_names.json"
    with open("/tmp/" + file_name, "w") as f:
        json.dump(feature_dict, f)

    copy_to_s3(
        "/tmp/" + file_name,
        "s3://cw-sagemaker-domain-2/"
        + "deep_ar/data/metadata/"
        + "OHZ055_"
        + date.today().strftime("%Y-%m-%d")
        + "_feature_names.json",
        override=True,
    )

    # Create test data

    encoding = "utf-8"
    file_name = "test.json"
    i = 0
    with open("/tmp/" + file_name, "wb") as f:
        for ts in time_series:
            f.write(
                series_to_jsonline(ts, feature_name=list(mylist[i].keys())[1]).encode(
                    encoding
                )
            )
            # print(series_to_jsonline(ts,list(mylist[i].keys())[1]))
            f.write("\n".encode(encoding))
            i += 1

    copy_to_s3(
        "/tmp/" + file_name,
        "s3://cw-sagemaker-domain-2/"
        + "deep_ar/data/test/"
        + "OHZ055_"
        + date.today().strftime("%Y-%m-%d")
        + "_test.json",
        override=True,
    )

    time_series_training = []
    for ts in time_series:
        time_series_training.append(ts[:-24])

    encoding = "utf-8"
    file_name = "train.json"
    i = 0
    with open("/tmp/" + file_name, "wb") as f:
        for ts in time_series:
            f.write(
                series_to_jsonline(ts, feature_name=list(mylist[i].keys())[1]).encode(
                    encoding
                )
            )
            # print(series_to_jsonline(ts,list(mylist[i].keys())[1]))
            f.write("\n".encode(encoding))
            i += 1

    copy_to_s3(
        "/tmp/" + file_name,
        "s3://cw-sagemaker-domain-2/"
        + "deep_ar/data/train/"
        + "OHZ055_"
        + date.today().strftime("%Y-%m-%d")
        + "_train.json",
        override=True,
    )

    # TODO implement
    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
