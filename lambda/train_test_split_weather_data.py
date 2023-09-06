import json
import pandas as pd
import boto3
import urllib.parse
import datetime

s3 = boto3.client("s3")


def write_dicts_to_file(path, data):
    with open(path, "wb") as fp:
        for d in data:
            fp.write(json.dumps(d).encode("utf-8"))
            fp.write("\n".encode("utf-8"))


def series_to_obj(ts, cat=None):
    obj = {"start": str(ts.index[0]), "target": list(ts)}
    if cat is not None:
        obj["cat"] = cat
    return obj


def series_to_jsonline(ts, cat=None):
    return json.dumps(series_to_obj(ts, cat))


def dict_to_series(time_dict: dict) -> pd.Series:
    """Translates a dictionary to a time series using a pandas series data type."""
    time_index = pd.date_range(
        start=time_dict["start"], periods=len(time_dict["target"]), freq="H"
    )
    return pd.Series(data=time_dict["target"], index=time_index)


def copy_to_s3(local_file, s3_path, override=False):
    s3 = boto3.resource("s3")

    assert s3_path.startswith("s3://")
    split = s3_path.split("/")
    bucket = split[2]
    path = "/".join(split[3:])
    buk = s3.Bucket(bucket)

    if len(list(buk.objects.filter(Prefix=path))) > 0:
        if not override:
            print(
                "File s3://{}/{} already exists.\nSet override to upload anyway.\n".format(
                    bucket, s3_path
                )
            )
            return
        else:
            print("Overwriting existing file")
    with open(local_file, "rb") as data:
        print("Uploading file to {}".format(s3_path))
        buk.put_object(Key=path, Body=data)


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    s3 = boto3.client("s3")

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    fc = s3.get_object(Bucket=bucket, Key=key)
    file_content = fc["Body"]
    raw_json = json.load(file_content)

    time_series = []
    for i in raw_json:
        time_series.append(dict_to_series(i))

    time_series_training = []
    for ts in time_series:
        time_series_training.append(ts[:-24])

    encoding = "utf-8"
    FILE_TRAIN = "train.json"
    FILE_TEST = "test.json"
    with open("/tmp/" + FILE_TRAIN, "wb") as f:
        for ts in time_series_training:
            f.write(series_to_jsonline(ts).encode(encoding))
            f.write("\n".encode(encoding))

    with open("/tmp/" + FILE_TEST, "wb") as f:
        for ts in time_series:
            f.write(series_to_jsonline(ts).encode(encoding))
            f.write("\n".encode(encoding))

    key_prefix_train = "deep_ar/data/train"
    key_prefix_test = "deep_ar/data/test"

    copy_to_s3(
        "/tmp/" + FILE_TRAIN,
        "s3://{}/"
        + key_prefix_train
        + "/"
        + key[key.find("raw/") + len("raw/") : key.find("_preprocessed")]
        + "_train.json",
        override=True,
    )
    copy_to_s3(
        "/tmp/" + FILE_TEST,
        "s3://{}/"
        + key_prefix_test
        + "/"
        + key[key.find("raw/") + len("raw/") : key.find("_preprocessed")]
        + "_test.json",
        override=True,
    )

    print(bucket)
    print(key)
    # TODO implement
    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
