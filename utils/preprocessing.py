import pandas as pd
import numpy as np
import json
import boto3
import urllib.parse
import datetime
from typing import Dict, List
from datetime import datetime, timezone, timedelta, date

# from sklearn.preprocessing import StandardScaler


def get_cloud_cover(x):
    try:
        return x[0]["amount"]
    except Exception as e:
        return e


def columnNameReformat(column_name: str) -> str:
    """Reformats a column name for easier use in applications."""
    column_name = column_name.lower()
    column_name = column_name.replace("properties", "")
    column_name = column_name.replace(".", "_")
    column_name = column_name.strip()
    return column_name


def preprocessQuant(feature) -> np.array:
    """Processes a quantiative feature for input to an ML algorithm"""
    # scaler = StandardScaler()
    x = np.array(feature)
    np.nan_to_num(x, copy=False)
    # x_tran = scaler.fit_transform(x.reshape(-1,1))
    return x


def featureDict(start: datetime, feature_name: str, feature_data: List) -> Dict:
    """Creates a python Dict object for a feature with a given start date."""

    return dict(
        {
            "start": start,
            feature_name: feature_data.reshape(1, len(feature_data))[0].tolist(),
        }
    )


def getStart(df: pd.DataFrame) -> str:
    """Gets the start date for the input data from the dataframe."""
    # sort the dataframe by the measurement time
    return df.index[0]


def getStartString(df: pd.DataFrame) -> str:
    """Gets the start date in a string format"""
    stp = datetime.strptime(df["properties.timestamp"][0], "%Y-%m-%dT%H:%M:%S%z")
    return datetime.strftime(stp, "%Y-%m-%d %H:%M:%S")


def preprocessDataFrame(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the dataframe."""

    stp = df["properties.timestamp"].apply(
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z")
    )

    # set the index
    df.index = stp

    # sort the dataframe by the index
    df.sort_index(inplace=True)

    # rename index
    df.index.name = "timestamp"

    return df


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
        start=time_dict["start"], periods=len(list(time_dict.values())[1]), freq="H"
    )
    return pd.Series(data=list(time_dict.values())[1], index=time_index)


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
