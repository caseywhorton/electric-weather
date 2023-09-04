import pandas as pd
import numpy as np
import json
import requests
import boto3

# from sklearn.preprocessing import StandardScaler
import datetime
from typing import Dict, List


def get_cloud_cover(x):
    try:
        return x[0]["amount"]
    except:
        return None


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


def featureDict(
    start: datetime.datetime, feature_name: str, feature_data: List
) -> Dict:
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
    stp = datetime.datetime.strptime(
        df["properties.timestamp"][0], "%Y-%m-%dT%H:%M:%S%z"
    )
    return datetime.datetime.strftime(stp, "%Y-%m-%d %H:%M:%S")


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
