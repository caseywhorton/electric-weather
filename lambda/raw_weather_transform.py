import boto3
import json
import datetime


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


# get the data from s3
# get the file that triggered the lambda
s3 = boto3.client("s3")
fc = s3.get_object(Bucket=bucket, Key=f"{prefix}{file_name}")
file_content = fc["Body"]
raw_json = json.load(file_content)

# write to /tmp
time_series = []
for i in raw_json:
    time_series.append(dict_to_series(i))

time_series_training = []
for ts in time_series:
    time_series_training.append(ts[:-24])


with open("tmp/train.json", "wb") as f:
    for ts in time_series_training:
        f.write(series_to_jsonline(ts).encode("utf-8"))
        f.write("\n".encode("utf-8"))

with open("tmp/test.json", "wb") as f:
    for ts in time_series:
        f.write(series_to_jsonline(ts).encode("utf-8"))
        f.write("\n".encode("utf-8"))


# write back to S3
s3 = boto3.resource("s3")
d = datetime.datetime.now().strftime("%Y%m%d")
copy_to_s3(
    "tmp/train.json",
    f"s3://{bucket}/{key_prefix_train}/train/train_{d}.json",
    override=True,
)
copy_to_s3(
    "tmp/test.json",
    f"s3://{bucket}/{key_prefix_test}/test/test_{d}.json",
    override=True,
)
