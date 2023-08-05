import json
import requests
import boto3
import logging
import datetime

print("Loading function")


def lambda_handler(event, context):
    zone_cde = "OHZ055"
    res = requests.get(
        f"https://api.weather.gov/zones/forecast/{zone_cde}/observations"
    )

    if res.status_code == 200:
        logging.info("***** Response code: 200 *****")
    else:
        logging.error("Error in API response.")

    s3_bucket = "cw-weather-data"
    s3_key = f"observations/{zone_cde}_" + str(datetime.datetime.now().date()) + ".json"
    s3 = boto3.resource("s3")
    object = s3.Object(s3_bucket, s3_key)
    object.put(Body=bytes(json.dumps(res.json()).encode("UTF-8")))
    print("object put to cw-weather-data")

    return None
