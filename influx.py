import numpy as np
from dotenv import load_dotenv

# Ciekawy statek:
# 215131000

load_dotenv()
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd

from csv_reader import ais_csv_to_df

token = os.environ.get("API_INFLUX_KEY")
org = os.environ.get("INFLUX_ORG_ID")
url = "http://localhost:" + os.environ.get("INFLUX_PORT", "55000")
print(f"Token: {token}")
print(f"Organization id: {org}")
print(f"Database endpoint: {url}")


def create_point(row: pd.Series, measurement_name: str,
                 mmsi_fieldname="MMSI", vessel_name_fieldname="VesselName",
                 latitude_fieldname="LAT", longitude_fieldname="LON", time_fieldname="BaseDateTime"
                 ):
    t = "vessels_ais_31_12"
    point = (
        Point(measurement_name=measurement_name)
        .tag("mmsi", row[mmsi_fieldname])
        .tag("vessel_name", row[vessel_name_fieldname])
        .field("lat", row[latitude_fieldname])
        .field("lon", row[longitude_fieldname])
        .time(row[time_fieldname])
    )
    return point


def upload_df_to_influx_in_batches(df: pd.DataFrame, influx_client: InfluxDBClient, bucket_name: str, organization_id: str,
                                   batch_size: int = 100000):
    print(f"Uploading to influxdb. Batch size: {batch_size}.")
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    rows = df.shape[0]
    divisions = rows // batch_size + 1
    dfs = np.array_split(df, divisions)

    for i in range(divisions):
        print(f"Uploading division {i}/{divisions - 1}. Shape: {dfs[i].shape}. Processing...")
        write_api.write(bucket=bucket_name, org=organization_id,
                        record=dfs[i],
                        data_frame_measurement_name="vessels_ais_31_12",
                        data_frame_tag_columns=["MMSI", "VesselName"],
                        data_frame_timestamp_column="BaseDateTime",
                        )


print("Connecting to InfluxDB...")
client: InfluxDBClient = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
bucket = "temp_bucket_4"

# query_api = client.query_api()
# query = """from(bucket: "temp_bucket_2")
#  |> range(start: 2020-04-28)
#  |> filter(fn: (r) => r._measurement == "vessels_ais_31_12" and r.MMSI == "211331640")"""
# tables = query_api.query(query, org=org)
#
# for table in tables:
#     for record in table.records:
#         print(record)
# client.close()
# exit()


# df["Points"] = df.apply(create_point, axis=1, args=("vessels_ais_31_12",))


df = ais_csv_to_df("data/AIS_2020_12_31.csv")
df = df[["MMSI", "VesselName", "LAT", "LON", "BaseDateTime"]]
df["VesselName"] = df["VesselName"].str.replace(" ", "_")
print(f"Dataframe shape: {df.shape}")

print("Uwaga wielka komenda")
upload_df_to_influx_in_batches(df, client, bucket, org)

print("Closing database connection...")
client.close()
