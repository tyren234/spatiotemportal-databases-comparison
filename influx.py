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

df = ais_csv_to_df("data/AIS_2020_12_31.csv")

print("Connecting to InfluxDB...")
client: InfluxDBClient = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
bucket = "temp_bucket_3"

bucket = "main_bucket"

write_api = client.write_api(write_options=SYNCHRONOUS)


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


# df["Points"] = df.apply(create_point, axis=1, args=("vessels_ais_31_12",))

print("Uwaga wielka komenda")
# write_api.write(bucket=bucket, org=org, record=point)
# df.set_index("BaseDateTime")

df = df[["MMSI", "VesselName", "LAT", "LON", "BaseDateTime"]]
print(f"Dataframe shape: {df.shape}")
df["VesselName"] = df["VesselName"].str.replace(" ", "_")
# df = df[["MMSI", "LAT", "LON", "BaseDateTime"]].head(10000)

# write_api.write(bucket=bucket, org=org, record=df,
#                 data_frame_measurement_name="vessels_ais_31_12",
#                 )
divisions = 64
dfs = np.array_split(df, divisions)

for i in range(divisions):
    print(f"Division {i}. shape: {dfs[i].shape}. Processing...")
    write_api.write(bucket=bucket, org=org,
                    record=dfs[i],
                    data_frame_measurement_name="vessels_ais_31_12",
                    data_frame_tag_columns=["MMSI", "VesselName"],
                    # data_frame_tag_columns=["MMSI"],
                    data_frame_timestamp_column="BaseDateTime",
                    )

# for value in range(5):
#     point = (
#         Point("measurement1")
#         .tag("tagname1", "tagvalue1")
#         .field("field1", value)
#     )
#     write_api.write(bucket=bucket, org="pw", record=point)
#     time.sleep(1)  # separate points by 1 second

# query_api = client.query_api()
# query = """from(bucket: "main_bucket")
#  |> range(start: -10m)
#  |> filter(fn: (r) => r._measurement == "measurement1")"""
# tables = query_api.query(query, org="pw")
#
# for table in tables:
#   for record in table.records:
#     print(record)

# query_api = client.query_api()
# query = """from(bucket: "main_bucket")
#   |> range(start: -10   m)
#   |> filter(fn: (r) => r._measurement == "measurement1")
#   |> mean()"""
#
# query = """from(bucket: "main_bucket")
#   |> range(start: -20d)
#   |> filter(fn: (r) => r._measurement == "measurement1")
#   |> mean()"""
# tables = query_api.query(query, org=org)
# print(f"Tables: {tables}")
# for table in tables:
#     for record in table.records:
#         print(record)
print("Closing database connection...")
client.close()
