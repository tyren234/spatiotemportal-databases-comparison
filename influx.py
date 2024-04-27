from dotenv import load_dotenv
load_dotenv()
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = os.environ.get("API_INFLUX_KEY")
org = os.environ.get("INFLUX_ORG_ID")
url = "http://localhost:" + os.environ.get("INFLUX_PORT", "55000")
print(f"Token: {token}")
print(f"Organization id: {org}")
print(f"Database endpoint: {url}")

client: InfluxDBClient = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

bucket = "main_bucket"

write_api = client.write_api(write_options=SYNCHRONOUS)
for value in range(5):
    point = (
        Point("measurement1")
        .tag("tagname1", "tagvalue1")
        .field("field1", value)
    )
    write_api.write(bucket=bucket, org="pw", record=point)
    time.sleep(1)  # separate points by 1 second

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

client.close()
