import time

import numpy as np
from dotenv import load_dotenv

import logger_setup
import logging

logger = logging.getLogger()

import pandas as pd
from influxdb_client import InfluxDBClient
# Ciekawy statek:
# 215131000

import os
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from csv_reader import ais_csv_to_df


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


def upload_df_to_influx_in_batches(df: pd.DataFrame, influx_client: InfluxDBClient, bucket_name: str,
                                   organization_id: str,
                                   batch_size: int = 100000,
                                   data_frame_tag_columns=["MMSI", "VesselName", "CallSign", "VesselType", "Status",
                                                           "Length", "Width", "Cargo", "TransceiverClass"]):
    logger.debug(f"Uploading to influxdb. Batch size: {batch_size}.")
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    rows = df.shape[0]
    divisions = rows // batch_size + 1
    dfs = np.array_split(df, divisions)

    for i in range(divisions):
        logger.debug(f"Uploading division {i}/{divisions - 1}. Shape: {dfs[i].shape}. Processing...")
        write_api.write(bucket=bucket_name, org=organization_id,
                        record=dfs[i],
                        data_frame_measurement_name="vessels_ais_31_12",
                        data_frame_tag_columns=data_frame_tag_columns,
                        data_frame_timestamp_column="BaseDateTime",
                        )


if __name__ == "__main__":
    logger_setup.setup_logger()
    load_dotenv()
    token = os.environ.get("API_INFLUX_KEY")
    org = os.environ.get("INFLUX_ORG_ID")
    url = "http://localhost:" + os.environ.get("INFLUX_PORT", "55000")

    logger.debug(f"Token: {token}")
    logger.debug(f"Organization id: {org}")
    logger.info(f"Database endpoint: {url}")
    logger.debug("Connecting to InfluxDB...")
    client: InfluxDBClient = InfluxDBClient(url=url, token=token, org=org)
    bucket = "temp_bucket_2"

    # query_api = client.query_api()
    # query = """from(bucket: "temp_bucket_4")
    #  |> range(start: 2020-12-31T00:00:00Z, stop: 2020-12-31T23:59:59Z)
    #  |> filter(fn: (r) => r._measurement == "vessels_ais_31_12" and r.MMSI == "215131000")"""
    # tables = query_api.query(query, org=org)
    #
    # for table in tables:
    #     for record in table.records:
    #         print(record)
    # client.close()
    # exit()

    # Uploading data one by one - slow

    # df = ais_csv_to_df("data/AIS_2020_12_31.csv")
    # df["VesselName"] = df["VesselName"].str.replace(" ", "\ ")
    # print("Creating points...")
    # df["Points"] = df.apply(create_point, axis=1, args=("vessels_ais_31_12",))
    # write_api = client.write_api(write_options=SYNCHRONOUS)
    # start_time = time.time()
    # print("Uploading points...")
    # for i, point in enumerate(df["Points"]):
    #     if i % 1000 == 0 and i != 0:
    #         print(f"Point {i}: {point}. Time elapsed: {time.time() - start_time}. Average time per point: {(time.time() - start_time) / i}")
    #     write_api.write(bucket=bucket, org=org, record=point)

    # Uploading data do influx database in batches - fast

    df = ais_csv_to_df("data/AIS_2020_12_31.csv")
    # df = df[["MMSI", "VesselName", "LAT", "LON", "BaseDateTime"]]
    df["VesselName"] = df["VesselName"].str.replace(" ", "\ ")
    df["CallSign"] = df["CallSign"].str.replace(" ", "\ ")
    logger.debug(f"Dataframe shape: {df.shape}")

    logger.debug("Beware! Executing The Command!")
    start_time = time.time()
    upload_df_to_influx_in_batches(df, client, bucket, org, 200000)
    end_time = time.time()
    logger.info(f"Upload time: {end_time - start_time}")
    logger.info("Closing database connection...")
    client.close()
