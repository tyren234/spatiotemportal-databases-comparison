import json
import os
import time

from dotenv import load_dotenv

import logging
import pymongo
from pymongo import MongoClient
from pymongo.database import Database, Collection

import logger_setup
from csv_reader import ais_csv_to_gdf

import pandas as pd




def upload_df_to_mongo():
    load_dotenv()
    mongo_port = os.environ.get("MONGO_PORT")

    start_time = time.time()
    tgdf = ais_csv_to_gdf("data/AIS_2020_12_31.csv")  # .head(1000)
    tgdf['BaseDateTime'] = pd.to_datetime(tgdf['BaseDateTime'], format='%Y-%m-%d %H:%M:%S')
    end_time = time.time()
    logger.info(f"Pandas creating geodataframe time: {end_time - start_time} seconds.")
    logger.debug(f"creating geojson")
    # geojson = tgdf._to_geo(drop_id=True)
    # geojson = tgdf.head(2000000).to_geo_dict(drop_id=True)
    geojson = tgdf.to_geo_dict(drop_id=True)
    end_time = time.time()
    logger.info(f"Geojson creation time: {end_time - start_time} seconds.")
    # print(len(geojson["features"]))
    # exit()
    # print(geojson["features"])

    mongo_url = f"mongodb://localhost:{mongo_port}/"
    database_name = "temp"
    collection_name = "aisdata31-12-2020"

    client: MongoClient = MongoClient(mongo_url)
    db: Database = client[database_name]
    collection: Collection = db[collection_name]

    collection.drop()

    logger.debug(f"Inserting many")
    collection.insert_many(geojson["features"])

    client.close()
    end_time = time.time()
    logger.info(f"Uploading time: {end_time - start_time} seconds")


if __name__ == '__main__':
    logger = logger_setup.setup_logging(level=logging.DEBUG)

    upload_df_to_mongo()
