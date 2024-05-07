import json

import logger_setup
import logging
import pymongo
from pymongo import MongoClient
from pymongo.database import Database, Collection
from csv_reader import ais_csv_to_gdf

logger_setup.setup_logger(level=logging.DEBUG)
logger = logging.getLogger()

tgdf = ais_csv_to_gdf("data/AIS_2020_12_31.csv")#.head(1000)
# tgdf['BaseDateTime'] = tgdf['BaseDateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
# geojson = tgdf._to_geo(drop_id=True)
geojson = tgdf.to_geo_dict(drop_id=True)
print(len(geojson["features"]))
exit()
# print(geojson["features"])

mongo_url = "mongodb://localhost:55000/"
database_name = "temp"
collection_name = "temp"

client: MongoClient = MongoClient(mongo_url)
db: Database = client[database_name]
collection: Collection = db[collection_name]

collection.drop()

collection.insert_many(geojson["features"])

client.close()