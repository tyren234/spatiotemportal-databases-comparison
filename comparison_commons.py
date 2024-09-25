import random
import datetime
import logging
import os
import time

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from pymongo import MongoClient
from pymongo.database import Database, Collection
import json
import logger_setup

import pandas as pd
from pandas import DataFrame
# mongo
import folium

# Example Datasets

class Bbox:
    def __init__(self, min_lon, min_lat, max_lon, max_lat, id: int):
        self.min_lon = min_lon
        self.min_lat = min_lat
        self.max_lon = max_lon
        self.max_lat = max_lat
        self.id = id

    def get_coords(self):
        return [self.min_lon, self.min_lat, self.max_lon, self.max_lat]

    def get_id(self):
        return self.id

class Timespan:
    def __init__(self, start: datetime.datetime, end: datetime.datetime, id: int):
        self.start = start
        self.end = end
        self.id = id

    def get_start_end(self):
        return self.start, self.end

    def get_id(self):
        return self.id

bounding_boxes = [
    Bbox(-123.247925, 48.136125, -122.739476, 48.362910, 0),  # Puget Sound, Washington
    Bbox(-123.016525, 37.639830, -122.283450, 37.929824, 1),  # San Francisco Bay, California
    Bbox(-76.510574, 37.973348, -75.962608, 38.393338, 2),  # Chesapeake Bay, Maryland/Virginia
    Bbox(-88.161018, 30.334953, -87.927567, 30.639975, 3),  # Mobile Bay, Alabama
    Bbox(-95.104218, 29.327599, -94.617409, 29.623018, 4),  # Galveston Bay, Texas
    Bbox(-82.775543, 27.599938, -82.320755, 27.934847, 5),  # Tampa Bay, Florida
    Bbox(-122.019295, 36.776848, -121.819153, 37.018274, 6),  # Monterey Bay, California
    Bbox(-71.484741, 41.454842, -71.173431, 41.735072, 7),  # Narragansett Bay, Rhode Island
    Bbox(-117.253113, 32.600235, -117.085083, 32.736514, 8),  # San Diego Bay, California
    Bbox(-88.135986, 44.474116, -87.745605, 44.794497, 9),  # Green Bay
    Bbox(-80.45290918232058, 29.060643707480367, -78.32704059023007, 31.29195079716895, 10),  # Georgia coast
    Bbox(-77.36586166597581, 31.282283517600803, -75.27345529761102, 33.760865420475, 11),  # North Carolina coast
    Bbox(-74.51771061340146, 34.75075477385059, -71.85177110891351, 37.752840882799006, 12),  # Delaware Bay area
    Bbox(-73.8132903076812, 39.28679980551155, -69.84951150110152, 40.36716788459955, 13),  # Long Island Sound, New York
    Bbox(-87.53216792628709, 42.614159443972795, -86.57811342936832, 43.95974516921851, 14),  # Lake Michigan
    Bbox(-88.8185473646937, 47.31402814776524, -85.97885758914987, 48.36369038185671, 15),  # Lake Superior
    Bbox(-119.28398346805034, 28.592051721892147, -116.88328729269529, 32.65616790678931, 16),  # Baja California coast
    Bbox(-115.22516221315561, 21.159217960533027, -112.50145598046407, 25.734344868162523, 17),  # Gulf of California 0
    Bbox(-111.04125138719715, 20.598750857797, -105.33212892067695, 22.5805686602515, 18),  # Northern Gulf of California 0
    Bbox(-96.79281541240942, 25.885529478254256, -93.32496228352338, 28.099129345180913, 19),  # Gulf of Mexico
    Bbox(-88.54476002663944, 27.4284592236325, -84.0568785413999, 30.09273196827901, 20),  # Gulf of Mexico, Alabama
    Bbox(-128.9871203472166, 40.000911885515904, -125.04987957500578, 48.818149529347096, 21),  # Alaska coast
    Bbox(-126.4987002469207, 35.49916842114703, -122.76107863128735, 38.76867969769498, 22),  # Off the coast of California
    Bbox(-90.41838995337584, 30.039892740717292, -89.89240461303508, 30.37633672889592, 23),  # Mississippi coast
    Bbox(-123.02323990734207, 32.99070496259917, -119.56454319294964, 35.24101423046929, 24)  # Northern California coast
]

timespans = [
    Timespan(start=datetime.datetime(2020, 12, 31, 0, 0, 0), end=datetime.datetime(2020, 12, 31, 0, 1, 0), id=0),  # 00:00:00 - 00:30:00 - 0 hours 1 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 0, 45, 0), end=datetime.datetime(2020, 12, 31, 0, 47, 0), id=1),  # 00:45:00 - 01:15:00 - 0 hours 2 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 1, 30, 0), end=datetime.datetime(2020, 12, 31, 1, 33, 0), id=2),   # 01:30:00 - 02:00:00 - 0 hours 3 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 2, 15, 0), end=datetime.datetime(2020, 12, 31, 2, 19, 0), id=3),   # 02:15:00 - 03:00:00 - 0 hours 4 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 3, 10, 0), end=datetime.datetime(2020, 12, 31, 3, 15, 0), id=4),   # 03:10:00 - 04:05:00 - 0 hours 5 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 4, 15, 0), end=datetime.datetime(2020, 12, 31, 4, 21, 0), id=5),   # 04:15:00 - 05:00:00 - 0 hours 6 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 5, 20, 0), end=datetime.datetime(2020, 12, 31, 5, 27, 0), id=6),  # 05:20:00 - 06:10:00 - 0 hours 7 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 6, 25, 0), end=datetime.datetime(2020, 12, 31, 6, 33, 0), id=7),  # 06:25:00 - 07:10:00 - 0 hours 8 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 7, 30, 0), end=datetime.datetime(2020, 12, 31, 7, 39, 0), id=8),  # 07:30:00 - 08:15:00 - 0 hours 9 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 8, 30, 0), end=datetime.datetime(2020, 12, 31, 8, 40, 0), id=9),   # 08:30:00 - 09:00:00 - 0 hours 10 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 9, 15, 0), end=datetime.datetime(2020, 12, 31, 9, 55, 0), id=10),  # 09:15:00 - 09:55:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 10, 5, 0), end=datetime.datetime(2020, 12, 31, 10, 40, 0), id=11), # 10:05:00 - 10:40:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 11, 0, 0), end=datetime.datetime(2020, 12, 31, 11, 35, 0), id=12), # 11:00:00 - 11:35:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 11, 50, 0), end=datetime.datetime(2020, 12, 31, 12, 30, 0), id=13), # 11:50:00 - 12:30:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 12, 45, 0), end=datetime.datetime(2020, 12, 31, 13, 20, 0), id=14), # 12:45:00 - 13:20:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 13, 35, 0), end=datetime.datetime(2020, 12, 31, 14, 20, 0), id=15), # 13:35:00 - 14:20:00 - 0 hours 45 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 14, 35, 0), end=datetime.datetime(2020, 12, 31, 15, 15, 0), id=16), # 14:35:00 - 15:15:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 15, 30, 0), end=datetime.datetime(2020, 12, 31, 16, 15, 0), id=17), # 15:30:00 - 16:15:00 - 0 hours 45 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 16, 25, 0), end=datetime.datetime(2020, 12, 31, 17, 0, 0), id=18),  # 16:25:00 - 17:00:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 17, 15, 0), end=datetime.datetime(2020, 12, 31, 17, 55, 0), id=19), # 17:15:00 - 17:55:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 18, 10, 0), end=datetime.datetime(2020, 12, 31, 18, 50, 0), id=20), # 18:10:00 - 18:50:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 19, 5, 0), end=datetime.datetime(2020, 12, 31, 19, 40, 0), id=21),  # 19:05:00 - 19:40:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 19, 50, 0), end=datetime.datetime(2020, 12, 31, 20, 30, 0), id=22), # 19:50:00 - 20:30:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 20, 45, 0), end=datetime.datetime(2020, 12, 31, 21, 20, 0), id=23), # 20:45:00 - 21:20:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 21, 35, 0), end=datetime.datetime(2020, 12, 31, 22, 10, 0), id=24), # 21:35:00 - 22:10:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 22, 25, 0), end=datetime.datetime(2020, 12, 31, 23, 0, 0), id=25),  # 22:25:00 - 23:00:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 23, 10, 0), end=datetime.datetime(2020, 12, 31, 23, 55, 0), id=26), # 23:10:00 - 23:55:00 - 0 hours 45 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 0, 10, 0), end=datetime.datetime(2020, 12, 31, 0, 55, 0), id=27),  # 00:10:00 - 00:55:00 - 0 hours 45 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 1, 5, 0), end=datetime.datetime(2020, 12, 31, 1, 45, 0), id=28),   # 01:05:00 - 01:45:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 2, 0, 0), end=datetime.datetime(2020, 12, 31, 2, 40, 0), id=29),   # 02:00:00 - 02:40:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 3, 0, 0), end=datetime.datetime(2020, 12, 31, 3, 50, 0), id=30),   # 03:00:00 - 03:50:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 4, 0, 0), end=datetime.datetime(2020, 12, 31, 4, 35, 0), id=31),   # 04:00:00 - 04:35:00 - 0 hours 35 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 5, 0, 0), end=datetime.datetime(2020, 12, 31, 5, 40, 0), id=32),   # 05:00:00 - 05:40:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 6, 0, 0), end=datetime.datetime(2020, 12, 31, 6, 50, 0), id=33),   # 06:00:00 - 06:50:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 7, 0, 0), end=datetime.datetime(2020, 12, 31, 7, 45, 0), id=34),   # 07:00:00 - 07:45:00 - 0 hours 45 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 8, 0, 0), end=datetime.datetime(2020, 12, 31, 8, 50, 0), id=35),   # 08:00:00 - 08:50:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 9, 0, 0), end=datetime.datetime(2020, 12, 31, 9, 55, 0), id=36),   # 09:00:00 - 09:55:00 - 0 hours 55 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 10, 10, 0), end=datetime.datetime(2020, 12, 31, 10, 55, 0), id=37),# 10:10:00 - 10:55:00 - 0 hours 45 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 11, 0, 0), end=datetime.datetime(2020, 12, 31, 11, 50, 0), id=38), # 11:00:00 - 11:50:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 12, 5, 0), end=datetime.datetime(2020, 12, 31, 12, 45, 0), id=39), # 12:05:00 - 12:45:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 13, 0, 0), end=datetime.datetime(2020, 12, 31, 13, 40, 0), id=40), # 13:00:00 - 13:40:00 - 0 hours 40 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 14, 0, 0), end=datetime.datetime(2020, 12, 31, 14, 50, 0), id=41), # 14:00:00 - 14:50:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 15, 5, 0), end=datetime.datetime(2020, 12, 31, 15, 50, 0), id=42), # 15:05:00 - 15:50:00 - 0 hours 45 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 16, 0, 0), end=datetime.datetime(2020, 12, 31, 16, 55, 0), id=43), # 16:00:00 - 16:55:00 - 0 hours 55 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 17, 5, 0), end=datetime.datetime(2020, 12, 31, 17, 55, 0), id=44), # 17:05:00 - 17:55:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 18, 10, 0), end=datetime.datetime(2020, 12, 31, 19, 0, 0), id=45), # 18:10:00 - 19:00:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 19, 10, 0), end=datetime.datetime(2020, 12, 31, 20, 0, 0), id=46), # 19:10:00 - 20:00:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 20, 10, 0), end=datetime.datetime(2020, 12, 31, 21, 0, 0), id=47), # 20:10:00 - 21:00:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 21, 10, 0), end=datetime.datetime(2020, 12, 31, 22, 0, 0), id=48), # 21:10:00 - 22:00:00 - 0 hours 50 minutes
    Timespan(start=datetime.datetime(2020, 12, 31, 22, 10, 0), end=datetime.datetime(2020, 12, 31, 23, 0, 0), id=49), # 22:10:00 - 23:00:00 - 0 hours 50 minutes
]

# General

logger_setup.setup_logging(level=logging.INFO)
logger = logging.getLogger()
load_dotenv()

def get_results_folium(mongo_output=[], influx_tables=[], mobility_output=[], bounding_boxes_to_display=[],
                       display_large_results=False):
    no_points = len(mongo_output) + len(influx_tables) + len(mobility_output)
    if no_points > 3000 and display_large_results == False:
        print(
            f"There are many results to display ({no_points}). If you are certain you want to do that change display_large_results to True.")
        return
    CHICAGO_COORDINATES = (42, -95)

    map_attributions = ('&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> '
                        'contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>')

    folium_map = folium.Map(location=CHICAGO_COORDINATES,
                            attr=map_attributions,
                            zoom_start=5,
                            control_scale=True,
                            height=800,
                            width=1400)

    for entry in mongo_output:
        entry['properties']['BaseDateTime'] = entry['properties']['BaseDateTime'].isoformat()
        geojson = (folium.GeoJson(entry))
        popup = folium.Popup(
            f"mmsi: {entry['properties']['MMSI']}, name {entry['properties']['VesselName']}, time: {entry['properties']['BaseDateTime']}")
        popup.add_to(geojson)
        geojson.add_to(folium_map)

    for table in influx_tables:
        for record in table:
            popup = folium.Popup(f"mmsi: {record['MMSI']}, time: {record['_time']}")
            folium.Marker(location=[record["lat"], record["lon"]],
                          popup=popup,
                          icon=folium.Icon(color='blue', icon='ship', prefix='fa')).add_to(folium_map)

    for mobility_output in mobility_output:
        mmsi = mobility_output[0]
        geojson_string = mobility_output[1]
        timestamp = mobility_output[2]

        # Parse the GeoJSON string to a Python dictionary
        geojson_obj = json.loads(geojson_string)

        # Extract coordinates
        coordinates = geojson_obj['coordinates']
        lat, lon = coordinates[1], coordinates[0]

        # Create the popup content
        popup_content = f"MMSI: {mmsi}<br>Timestamp: {timestamp}"

        # Add the point to the map
        folium.Marker(
            location=[lat, lon],
            popup=popup_content
        ).add_to(folium_map)

    for bbox in bounding_boxes_to_display:
        coords = bbox.get_coords()
        min_lon, min_lat, max_lon, max_lat = coords

        # Create the bounding box as a rectangle
        folium.Rectangle(
            bounds=[(min_lat, min_lon), (max_lat, max_lon)],
            color="blue",  # You can change the color as needed
            fill=True,
            popup=bbox.get_id(),
            fill_opacity=0.2
        ).add_to(folium_map)

    return folium_map

def generate_random_timespans(count):
    timespans = []
    for i in range(count):
        # Generate random start time between 00:00 and 23:00
        start_hour = random.randint(0, 23)
        start_minute = random.randint(0, 59)
        start_second = random.randint(0, 59)
        start_time = datetime.datetime(2020, 12, 31, start_hour, start_minute, start_second)

        # Generate random duration between 10 minutes and 3 hours
        min_duration = 10
        max_duration = 180
        duration_minutes = random.randint(min_duration, max_duration)
        end_time = start_time + datetime.timedelta(minutes=duration_minutes)

        # Ensure end time does not exceed the day
        if end_time > datetime.datetime(2020, 12, 31, 23, 59, 59):
            end_time = datetime.datetime(2020, 12, 31, 23, 59, 59)

        # Create the Timespan object and append to the list
        timespans.append(
            Timespan(
                start=start_time,
                end=end_time,
                id=i
            )
        )

    return timespans


def get_time_passed(end_time: float, start_time: float) -> float:
    time_passed = start_time - end_time
    return time_passed


def get_time_per_result(time_passed: float, no_results: float) -> float:
    if not no_results: return -1
    time_per_result = time_passed / no_results
    return time_per_result


def log_spatial_info(bbox_id: int, iterations_no: int, time_passed: float, time_per_result: float, no_results: int):
    logger.info(
        f"{bbox_id}/{iterations_no}. Query time: {round(time_passed, 5)}, query time per result {round(time_per_result, 5)} no. of results: {no_results}, bbox {bbox_id}")


def log_time_info(iteration: int, iterations_no: int, time_passed: float, time_per_result: float, no_results: int,
                  timespan: Timespan):
    start, end = timespan.get_start_end()
    logger.info(
        f"{iteration}/{iterations_no}. Query time: {round(time_passed, 5)}, query time per result {round(time_per_result, 5)} no. of results: {no_results}, timespan: {start}-{end}")


def log_spatiotemporal_info(iteration: int, iterations_no: int, time_passed: float, time_per_result: float,
                            no_results: int, timespan: Timespan, bbox: Bbox):
    start, end = timespan.get_start_end()
    logger.info(
        f"{iteration}/{iterations_no}. Query time: {time_passed}, time per result: {time_per_result}, no. of results: {no_results}, bbox id: {bbox.get_id()}, timespan: {start}-{end}")


def get_results_dataframe(times_table: list, no_results_table: list, bounding_boxes_table: list | None,
                          timespans_table: list | None, filename: str,
                          directory: str = "./data/results") -> pd.DataFrame:
    correct_len = len(times_table)

    bounding_boxes_ids_table = [None] * correct_len if bounding_boxes_table is None else [bbox.get_id() for bbox in
                                                                                          bounding_boxes_table]
    timespans_ids_table = [None] * correct_len if timespans_table is None else [timespan.get_id() for timespan in
                                                                                timespans_table]

    if correct_len != len(no_results_table) != len(bounding_boxes_ids_table) != len(timespans_ids_table):
        raise Exception("Every list should be of the same size")

    df = pd.DataFrame({
        "Query time": times_table,
        "No results": no_results_table,
        "Bbox id": bounding_boxes_ids_table,
        "Timespan id": timespans_ids_table
    })
    df.index.name = "Id"
    if filename.endswith(".csv"): filename = filename[:-4]
    df.to_csv(f"{directory}/{filename}.csv")
    return df


# Setup

def mongo_setup():
    mongo_url = "mongodb://localhost:" + os.environ.get("MONGO_PORT", "55000")
    mongo_database = "temp"
    mongo_collection = "aisdata31-12-2020"

    logger.info(f"MongoDB endpoint: {mongo_url}")
    logger.info(f"MongoDB database name: {mongo_database}")
    logger.info(f"MongoDB collection name: {mongo_collection}")
    return mongo_url, mongo_database, mongo_collection


def influx_setup():
    influx_token = os.environ.get("API_INFLUX_KEY")
    influx_org = os.environ.get("INFLUX_ORG_ID")
    influx_url = "http://localhost:" + os.environ.get("INFLUX_PORT", "55000")

    logger.info(f"InfluxDB Token: {influx_token}")
    logger.info(f"InfluxDB Organization id: {influx_org}")
    logger.info(f"InfluxDB Database endpoint: {influx_url}")
    return influx_token, influx_org, influx_url


def mobility_setup():
    mobility_host = os.environ.get("MOBILITY_HOST")
    mobility_port = os.environ.get("MOBILITY_PORT")
    mobility_user = os.environ.get("MOBILITY_USER")
    mobility_password = os.environ.get("MOBILITY_PASSWORD")
    mobility_database = os.environ.get("MOBILITY_DATABASE")

    logger.info(f"MobilityDB endpoint: {mobility_host}:{mobility_port}")
    logger.info(f"MobilityDB user name: {mobility_user}")
    logger.info(f"MobilityDB database name: {mobility_database}")
    return mobility_host, mobility_port, mobility_user, mobility_password, mobility_database


def setup():
    mongo_setup()
    influx_setup()
    mobility_setup()


# MongoDB

def measure_mongo_spatial(create_results: bool = False, iteration_table: list = bounding_boxes,
                          export_csv_filename: str | bool = False) -> None | DataFrame:
    mongo_url, mongo_database, mongo_collection = mongo_setup()

    client: MongoClient = MongoClient(mongo_url)
    db: Database = client[mongo_database]
    collection: Collection = db[mongo_collection]

    mongo_spatial_times = []
    mongo_spatial_results = []
    mongo_spatial_no_results = []
    logger.info(f"Running Mongo spatial")
    for bbox in iteration_table:
        min_lon, min_lat, max_lon, max_lat = bbox.get_coords()

        # Define the query using $geoWithin and $box
        query = {
            'geometry': {
                '$geoWithin': {
                    '$box': [
                        [min_lon, min_lat],
                        [max_lon, max_lat]
                    ]
                }
            }
        }
        start_time = time.time()
        # Execute the query
        mongo_results = list(collection.find(query, {'_id': False}))
        end_time = time.time()

        iteration = bbox.get_id()
        no_results = len(mongo_results)
        time_passed = get_time_passed(start_time, end_time)
        time_per_result = get_time_per_result(time_passed, no_results)
        log_spatial_info(iteration, len(iteration_table), time_passed, time_per_result, no_results)

        mongo_spatial_no_results.append(no_results)
        mongo_spatial_times.append(time_passed)
        if create_results: mongo_spatial_results.append(mongo_results)
    logger.info(f"Mongo spatial average time: {sum(mongo_spatial_times) / len(mongo_spatial_times)}")

    client.close()

    if export_csv_filename:
        return get_results_dataframe(mongo_spatial_times, mongo_spatial_no_results, iteration_table, None,
                                     export_csv_filename, directory="./data/results/mongo")


def measure_mongo_temporal(create_results: bool = False, iteration_table: list = timespans,
                           export_csv_filename: str | bool = False) -> None | DataFrame:
    mongo_url, mongo_database, mongo_collection = mongo_setup()
    # Mongo

    client: MongoClient = MongoClient(mongo_url)
    db: Database = client[mongo_database]
    collection: Collection = db[mongo_collection]

    mongo_time_times = []
    mongo_time_results = []
    mongo_time_no_results = []

    logger.info(f"Running Mongo temporal")
    for timespan in iteration_table:
        start, end = timespan.get_start_end()
        # Define the query using $geoWithin and $box
        query = {
            "properties.BaseDateTime": {
                "$gte": start,
                "$lte": end
            }
        }
        start_time = time.time()
        # Execute the query
        mongo_time_result = list(collection.find(query, {'_id': False}))
        end_time = time.time()

        iteration = timespan.get_id()
        no_results = len(mongo_time_results)

        time_passed = get_time_passed(start_time, end_time)
        time_per_result = get_time_per_result(time_passed, no_results)

        log_time_info(timespan.get_id(), len(iteration_table), time_passed, time_per_result, no_results, timespan)
        mongo_time_no_results.append(no_results)
        mongo_time_times.append(time_passed)
        if create_results: mongo_time_results.append(mongo_time_result)

    logger.info(f"Mongo time average time: {sum(mongo_time_times) / len(mongo_time_times)}")

    client.close()

    if export_csv_filename:
        return get_results_dataframe(mongo_time_times, mongo_time_no_results, None, iteration_table,
                                     export_csv_filename, directory="./data/results/mongo")


def measure_mongo_spatiotemporal(create_results: bool = False, bounding_boxes_table: list = bounding_boxes,
                                 timespans_table: list = timespans,
                                 export_csv_filename: str | bool = False) -> None | DataFrame:
    mongo_url, mongo_database, mongo_collection = mongo_setup()
    # Mongo

    client: MongoClient = MongoClient(mongo_url)
    db: Database = client[mongo_database]
    collection: Collection = db[mongo_collection]

    mongo_spatiotemporal_times = []
    mongo_spatiotemporal_results = []
    mongo_spatiotemporal_no_results = []

    logger.info(f"Running Mongo spatiotemporal")
    no_of_iterations = min(len(timespans_table), len(bounding_boxes_table))
    for iteration in range(no_of_iterations):
        timespan = timespans_table[iteration]
        bbox = bounding_boxes_table[iteration]

        start, end = timespan.get_start_end()
        min_lon, min_lat, max_lon, max_lat = bbox.get_coords()

        query = {
            "properties.BaseDateTime": {
                "$gte": start,
                "$lte": end
            },
            'geometry': {
                '$geoWithin': {
                    '$box': [
                        [min_lon, min_lat],
                        [max_lon, max_lat]
                    ]
                }
            }
        }
        start_time = time.time()
        # Execute the query
        mongo_spatiotemporal_result = list(collection.find(query, {'_id': False}))
        end_time = time.time()

        no_results = len(mongo_spatiotemporal_result)
        time_passed = get_time_passed(start_time, end_time)
        time_per_result = get_time_per_result(time_passed, no_results)

        logger.info(
            f"{iteration}/{no_of_iterations}. Query time: {time_passed}, time per result: {time_per_result}, no. of results: {no_results}, timespan: {start}-{end}")

        mongo_spatiotemporal_no_results.append(no_results)
        mongo_spatiotemporal_times.append(time_passed)
        if create_results: mongo_spatiotemporal_results.append(mongo_spatiotemporal_result)

    logger.info(
        f"Mongo spatiotemporal average time: {sum(mongo_spatiotemporal_times) / len(mongo_spatiotemporal_times)}")

    client.close()

    if export_csv_filename:
        return get_results_dataframe(mongo_spatiotemporal_times, mongo_spatiotemporal_no_results, bounding_boxes_table,
                                     timespans_table, export_csv_filename, directory="./data/results/mongo")

# InfluxDB

def measure_influx_spatial():
    # This unfortunately doesn't yield results of any value, so it's not as usable as the other 'measure_...' functions.
    # Influx
    influx_token, influx_org, influx_url = influx_setup()
    logger.info(f"Running Influx spatial")

    # influx_url = "http://localhost:55002"

    # bucket = "shapedData_bucket2"
    # bucket = "temp"
    bucket = "aisdata_s2indexed_lvl24"
    start_date = "2020-12-31T00:00:00Z"
    stop_date = "2020-12-31T00:00:59Z"
    # min_lat = 41.80
    # max_lat = 41.87
    # min_lon = -88.0
    # max_lon = -87.0
    influx_spatial_results = []
    level = 10
    strict = "true"
    levels = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    for level in levels:
        # for bbox in bounding_boxes:
        client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org,
                                timeout=600000)
        query_api = client.query_api()
        bbox = bounding_boxes[0]
        min_lon, min_lat, max_lon, max_lat = bbox.get_coords()

        query = f"""
        import "experimental/geo"

        region = {{
            minLat: {min_lat},
            maxLat: {max_lat},
            minLon: {min_lon},  
            maxLon: {max_lon},
        }}

        from(bucket: "{bucket}")
            |> range(start: {start_date}, stop: {stop_date})
            |> filter(fn: (r) => r._measurement == "vessels_ais_31_12")
            |> geo.filterRows(region: region, level: {level}, strict: {strict})
        """

        start_time = time.time()
        # tables = query_api.query_data_frame(query=query)
        influx_results = query_api.query(query=query)
        influx_spatial_results.append(influx_results)
        end_time = time.time()

        record_count = sum(len(table.records) for table in influx_results)
        logger.info(f"Query took {end_time - start_time} seconds, no. of results: {record_count}. Level: {level}")
        client.close()
        # break


def measure_influx_temporal(create_results: bool = False, iteration_table: list = timespans,
                            export_csv_filename: str | bool = False) -> None | DataFrame:
    influx_token, influx_org, influx_url = influx_setup()
    logger.info(f"Running Influx temporal")

    influx_time_times = []
    influx_time_results = []
    influx_time_no_results = []

    bucket = "temp_bucket_2"

    for timespan in iteration_table:
        start, end = timespan.get_start_end()
        client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org,
                                timeout=600000)
        query_api = client.query_api()

        query = f"""
        from(bucket: "{bucket}")
          |> range(start: {start.isoformat()}Z, stop: {end.isoformat()}Z)
          |> filter(fn: (r) => r._measurement == "vessels_ais_31_12")
          |> filter(fn: (r) => r._field == "LAT" or r._field == "LON")
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> keep(columns: ["_time", "MMSI", "lat", "lon"])
        """

        start_time = time.time()
        influx_time_result = query_api.query(query=query)
        end_time = time.time()

        iteration = timespan.get_id()
        no_results = sum(len(table.records) for table in influx_time_result)

        time_passed = get_time_passed(start_time, end_time)
        time_per_result = get_time_per_result(time_passed, no_results)

        log_time_info(timespan.get_id(), len(iteration_table), time_passed, time_per_result, no_results, timespan)

        # influx_time_results.append(influx_time_result)
        influx_time_times.append(time_passed)

        client.close()

        influx_time_no_results.append(no_results)
        influx_time_times.append(time_passed)
        if create_results: influx_time_results.append(influx_time_result)
    logger.info(f"Influx time average time: {sum(influx_time_times) / len(influx_time_times)}")

    if export_csv_filename:
        return get_results_dataframe(influx_time_times, influx_time_no_results, None, iteration_table,
                                     export_csv_filename, directory="./data/results/influx")

# Mobility

def measure_mobility_spatial(create_results: bool = False, iteration_table: list = bounding_boxes,
                             export_csv_filename: str | bool = False) -> None | DataFrame:
    # Mobility
    logger.info(f"Running Mobility spatial")

    mobility_host, mobility_port, mobility_user, mobility_password, mobility_database = mobility_setup()

    conn = psycopg2.connect(
        database=mobility_database,
        host=mobility_host,
        user=mobility_user,
        password=mobility_password,
        port=mobility_port,
    )
    mobility_spatial_times = []
    mobility_spatial_results = []
    mobility_spatial_no_results = []

    cursor = conn.cursor()
    for bbox in iteration_table:
        min_lon, min_lat, max_lon, max_lat = bbox.get_coords()
        start_time = time.time()
        cursor.execute(f'''
    select mmsi, st_asgeojson(geom), timestamp from
    (
    select mmsi, unnest(instants(route))::geometry as geom, starttimestamp(unnest(instants(route))) as timestamp 
    from aggregated_vessel_positions
    where eintersects(st_setsrid(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}), 4326), route)
    )
    where st_intersects(st_setsrid(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}), 4326), geom);
        ''')

        mobility_result = []
        rows_limit_per_fetch = 5000
        while True:
            rows_queried = cursor.fetchmany(size=rows_limit_per_fetch)
            if not rows_queried:
                break
            mobility_result += rows_queried
        end_time = time.time()

        iteration = bbox.get_id()
        no_results = len(mobility_result)
        time_passed = get_time_passed(start_time, end_time)
        time_per_result = get_time_per_result(time_passed, no_results)

        log_spatial_info(iteration, len(iteration_table), time_passed, time_per_result, no_results)

        mobility_spatial_no_results.append(no_results)
        mobility_spatial_times.append(time_passed)
        if create_results: mobility_spatial_results.append(mobility_result)
    conn.close()
    logger.info(f"Mobility spatial average time: {sum(mobility_spatial_times) / len(mobility_spatial_times)}")

    if export_csv_filename:
        return get_results_dataframe(mobility_spatial_times, mobility_spatial_no_results, iteration_table, None,
                                     export_csv_filename, directory="./data/results/mobility")


def measure_mobility_temporal(create_results: bool = False, iteration_table: list = timespans,
                              export_csv_filename: str | bool = False) -> None | DataFrame:
    logger.info(f"Running Mobility temporal")

    mobility_host, mobility_port, mobility_user, mobility_password, mobility_database = mobility_setup()

    conn = psycopg2.connect(
        database=mobility_database,
        host=mobility_host,
        user=mobility_user,
        password=mobility_password,
        port=mobility_port,
    )
    mobility_time_times = []
    mobility_time_results = []
    mobility_time_no_results = []

    cursor = conn.cursor()
    for timespan in iteration_table:
        start, end = timespan.get_start_end()
        start_time = time.time()
        cursor.execute(f'''
    SELECT 
        mmsi, 
        st_asgeojson(instance::geometry),
        starttimestamp(instance)
    FROM 
        (
        SELECT
            mmsi,
            unnest(instants(attime(route, tstzspan('[{start.isoformat()}, {end.isoformat()}]')))) as instance 
        FROM
            aggregated_vessel_positions
        );
        ''')

        mobility_result = []
        rows_limit_per_fetch = 5000
        while True:
            rows_queried = cursor.fetchmany(size=rows_limit_per_fetch)
            if not rows_queried:
                break
            mobility_result += rows_queried
        end_time = time.time()

        iteration = timespan.get_id()
        no_results = len(mobility_result)

        time_passed = get_time_passed(start_time, end_time)
        time_per_result = get_time_per_result(time_passed, no_results)

        log_time_info(timespan.get_id(), len(iteration_table), time_passed, time_per_result, no_results, timespan)
        # logger.info(f"{timespan.get_id()}/{len(timespans)}. Query time: {end_time - start_time}, no. of results: {len(mobility_result)}, timespan {timespan.get_id()}/{len(timespans)}: {start}-{end}")

        mobility_time_times.append(time_passed)
        mobility_time_no_results.append(no_results)
        if create_results: mobility_time_results.append(mobility_result)

    conn.close()
    logger.info(f"Mobility time average time: {sum(mobility_time_times) / len(mobility_time_times)}")

    if export_csv_filename:
        return get_results_dataframe(mobility_time_times, mobility_time_no_results, None, iteration_table,
                                     export_csv_filename, directory="./data/results/mobility")


def measure_mobility_spatiotemporal(create_results: bool = False, bounding_boxes_table: list = bounding_boxes,
                                    timespans_table: list = timespans,
                                    export_csv_filename: str | bool = False) -> None | DataFrame:
    mobility_host, mobility_port, mobility_user, mobility_password, mobility_database = mobility_setup()
    # Mobility

    logger.info(f"Running Mobility spatiotemporal")

    conn = psycopg2.connect(
        database=mobility_database,
        host=mobility_host,
        user=mobility_user,
        password=mobility_password,
        port=mobility_port,
    )

    mobility_spatiotemporal_times = []
    mobility_spatiotemporal_results = []
    mobility_spatiotemporal_no_results = []

    cursor = conn.cursor()
    no_of_iterations = min(len(timespans_table), len(bounding_boxes_table))
    for iteration in range(no_of_iterations):
        timespan = timespans_table[iteration]
        bbox = bounding_boxes_table[iteration]

        start, end = timespan.get_start_end()
        min_lon, min_lat, max_lon, max_lat = bbox.get_coords()
        start_time = time.time()

        cursor.execute(f'''
    select mmsi, st_asgeojson(instance::geometry) as geom, starttimestamp(instance) as timestamp from
    (
        select mmsi, unnest(instants(attime(route, tstzspan('[{start.isoformat()}, {end.isoformat()}]')))) as instance 
        from
        (
            select mmsi, route
            from aggregated_vessel_positions
            where eintersects(st_setsrid(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}), 4326), route)
        )
    )
    where st_intersects(st_setsrid(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}), 4326), instance::geometry);
        ''')

        mobility_spatiotemporal_result = []
        rows_limit_per_fetch = 5000
        while True:
            rows_queried = cursor.fetchmany(size=rows_limit_per_fetch)
            if not rows_queried:
                break
            mobility_spatiotemporal_result += rows_queried
        end_time = time.time()

        iteration = iteration
        no_results = len(mobility_spatiotemporal_result)
        time_passed = get_time_passed(start_time, end_time)
        time_per_result = get_time_per_result(time_passed, no_results)

        log_spatiotemporal_info(iteration, no_of_iterations, time_passed, time_per_result, no_results, timespan, bbox)
        logger.info(
            f"{iteration}/{no_of_iterations}. Query time: {time_passed}, time per result: {time_per_result}, no. of results: {no_results}, timespan: {start}-{end}")

        mobility_spatiotemporal_no_results.append(no_results)
        mobility_spatiotemporal_times.append(time_passed)
        if create_results: mobility_spatiotemporal_results.append(mobility_spatiotemporal_result)

    conn.close()
    logger.info(
        f"Mobility time average time: {sum(mobility_spatiotemporal_times) / len(mobility_spatiotemporal_times)}")

    if export_csv_filename:
        return get_results_dataframe(mobility_spatiotemporal_times, mobility_spatiotemporal_no_results,
                                     bounding_boxes_table, timespans_table, export_csv_filename,
                                     directory="./data/results/mobility")
