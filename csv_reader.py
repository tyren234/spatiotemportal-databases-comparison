import numpy as np
import pandas as pd
import json
import geopandas as gpd
from matplotlib import pyplot as plt
from shapely.geometry import LineString, Point
import logger_setup
import logging

logger = logging.getLogger()


# 'MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG', 'COG', 'Heading', 'VesselName', 'IMO', 'CallSign', 'VesselType', 'Status', 'Length', 'Width', 'Draft', 'Cargo', 'TransceiverClass'

# MMSI               int	              MMSI number of the vessel (AIS identifier)
# BaseDateTime       object               Date and Time (in UTC) when position was recorded by AIS
# LAT                float                Geographical latitude (WGS84)
# LON                float                Geographical longitude (WGS84)
# SOG                float                Speed over ground (knots)
# COG                float                Course over ground (degrees)
# Heading            float                Heading (degrees) of the vessel's hull. A value of 511 indicates there is no heading data.
# VesselName         string                Name of the vessel
# IMO                string               IMO number of the vessel
# CallSign           string               Callsign of the vessel
# VesselType         float                Type of the vessel according to AIS Specification
# Status             float                Navigation status according to AIS Specification
# Length             float
# Width              float
# Draft              float                draught
# Cargo              float
# TransceiverClass   string

# MMSI, BaseDateTime,   LAT,    LON,    SOG,    COG,    Heading,    VesselName, IMO,    CallSign,   VesselType, Status, Length, Width,  Draft, Cargo,   TransceiverClass
# Int,  date,           float,  float,  float,  float,  float,      string,     string, string,     int,        int,    float    float,    float, int,     string

def ais_csv_to_gdf(csv_filename: str) -> gpd.GeoDataFrame:
    temp_df = ais_csv_to_df(csv_filename)
    logger.debug("Creating new GeoDataFrame...")
    gdf = gpd.GeoDataFrame(temp_df, geometry=gpd.GeoSeries.from_xy(temp_df['LON'], temp_df['LAT']), crs=4326)
    return gdf


def ais_csv_to_df(csv_filename: str, timestamp_field_name: str = "BaseDateTime") -> pd.DataFrame:
    logger.debug("Loading data...")
    return pd.read_csv(csv_filename, parse_dates=[timestamp_field_name])


def plot_gdf(gdf: gpd.GeoDataFrame):
    logger.debug("Starting plotting...")
    fig, ax = plt.subplots()
    pd.options.display.max_columns = 6

    logger.debug("Reading continents...")
    continents: gpd.GeoDataFrame = gpd.read_file("data/coastline/north_america.shp").to_crs(epsg=4326)
    continents.plot(ax=ax)

    def points_to_linestring_or_point(points: gpd.GeoSeries):
        if len(points) == 0:
            raise ValueError("No points to linestring")
        if len(points) == 1:
            output = points.iloc[0]
            return output
        return LineString(points.tolist())

    logger.debug(f"Creating gdf_lines...")
    gdf_lines = gdf[["MMSI", "geometry"]].groupby('MMSI')["geometry"].apply(points_to_linestring_or_point)
    logger.debug("Plotting...")
    gdf_lines.head(100).plot(ax=ax)
    plt.show()


def split_df_by_batch_size(df: pd.DataFrame | gpd.GeoDataFrame, batch_size: int) -> list[pd.DataFrame] | list[gpd.GeoDataFrame]:
    if batch_size > len(df) or batch_size < 1:
        raise ValueError("Batch size must be positive and not greater than number of rows")
    start = 0
    end = batch_size
    dfs = []
    while end < len(df):
        dfs.append(df.iloc[start:end])
        start += batch_size
        end += batch_size
    dfs.append(df.iloc[end-batch_size:])
    return dfs


if __name__ == "__main__":
    logger_setup.setup_logging()

    tgdf = ais_csv_to_gdf("data/AIS_2020_12_31.csv")
    print(tgdf.shape)
    logger.debug("Setting up batch sizes and smaller dataframes...")
    # batch_size = 6300000
    # rows = tgdf.shape[0]
    # divisions = rows // batch_size + 1
    # dfs = np.array_split(tgdf, divisions)
    split_point = 4000000
    dfs = [tgdf.iloc[:split_point], tgdf.iloc[split_point:]]

    logger.debug("Creating geojsons")
    for i, df in enumerate(dfs):
        logger.debug(f"Creating geojson {i + 1} from df of shape {df.shape}")
        geojson = df.to_geo_dict(drop_id=True)
        print(geojson["features"])

    # for i, point in enumerate(df["MMSI"]):
    #     print(f"{i}: {point}")
    #     exit()
    # print(tdf[["BaseDateTime"]].head(10))
    # dfs = np.array_split(tdf, 2)
    # print("DFS[0]")
    # print(dfs[0])
    # print("DFS[1]")
    # print(dfs[1])

    # print(tgdf.columns)
    # print(tgdf.shape)
    # print(tgdf.dtypes)
    # print(tgdf.head())
    # mmsis = [366989380, 366685950, 367463740]

    # print(df.loc[df['MMSI'] == mmsis[0]][['BaseDateTime', 'LAT', 'LON']])
    # plot_gdf(gdf)
