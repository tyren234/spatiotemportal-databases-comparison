import pandas as pd
import geopandas as gpd
from matplotlib import pyplot as plt
from shapely.geometry import LineString, Point

pd.options.display.max_columns = 6

# 'MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG', 'COG', 'Heading', 'VesselName', 'IMO', 'CallSign', 'VesselType', 'Status', 'Length', 'Width', 'Draft', 'Cargo', 'TransceiverClass'
# MMSI               int	              MMSI number of the vessel (AIS identifier)
# BaseDateTime       object               Date and Time (in UTC) when position was recorded by AIS
# LAT                float                Geographical latitude (WGS84)
# LON                float                Geographical longitude (WGS84)
# SOG                float                Speed over ground (knots)
# COG                float                Course over ground (degrees)
# Heading            float                Heading (degrees) of the vessel's hull. A value of 511 indicates there is no heading data.
# VesselName         sting                Name of the vessel
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

print("Loading data...")
df: pd.DataFrame = pd.read_csv("data/AIS_2020_12_31.csv")
# print(df.columns)
# print(df.shape)
# print(df.dtypes)
# print(df.head())

# mmsis = [366989380, 366685950, 367463740]
mmsis = df["MMSI"].unique()
fig, ax = plt.subplots()

# print(df.loc[df['MMSI'] == mmsis[0]][['BaseDateTime', 'LAT', 'LON']])
gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_xy(df['LON'], df['LAT']), crs=4326)
continents: gpd.GeoDataFrame = gpd.read_file("data/coastline/north_america.shp").to_crs(epsg=4326)
continents.plot(ax=ax)


def points_to_linestring_or_point(points: gpd.GeoSeries):
    if len(points) == 0:
        raise ValueError("No points to linestring")
    if len(points) == 1:
        output = points.iloc[0]
        return output
    return LineString(points.tolist())


print(f"Creating gdf_lines...")
gdf_lines = gdf[["MMSI", "geometry"]].groupby('MMSI')["geometry"].apply(points_to_linestring_or_point)
print("Plotting...")
gdf_lines.head(100).plot(ax=ax)
plt.show()

plt.show()
