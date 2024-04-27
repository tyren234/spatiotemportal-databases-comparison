import pandas as pd
import geopandas as gpd
from matplotlib import pyplot as plt
from shapely.geometry import LineString, Point

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
gdf: gpd.GeoDataFrame = None
df: pd.DataFrame = None


def ais_csv_to_gdf(csv_filename: str) -> gpd.GeoDataFrame:
    global gdf
    print("Loading data...")
    if gdf is not None:
        return gdf.copy(deep=True)
    temp_df = ais_csv_to_df(csv_filename)
    print("Creating new GeoDataFrame...")
    gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_xy(temp_df['LON'], temp_df['LAT']), crs=4326)
    return gdf.copy(deep=True)


def ais_csv_to_df(csv_filename: str) -> pd.DataFrame:
    global df
    print("Loading data...")
    if df is not None:
        return df.copy(deep=True)
    print("Creating new DataFrame...")
    temp_df: pd.DataFrame = pd.read_csv(csv_filename)
    df = temp_df.copy(deep=True)

    return df.copy(deep=True)


def plot_gdf(gdf: gpd.GeoDataFrame):
    print("Starting plotting...")
    fig, ax = plt.subplots()
    pd.options.display.max_columns = 6

    print("Reading continents...")
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


if __name__ == "__main__":
    tgdf = ais_csv_to_gdf("data/AIS_2020_12_31.csv")
    print(tgdf.columns)
    print(tgdf.shape)
    print(tgdf.dtypes)
    print(tgdf.head())
    # mmsis = [366989380, 366685950, 367463740]

    # print(df.loc[df['MMSI'] == mmsis[0]][['BaseDateTime', 'LAT', 'LON']])
    # plot_gdf(gdf)
