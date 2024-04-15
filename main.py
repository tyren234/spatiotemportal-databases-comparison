import pandas as pd
import geopandas as gpd
from matplotlib import pyplot as plt

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

df: pd.DataFrame = pd.read_csv("data/AIS_2020_12_31.csv")
# print(df.columns)
# print(df.shape)
# print(df.dtypes)
print(df.head())

mmsis = [366989380, 366685950, 367463740]
print(df.loc[df['MMSI'] == mmsis[0]][['BaseDateTime', 'LAT', 'LON']])
gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_xy(df['LON'], df['LAT']), crs=4326)
continents: gpd.GeoDataFrame = gpd.read_file("data/ne_110m_coastline/ne_110m_coastline.shp").to_crs(epsg=4326)

fig, ax = plt.subplots()
# continents.plot(ax=ax)

for mmsi in mmsis:
    to_plot = gdf.loc[gdf['MMSI'] == mmsi][['BaseDateTime', 'SOG', 'Heading', 'VesselName', 'geometry']]
    to_plot.plot(ax=ax)

plt.show()