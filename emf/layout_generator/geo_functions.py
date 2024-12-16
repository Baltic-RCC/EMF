import geopandas as gpd
import pandas
from shapely import LineString, unary_union
from thefuzz import process


def map_by_column(data_frame_1, column_1: str, data_frame_2, column_2: str):
    return data_frame_1.merge(data_frame_2, left_on=data_frame_1[column_1].apply(
        lambda x: process.extractOne(x, data_frame_2[column_2])[0]),
                              right_on=column_2, suffixes=('_1', '_2'))


def map_by_column_new(df1, col1: str, df2, col2: str):
    map_list = df2[[col2, 'geometry']]
    new_df = pandas.DataFrame()
    for i in range(df1[[col1]].size):
        if map_list[col2].size == 0:
            break
        match = process.extractOne(df1.loc[i, col1], map_list[col2])[0]
        row1 = df1.loc[[i]].reset_index(drop=True)
        row2 = df2.loc[[df2[df2[col2].str.contains(match)].head(1).index[0]]].reset_index(drop=True)
        new_row = pandas.concat([row1, row2], axis=1)
        new_df = pandas.concat([new_df, new_row], ignore_index=True)
        map_list = map_list.drop(map_list[col2].str.contains(match).index[0])
    return new_df


def generate_geomap(df, name: str):
    geo_df = gpd.GeoDataFrame(df)
    m = geo_df.explore()
    m.save(f"{name}.html")


def geojson_from_dataframe(name_path: str, df: gpd.GeoDataFrame | pandas.DataFrame):
    geo_df = gpd.GeoDataFrame(df, crs='EPSG:4326')
    geo_df.to_file(name_path, driver='GeoJSON')


def multiline_to_linestring(multiline):
    points = [point for line in multiline for point in line.coords]
    return LineString(points)


def process_group(group):
    result = group.agg(lambda x: x.unique()[0] if x.nunique() == 1 else None)
    result['geometry'] = unary_union(group['geometry'].tolist())
    return result
