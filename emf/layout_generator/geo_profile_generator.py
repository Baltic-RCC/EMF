import pandas
import json
import geopandas as gpd
import shapely
from shapely import unary_union
from pathlib import Path
import triplets

from emf.layout_generator.geo_functions import map_by_column, generate_geomap, geojson_from_dataframe
from emf.layout_generator.geo_config import *

# Load EQ profiles
eq_df = pandas.concat([
    pandas.read_RDF([Path(__file__).parent.joinpath('inputs/20240110T0030Z_1D_LT_EQ_001.xml')]),
    pandas.read_RDF([Path(__file__).parent.joinpath('inputs/20240110T0030Z_1D_LV_EQ_001.xml')]),
    pandas.read_RDF([Path(__file__).parent.joinpath('inputs/20240213T2330Z_1D_Estonia_EQ_002.xml')]),
])

rdf_map = json.load(open(Path(__file__).parent.joinpath("inputs/entsoe_v2.4.15_2014-08-07.json")))

# Load GeoJSON data
geo_df = gpd.read_file(Path(__file__).parent.joinpath('inputs/grid_data_cleaned.geojson'))

geo_df.loc[geo_df['ref'].isnull() == True, 'ref'] = geo_df['name']
geo_df.loc[geo_df['name'].isnull() == True, 'name'] = geo_df['ref']
geo_df = geo_df.dropna(subset=['name', 'ref'])


# Geographic substation data
substation_geo_df = geo_df[geo_df['power'].isin(['substation'])][['name', 'voltage', 'geometry']]
substation_geo_df['geometry'] = substation_geo_df['geometry'].centroid
substation_geo_df_330 = substation_geo_df[substation_geo_df['voltage'].str.contains('330000')]
substation_geo_df_400 = substation_geo_df[substation_geo_df['voltage'].str.contains('400000')]
substation_geo_df_330 = pandas.concat([substation_geo_df_330, substation_geo_df_400], ignore_index=True).drop_duplicates()

# Geographic line data
line_geo_df = geo_df[geo_df['power'].isin(['line'])].reset_index(drop=True)
line_geo_df_330 = line_geo_df[line_geo_df['voltage'].str.contains('330000')]
line_geo_df_400 = line_geo_df[line_geo_df['voltage'].str.contains('400000')]
line_geo_df_330 = pandas.concat([line_geo_df_330, line_geo_df_400], ignore_index=True).drop_duplicates()


# Substation EQ profile data
substations_EQ = eq_df.type_tableview("Substation").reset_index().rename(columns={'IdentifiedObject.name': 'name', })
substations = eq_df.type_tableview("VoltageLevel").reset_index(drop=True).rename(columns={'VoltageLevel.Substation': 'ID'})
substations = substations[substations['VoltageLevel.BaseVoltage'].isin(['b8e17237e0ca4fca9e4e285b80ab30d0',
                                                                        '6d63ed36bf6842f3b98995e04eed3dd0',
                                                                        '65dd04e792584b3b912374e35dec032e'])]
substations = substations.merge(substations_EQ, on="ID", how='inner')
substations_330 = substations[substations['VoltageLevel.BaseVoltage'].isin(['6d63ed36bf6842f3b98995e04eed3dd0',
                                                                            '65dd04e792584b3b912374e35dec032e'])].reset_index(drop=True)

# Line EQ profile data
lines_EQ = eq_df.type_tableview("ACLineSegment").reset_index()
lines = lines_EQ[lines_EQ['ConductingEquipment.BaseVoltage'].isin(['b8e17237e0ca4fca9e4e285b80ab30d0',
                                                                   '6d63ed36bf6842f3b98995e04eed3dd0',
                                                                   '65dd04e792584b3b912374e35dec032e'])].reset_index(drop=True)
lines.loc[lines['IdentifiedObject.shortName'].isnull() == True, 'IdentifiedObject.shortName'] = lines['IdentifiedObject.name']
lines_330 = lines[lines['ConductingEquipment.BaseVoltage'].isin(['6d63ed36bf6842f3b98995e04eed3dd0',
                                                                 '65dd04e792584b3b912374e35dec032e'])].reset_index(drop=True)
# Manual mapping
for i in range(300, 600):
    if i != 330 or i != 400:
        line_geo_df_330.loc[line_geo_df_330['ref'].str.contains(f'{i}'), 'ref'] = f'LN{i}'

line_geo_df_330.loc[line_geo_df_330['name'].str.contains('LitPol-Link'), 'ref'] = 'Alytus-Elk'
line_geo_df_330.loc[line_geo_df_330['name'].str.contains('Grobiņa - Ventspils'), 'ref'] = 'LN425'
line_geo_df_330.loc[line_geo_df_330['name'].str.contains('Sindi — Harku'), 'ref'] = 'LN503'

#TODO
# Join line segments into single line (optional)
lines_alt = line_geo_df_330.groupby('ref')['geometry'].apply(lambda x: unary_union(x)).reset_index(name='geometry')

# Mapping names to geographic coordinates
lines_mapped_330 = map_by_column(lines_330, 'IdentifiedObject.shortName', line_geo_df_330, 'ref')
substations_mapped_330 = map_by_column(substations_330, 'name', substation_geo_df_330, 'name').drop(columns='name')

# lines_mapped = map_by_column(lines, 'IdentifiedObject.shortName', line_geo_df, 'ref')
# substations_mapped = map_by_column(substations, 'name', substation_geo_df, 'name').drop(columns='name')

mapping_330 = pandas.concat([substations_mapped_330, lines_mapped_330], ignore_index=True)
mapping_330['location_id'] = [uuid.uuid4() for num in range(len(mapping_330.index))]

# full_mappping = pandas.concat([substations_mapped, lines_mapped], ignore_index=True)
# full_mappping['location_id'] = [uuid.uuid4() for num in range(len(full_mappping.index))]
# locations = full_mappping.rename(columns={'ID': 'Location.PowerSystemResources', 'location_id': 'ID'})

# Generating CIM GL locations
mapping_330 = mapping_330.rename(columns={'ID': 'grid_id'})
locations = mapping_330.rename(columns={'grid_id': 'Location.PowerSystemResources', 'location_id': 'ID'})
locations['Type'] = 'Location'
locations['Location.CoordinateSystem'] = COORD_ID
locations_triplet = locations[['Type', 'ID', 'Location.PowerSystemResources', 'Location.CoordinateSystem']].melt(id_vars="ID", value_name="VALUE", var_name="KEY")

#TODO
# group locations by Location.PowerSystemResources and apply function to geometry
# c['geometry'] = a['geometry'].apply(lambda x: [Point(p) for p in list(x.coords)])
# def process_group(group):
#     result = group.agg(lambda x: x.unique()[0] if x.nunique() == 1 else None)
#     result['geometry'] = unary_union(group['geometry'].tolist())
#     return result
# a_grouped = a.groupby('Location.PowerSystemResources').apply(process_group)


# Generating CIM GL position points based on locations
position_points = locations.rename(columns={'ID': 'PositionPoint.Location'})
generated_points = pandas.DataFrame(columns=['Type',
                                             'PositionPoint.Location',
                                             'PositionPoint.sequenceNumber',
                                             'PositionPoint.xPosition',
                                             'PositionPoint.yPosition'])
new_point = pandas.DataFrame()

for num in range(len(position_points.index)):
    if gpd.GeoDataFrame(position_points['geometry'], crs="EPSG:4326").geom_type[num] == 'LineString':
        # position_points.loc[num, 'geometry'] = LineString([position_points.loc[num, 'geometry'].coords[0], position_points.loc[num, 'geometry'].coords[-1]])
        # TODO
        #  replace simplification logic with:
        # df['geometry'] = df['geometry'].apply(lambda x: x.simplify(TOLERANCE))
        position_points.loc[num, 'geometry'] = position_points.loc[num, 'geometry'].simplify(TOLERANCE)
    for point_num in range(shapely.count_coordinates(position_points['geometry'][num])):
        new_point.loc[0, 'Type'] = 'PositionPoint'
        new_point.loc[0, 'PositionPoint.Location'] = position_points.loc[num, 'PositionPoint.Location']
        new_point.loc[0, 'PositionPoint.sequenceNumber'] = point_num + 1
        new_point.loc[0, 'PositionPoint.xPosition'] = position_points.loc[num, 'geometry'].coords[point_num][0]
        new_point.loc[0, 'PositionPoint.yPosition'] = position_points.loc[num, 'geometry'].coords[point_num][1]
        generated_points = pandas.concat([generated_points, new_point])

generated_points['ID'] = [uuid.uuid4() for num in range(len(generated_points.index))]
point_triplet = generated_points.melt(id_vars="ID", value_name="VALUE", var_name="KEY")

# Exporting data frame as CMES GL profile
header = pandas.DataFrame(header_list, columns=['ID', 'KEY', 'VALUE'])
export = pandas.concat([header, locations_triplet, point_triplet])
export["INSTANCE_ID"] = INSTANCE_ID

export.export_to_cimxml(rdf_map=rdf_map,
                        namespace_map=namespace_map,
                        export_undefined=False,
                        export_type="xml_per_instance",
                        debug=False)


# Create visualisation of mapped data
generate_geomap(mapping_330[['IdentifiedObject.name', 'name', 'Type', 'voltage', 'ref', 'grid_id', 'geometry']],
                'lines_substations_mapped')
generate_geomap(position_points[['IdentifiedObject.name', 'IdentifiedObject.shortName', 'name', 'Type', 'voltage', 'ref', 'geometry']],
                'lines_substations_simplified')
generate_geomap(geo_df[['name', 'voltage', 'ref', 'geometry']],
                'lines_substations_unmapped')
generate_geomap(pandas.concat([line_geo_df_330, substation_geo_df_330])[['name', 'voltage', 'ref', 'geometry']],
                'lines_substations_330_unmapped')

grid_geojson = position_points[
    ['IdentifiedObject.name', 'IdentifiedObject.shortName', 'name', 'Type', 'voltage', 'ref', 'Location.PowerSystemResources', 'geometry']]
grid_geojson = grid_geojson.rename(columns={'Location.PowerSystemResources': 'grid_id'})
geojson_from_dataframe('BA_grid_simplified.geojson', grid_geojson)

print(f'GL profile {NAME} exported')
