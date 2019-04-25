import pandas as pd
import censusdata
import re
import time

search = censusdata.search('acs5', 2017, 'label', 'age of household')

df = pd.DataFrame.from_records(search)

censusdata.printtable(censusdata.censustable('acs5', 2017, 'B06011'))

# Exploration

# Metro counties (Mecklenburg 119, York, Union 179, Gaston 071, Cabarrus 025, Iredell 097,
# Rowan 159, Cleveland 045, Lancaster, Lincoln 109, Stanly 167, Chester)

# NC is state 37
censusdata.geographies(censusdata.censusgeo([('state', '*')]), 'acs5', 2017)

# Gather NC Counties
search = censusdata.geographies(censusdata.censusgeo([('state', '37'), ('county', '*')]), 'acs5', 2017)
df = pd.DataFrame.from_dict(search, orient='index')

# Gather SC Counties
censusdata.geographies(censusdata.censusgeo([('state', '*')]), 'acs5', 2017)  # SC is state 45
search = censusdata.geographies(censusdata.censusgeo([('state', '45'), ('county', '*')]), 'acs5', 2017)
df = pd.DataFrame.from_dict(search, orient='index')

#########################################################################

# Define the states and counties
# SC 45, NC 37
states = ['37', '45']

# NC counties (Mecklenburg 119, Union 179, Gaston 071, Cabarrus 025, Iredell 097, Rowan 159, Cleveland 045,
# Lincoln 109, Stanly 167)
nc_counties = ['119', '179', '071', '025', '097', '159', '045', '109', '167']

# Metro counties (York 091, Lancaster 057, Chester 023)
sc_counties = ['091', '057', '023']

# Download Data from Census.gov for the selected states and counties
data = pd.DataFrame()

for state in states:
    if state == '37':
        for county in nc_counties:
            temp_data = censusdata.download('acs5', 2017,
                                            censusdata.censusgeo([('state', state),
                                                                  ('county', county),
                                                                  ('tract', '*')]),
                                            ['B06011_001E', 'B09002_001E', 'B15003_017E', 'B15003_022E', 'B25064_001E'])
            data = data.append(temp_data)
            time.sleep(5)  # Prevent API timeout
    elif state == '45':
        for county in sc_counties:
            temp_data = censusdata.download('acs5', 2017,
                                            censusdata.censusgeo([('state', state),
                                                                  ('county', county),
                                                                  ('tract', '*')]),
                                            ['B06011_001E', 'B09002_001E', 'B15003_017E', 'B15003_022E', 'B25064_001E'])
            data = data.append(temp_data)
            time.sleep(5)  # Prevent API timeout

# Save Data as CSV for Backup
data.to_csv('data_backup_v1.csv')


# Add GeoID as a category


def pullgeoid(input):
    regex = re.findall(r"(?<=:)\d+", input)
    result = ''.join(regex)
    return result


data['GEOID'] = data.index.values.astype('str')  # Convert index into a string
data['GEOID'] = data['GEOID'].apply(pullgeoid)

# Export CSV
# data = data[['GEOID', 'B06011_001E']]
data.to_csv("census_geoid.csv", index=False)

# Prep Shapefile CSV

# Load csv of combined Census tracts for NC/SC
shape_file = pd.read_csv('/mnt/Eric/PycharmProjects_NAS/big_data_class/census_shapes_geoid.csv')

# Construct list of first parts of GEOID for counties
geo_list = set()
for state in states:
    if state == '37':
        for county in nc_counties:
            geo_list.add(state + county)
    elif state == '45':
        for county in sc_counties:
            geo_list.add(state + county)

# Convert column to string
shape_file['GEOID'] = shape_file['GEOID'].astype(str)

# Search GEOID column for just the counties we want
selection = pd.DataFrame()
for county in geo_list:
    temp_df = shape_file.loc[shape_file.iloc[:, 0].str.startswith(county)]
    selection = selection.append(temp_df, ignore_index=True)

selection.to_csv("census_shapes_geoid_trimmed.csv")
