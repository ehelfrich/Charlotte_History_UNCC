import pandas as pd
import censusdata
import re
import time
import censusgeocode as cg
import json

test = cg.onelineaddress('324 Banks Street, FortMill, NC', layers='tract')

nugget = test[0]['geographies']['Census Tracts'][0]['GEOID']


# Load CSV
input = pd.read_csv('/mnt/Eric/PycharmProjects_NAS/big_data_class/data/unc_donor_history_cleaned.csv')

# Clean
input_cleaned = input.dropna(subset=['ADDRESS1', 'CITY', 'STATECODE'])
non_carolina_index = input_cleaned[(input_cleaned['STATECODE'] != 'SC') & (input_cleaned['STATECODE'] != 'NC')].index
input_cleaned.drop(non_carolina_index, inplace=True)

# Pull Addr and ID


def create_addr(row):
    return row['ADDRESS1'] + ', ' + row['CITY'] + ', ' + row['STATECODE']


def pullgeoid(row):
    addr = row['ADDRESS1'] + ', ' + row['CITY'] + ', ' + row['STATECODE']
    print("Address: " + addr)
    time.sleep(1)
    query = cg.onelineaddress(addr, layers='tract')
    try:
        geoid = query[0]['geographies']['Census Tracts'][0]['GEOID']
    except Exception:
        geoid = 'Error'
        pass
    print("GeoID: " + geoid)

    return geoid


input_cleaned['Full_Address'] = input_cleaned.apply(create_addr, axis=1)
input_cleaned['GEOID'] = input_cleaned.apply(pullgeoid, axis=1)

#Export csv of results
input_cleaned.to_csv("addr_GeoID_no_outofstate.csv", index=False)

# Drop errored columns
geoid_input = pd.read_csv('/mnt/Eric/PycharmProjects_NAS/big_data_class/addr_GeoID_no_outofstate.csv')
error_index = geoid_input[(geoid_input['GEOID'] == 'Error')].index
geoid_input.drop(error_index, inplace=True)

geoid_input.to_csv("muesuem_geoid.csv", index=False)
