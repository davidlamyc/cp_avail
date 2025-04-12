import pandas as pd
import pyproj

## HDB

# Define CRS
# https://github.com/hxchua/datadoubleconfirm/blob/master/notebooks/OneMapSG_XY_LatLon.ipynb
crs_singapore = pyproj.CRS.from_epsg(3414)
crs_wgs84 = pyproj.CRS.from_epsg(4326)

# Create a transformer
transformer = pyproj.Transformer.from_crs(crs_singapore, crs_wgs84,always_xy=True)

def getLatitude(row):
    longitude, latitude = transformer.transform(row['x_coord'], row['y_coord'])
    return latitude

def getLongitude(row):
    longitude, latitude = transformer.transform(row['x_coord'], row['y_coord'])
    return longitude

hdb_df = pd.read_csv('HDBCarparkInformation.csv')

hdb_df['latitude'] = hdb_df.apply(getLatitude, axis=1)
hdb_df['longitude'] = hdb_df.apply(getLongitude, axis=1)
hdb_result_df = hdb_df[['car_park_no','address','latitude','longitude']]
hdb_result_df = hdb_result_df.rename(columns={
    'car_park_no': 'carpark_id',
    'address': 'name'
})

lta_df = pd.read_csv('LTACarparkInformation.csv')
lta_result_df = lta_df[['carpark_id','name','latitude','longitude']]

agg_df = pd.concat([hdb_result_df, lta_result_df], axis=0)

hdb_result_df.to_csv('AggCarparkInformation.csv', encoding='utf-8', index=False)
