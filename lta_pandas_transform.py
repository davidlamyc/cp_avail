import json
import pandas as pd
import datetime

data = None
timestamp = None

# LTA transformation

lta_filename = 'lta_avail_2025-03-03_22-08-50_bc7eb704-a3fc-40d0-873f-223e75da5d1c'
filename_components = lta_filename.split('_')
print(filename_components[2].split('-'))
print(filename_components[3].split('-'))
date_components = filename_components[2].split('-')
time_components = filename_components[3].split('-')
timestamp = datetime.datetime(int(date_components[0]), int(date_components[1]), int(date_components[2]), int(time_components[0]), int(time_components[1]), int(time_components[2]), 00000).isoformat()

with open(f'{lta_filename}.json') as f:
    d = json.load(f)
    data = d['value']

df = pd.DataFrame(data)
df = df.rename(columns={
    'CarParkID': 'carpark_id',
    'Area': 'area',
    'Development': 'development',
    'Location': 'location',
    'AvailableLots': 'available_lots',
    'LotType': 'lot_type',
    'Agency': 'agency'
})
df = df.assign(timestamp=timestamp)
df = df.assign(source='lta')

df.to_csv('lta_out.csv', encoding='utf-8', index=False)