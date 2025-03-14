import json
import pandas as pd
import sys

print ('argument list', sys.argv)
filename = sys.argv[1]

data = None
timestamp = None

## HDB transformation

with open(f'{filename}.json') as f:
    d = json.load(f)
    timestamp = d['items'][0]['timestamp']
    data = d['items'][0]['carpark_data']

df = pd.DataFrame(data)
carpark_info_explode = df.explode('carpark_info', ignore_index=True)
carpark_info_norm = pd.json_normalize(carpark_info_explode['carpark_info'])
carpark_info = carpark_info_explode.join(carpark_info_norm)
carpark_info = carpark_info.drop(columns=['carpark_info'])
carpark_info = carpark_info.assign(timestamp=timestamp) # todo: strip out utc
carpark_info = carpark_info.rename(columns={
    'carpark_number': 'carpark_id',
    'lots_available': 'available_lots',
    'Agency': 'agency'
})
carpark_info = carpark_info.assign(source='hdb')
print(carpark_info.head(15))

carpark_info.to_csv(f'{filename}.csv', encoding='utf-8', index=False)

