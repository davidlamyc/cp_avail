import json
import pandas as pd
import sys
import pytz
from datetime import datetime

# print ('argument list', sys.argv)
# filename = sys.argv[1]

# data = None
# timestamp = None

## HDB transformation

import glob

json_files = []
for file in glob.glob(".\input_data\*.json"):
    json_files.append(file)

for jf in json_files:
    print(jf)
    output_file_name_list = jf.split('\\')
    output_file_name_id = output_file_name_list[2].split('.')[0]
    print(">>> ", output_file_name_id)
    print
    with open(jf) as f:
        d = json.load(f)
        timestamp = d['items'][0]['timestamp']
        print(timestamp)
        dt = datetime.fromisoformat(timestamp)
        sgt = pytz.timezone('Asia/Singapore')
        dt_sgt = dt.astimezone(sgt).replace(tzinfo=None)
        # print(dt_sgt.isoformat())
        data = d['items'][0]['carpark_data']

        df = pd.DataFrame(data)
        carpark_info_explode = df.explode('carpark_info', ignore_index=True)
        carpark_info_norm = pd.json_normalize(carpark_info_explode['carpark_info'])
        carpark_info = carpark_info_explode.join(carpark_info_norm)
        carpark_info = carpark_info.drop(columns=['carpark_info'])
        carpark_info = carpark_info.assign(timestamp=dt_sgt.isoformat()) # todo: strip out utc
        carpark_info = carpark_info.rename(columns={
            'carpark_number': 'carpark_id',
            'lots_available': 'available_lots',
            'Agency': 'agency'
        })
        carpark_info = carpark_info.assign(source='hdb')
        carpark_info[['area','development','location','agency']] = None
        df_reordered = carpark_info[[
            'carpark_id',
            'area',
            'development',
            'location',
            'available_lots',
            'lot_type',
            'agency',
            'timestamp',
            'source',
            'update_datetime',
            'total_lots'
        ]]
        print(df_reordered.head(15))

        df_reordered.to_csv(f'.\output_data\{output_file_name_id}.csv', encoding='utf-8', index=False)