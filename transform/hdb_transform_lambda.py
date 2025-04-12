import json
import boto3
import pandas as pd
import pytz
from datetime import datetime
from io import StringIO

s3Client = boto3.client('s3')

def lambda_handler(event, context):
    # Get bucket and file name
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print('bucket ' + bucket)
    print('key ' + key)

    # Get object
    result = s3Client.get_object(Bucket=bucket, Key=key)
    json_str = result["Body"].read().decode('utf-8')
    json_data = json.loads(json_str)

    # Process
    timestamp = json_data['items'][0]['timestamp']
    dt = datetime.fromisoformat(timestamp)
    sgt = pytz.timezone('Asia/Singapore')
    dt_sgt = dt.astimezone(sgt).replace(tzinfo=None)
    data = json_data['items'][0]['carpark_data']
    df = pd.DataFrame(data)
    carpark_info_explode = df.explode('carpark_info', ignore_index=True)
    carpark_info_norm = pd.json_normalize(carpark_info_explode['carpark_info'])
    carpark_info = carpark_info_explode.join(carpark_info_norm)
    carpark_info = carpark_info.drop(columns=['carpark_info'])
    carpark_info = carpark_info.assign(timestamp=dt_sgt.isoformat())
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
    # print(df_reordered.head(15))

    # df.show()

    # write to carpark-availability-1303
    output_filename = key.split('.')[0]
    csv_buffer = StringIO()
    df_reordered.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('carpark-availability-1303', f'{output_filename}.csv').put(Body=csv_buffer.getvalue())

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


## HDB transformation

# with open('.\\input_data\\hdb_avail_2025-03-05_00-30-00_6d3ee153-487b-4792-900e-244930521631.json') as f:
#     d = json.load(f)
#     timestamp = d['items'][0]['timestamp']
#     print(timestamp)
#     dt = datetime.fromisoformat(timestamp)
#     sgt = pytz.timezone('Asia/Singapore')
#     dt_sgt = dt.astimezone(sgt).replace(tzinfo=None)
#     data = d['items'][0]['carpark_data']

#     df = pd.DataFrame(data)
#     carpark_info_explode = df.explode('carpark_info', ignore_index=True)
#     carpark_info_norm = pd.json_normalize(carpark_info_explode['carpark_info'])
#     carpark_info = carpark_info_explode.join(carpark_info_norm)
#     carpark_info = carpark_info.drop(columns=['carpark_info'])
#     carpark_info = carpark_info.assign(timestamp=dt_sgt.isoformat()) # todo: strip out utc
#     carpark_info = carpark_info.rename(columns={
#         'carpark_number': 'carpark_id',
#         'lots_available': 'available_lots',
#         'Agency': 'agency'
#     })
#     carpark_info = carpark_info.assign(source='hdb')
#     carpark_info[['area','development','location','agency']] = None
#     df_reordered = carpark_info[[
#         'carpark_id',
#         'area',
#         'development',
#         'location',
#         'available_lots',
#         'lot_type',
#         'agency',
#         'timestamp',
#         'source',
#         'update_datetime',
#         'total_lots'
#     ]]
#     print(df_reordered.head(15))

#     df_reordered.to_csv(f'.\output_data\hdb_avail_2025-03-05_00-30-00_6d3ee153-487b-4792-900e-244930521631.csv', encoding='utf-8', index=False)