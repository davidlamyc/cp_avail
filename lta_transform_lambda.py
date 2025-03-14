import json
import boto3
import pandas as pd
import datetime
from io import StringIO

s3Client = boto3.client('s3')

def lambda_handler(event, context):
    # Get bucket and file name
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print('bucket ' + bucket)
    print('key ' + key)

    # generate timestamp
    filename_components = key.split('_')
    date_components = filename_components[2].split('-')
    time_components = filename_components[3].split('-')
    timestamp = datetime.datetime(int(date_components[0]), int(date_components[1]), int(date_components[2]), int(time_components[0]), int(time_components[1]), int(time_components[2]), 00000).isoformat()
    print('timestamp ' + timestamp)

    # Get object
    result = s3Client.get_object(Bucket=bucket, Key=key)
    json_str = result["Body"].read().decode('utf-8')
    json_data = json.loads(json_str)
    json_result = json_data['value']
    # print(json_result)

    # Process
    df = pd.DataFrame(json_result)
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

    # df.show()

    # write to carpark-availability-1303
    output_filename = key.split('.')[0]
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('carpark-availability-1303', f'{output_filename}.csv').put(Body=csv_buffer.getvalue())

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
