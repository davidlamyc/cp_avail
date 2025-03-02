import requests
import json
import uuid
from datetime import datetime
from pytz import timezone
# import boto3
import os
from dotenv import load_dotenv

load_dotenv()

LTA_API_KEY = os.getenv('LTA_API_KEY')

def fetch_lta_avail():
    url = f'https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2'
    headers = {
        'Content-Type': 'application/json',
        'AccountKey': LTA_API_KEY
    }
    response = requests.get(url,headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return {"Error": "Data not found"}

def get_file_name():
    my_uuid = uuid.uuid4()
    now_sg = datetime.now(timezone('Asia/Singapore')).replace(microsecond=0)
    now_sg_date = now_sg.date()
    now_sg_time_fmt = now_sg.strftime("%H:%M:%S").replace(":", "-")
    return f'lta_avail_{now_sg_date}_{now_sg_time_fmt}_{my_uuid}.json'

def upload_to_s3(json_data, file_name):
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket="lta-avail",
        Key=file_name,
        Body=json.dumps(json_data),
    )

# get lambda to write file to s3
def lambda_handler(event, context):
    json_data = fetch_lta_avail()

    upload_to_s3(json_data, get_file_name())
    
    return {
        'statusCode': 200,
        'body': 'Data uploaded successfully!'
    }

# write file to local fs
def write_local(results):
    file_name = get_file_name()
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False)

def main():
    results = fetch_lta_avail()
    print(results)
    write_local(results)

if __name__ == "__main__":
    main()