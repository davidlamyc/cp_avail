import requests
import json
import uuid
from datetime import datetime
from pytz import timezone
import boto3
from botocore.exceptions import ClientError

def get_api_key():
    secret_name = "ura_service_key"
    region_name = "ap-southeast-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    api_key = json.loads(get_secret_value_response['SecretString'])['URA_SERVICE_KEY']
    print('key: ' + api_key)
    return api_key

def fetch_token(access_key_string):
    token_url = f'https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1'
    headers = {
        'AccessKey': access_key_string,
        'User-Agent': 'Mozilla/5.0'
    }
    response = requests.get(token_url,headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return {"Error": "Data not found"}

# lambda
def fetch_ura_rates(AccessKey, token):
    url = f'https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1?service=Car_Park_Details'
    headers = {
        'AccessKey': AccessKey,
        'Token': token,
        'User-Agent': 'Mozilla/5.0'
    }
    params = {
        'service': 'Car_Park_Details'
    }
    response = requests.get(url,headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return {"Error": "Data not found"}

# lambda
def get_file_name():
    my_uuid = uuid.uuid4()
    now_sg = datetime.now(timezone('Asia/Singapore')).replace(microsecond=0)
    now_sg_date = now_sg.date()
    now_sg_time_fmt = now_sg.strftime("%H:%M:%S").replace(":", "-")
    return f'ura_carpark_{now_sg_date}_{now_sg_time_fmt}_{my_uuid}.json'

# lambda
def upload_to_s3(json_data, file_name):
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket="ura-carpark",
        Key=file_name,
        Body=json.dumps(json_data),
    )

# lambda
def lambda_handler(event, context):
    print("We are starting now!")
    api_key = get_api_key()
    token = fetch_token(api_key)
    token_retrieve = token.get("Result")
    print("token retrieval complete:", token_retrieve)

    json_data = fetch_ura_rates(api_key, token_retrieve)
    print("Rates Complete!")
    print("Uploading file to S3...")
    upload_to_s3(json_data, get_file_name())
    print("Upload complete.")
    
    return {
        'statusCode': 200,
        'body': 'Data uploaded successfully!'
    }