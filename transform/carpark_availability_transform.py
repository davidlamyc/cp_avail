import time
import boto3

query = 'SELECT * from carpark_availability;'
DATABASE = 'carpark'
output='s3://carpark-availability-master/'

def lambda_handler(event, context):
    client = boto3.client('athena')

    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': output,
        }
    )
    return response