# import main Flask class and request object
from flask import Flask, request
import json
import pandas as pd
import math
import requests
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

def getDistance(row, my_latitude, my_longitude):
    return math.dist([row['latitude'], row['longitude']], [my_latitude, my_longitude])

def parse_csv(df):
    res = df.to_json(orient="records")
    parsed = json.loads(res)
    return parsed

@app.post("/recommendations")
async def get_recommendations():
    print(request.get_json()) # print request
    request_data = request.get_json()
      
    url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={request_data['postal_code']}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
        
    response = requests.get(url)
    print(response.json()) # print one map's response

    json_res = response.json()
    array_first_item = json_res['results'][0]
    my_latitude = float(array_first_item['LATITUDE'])
    my_longitude = float(array_first_item['LONGITUDE'])

    df = pd.read_csv('AggCarparkInformation.csv') # TODO: store carpark details in memory instead of new reads each time
    df['distance'] = df.apply(getDistance, axis=1, my_latitude=my_latitude, my_longitude=my_longitude) 
    df_sorted = df.sort_values('distance')

    print(df_sorted.head(20))

    # TODO: add model + scoring
    return parse_csv(df_sorted.head(20))