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

# Function to get walking distance & time using OneMap Routing API
def get_walking_distance_time(start_lat, start_lon, end_lat, end_lon, token):
    route_url = f"https://www.onemap.gov.sg/api/public/routingsvc/route?start={start_lat},{start_lon}&end={end_lat},{end_lon}&routeType=walk&token={token}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(route_url, headers=headers)
        response.raise_for_status()  # Check for any HTTP errors
        data = response.json()

        if "route_summary" in data:
            total_time = data["route_summary"]["total_time"]  # seconds
            total_distance = data["route_summary"]["total_distance"]  # Meters

            # Covert to min and round up total time to the nearest minute
            total_time = total_time/60
            total_time = math.ceil(total_time)

            # Convert distance from meters to kilometers and round it to 2 decimal places
            total_distance = total_distance / 1000  # Convert meters to kilometers
            total_distance = round(total_distance, 2)  # Round to 2 decimal places

            return total_time, total_distance
        else:
            print(f"Error: Route summary not found.")
            print(f"Route API Response: {data}")
            return None, None
    except Exception as e:
        print(f"Error getting walking distance/time: {e}")
        return None, None

@app.post("/recommendations")
async def get_recommendations():
    print(request.get_json()) # print request
    request_data = request.get_json()

    authUrl = "https://www.onemap.gov.sg/api/auth/post/getToken"
    authData = {
        "email": "davidlam.yc@gmail.com",
        "password": "Iamthehighway1!"
    }
    authResponse = requests.post(authUrl, json=authData)

    authResponseJson = authResponse.json()

    access_token = authResponseJson['access_token']
        
    url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={request_data['postal_code']}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
    
    response = requests.get(url)

    json_res = response.json()
    array_first_item = json_res['results'][0]
    my_latitude = float(array_first_item['LATITUDE'])
    my_longitude = float(array_first_item['LONGITUDE'])

    df = pd.read_csv('carpark_information.csv') # TODO: store carpark details in memory instead of new reads each time
    df['distance'] = df.apply(getDistance, axis=1, my_latitude=my_latitude, my_longitude=my_longitude) 
    df_sorted = df.sort_values('distance')

    print(df_sorted.head(5))

    df_sorted_top_n = df_sorted.head(5).to_json(orient="records")
    df_sorted_top_n_json = json.loads(df_sorted_top_n)

    result = []
    for record in df_sorted_top_n_json:
        total_time, total_distance = get_walking_distance_time(my_latitude, my_longitude, record['latitude'], record['longitude'], access_token)
        record['total_time_in_min'] = total_time
        record['total_distance_in_km'] = total_distance
        result.append(record)
    
    return result