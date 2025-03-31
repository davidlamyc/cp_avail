from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app_models import Item, RecommendationsRequest
import json
import pandas as pd
import math
import requests
from haversine import haversine, Unit
import os
from dotenv import load_dotenv # Comment out before building docker image

dataframes = {}
tokens = {}
TOP_N = 5

load_dotenv() # Comment out before building docker image

MY_EMAIL = os.getenv('MY_EMAIL') # replace before building docker image
MY_PASSWORD = os.getenv('MY_PASSWORD') # replace before building docker image

@asynccontextmanager
async def lifespan(app: FastAPI):
    dataframes['init_carpark_info_df'] = pd.read_csv('carpark_information.csv',encoding='cp1252')
    tokens['onemap_token'] = getOnemapAuthToken()
    yield
    dataframes.clear()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def getOnemapAuthToken():
    url = "https://www.onemap.gov.sg/api/auth/post/getToken"
    payload = {
        "email": MY_EMAIL,
        "password": MY_PASSWORD
    }
    response = requests.post(url, json=payload)
    response_json = response.json()
    access_token = response_json['access_token']
    return access_token

def getOnemapSearchUrl(postal_code):
    return f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={postal_code}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
    
def getDistance(row, my_latitude, my_longitude):
    source = (row['latitude'],row['longitude'])
    destination = (my_latitude, my_longitude)
    return haversine(source,destination)

def getGeolocationByPostalCode(postal_code):
    try:
        response = requests.get(getOnemapSearchUrl(postal_code))
        json_response = response.json()
        array_first_item = json_response['results'][0]
        my_latitude = float(array_first_item['LATITUDE'])
        my_longitude = float(array_first_item['LONGITUDE'])
        return my_latitude, my_longitude
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail=f'postal code {postal_code} not valid')
    
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
def get_recommendations(recommendationRequest:RecommendationsRequest):
    print(f'Processing starting with postal code {recommendationRequest.postal_code}')
    print('Onemap token' + tokens['onemap_token'])

    carpark_info_df = dataframes['init_carpark_info_df']
    my_latitude, my_longitude = getGeolocationByPostalCode(recommendationRequest.postal_code)
    carpark_info_df['distance'] = carpark_info_df.apply(getDistance, axis=1, my_latitude=my_latitude, my_longitude=my_longitude)
    carpark_info_df_sorted = carpark_info_df.sort_values('distance')
    
    print(carpark_info_df_sorted.head(TOP_N))

    carpark_info_top_n = carpark_info_df_sorted.head(TOP_N).to_json(orient="records")
    carpark_info_top_n_json = json.loads(carpark_info_top_n)

    result = []
    for record in carpark_info_top_n_json:
        total_time, total_distance = get_walking_distance_time(my_latitude, my_longitude, record['latitude'], record['longitude'], tokens['onemap_token'])
        record['total_time_in_min'] = total_time
        record['total_distance_in_km'] = total_distance
        result.append(record)
    
    return result # sort by total distance