from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app_models import RecommendationsRequest
from predict import predict_multiple_carparks_same_timestamp
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

# load carparkavail data
# specify the data type
dtype_spec = {
    'carpark_id': 'string',
    'area': 'category',
    'development': 'category',
    'location': 'string',
    'available_lots': 'int',
    'lot_type': 'category',
    'agency': 'category',
    'source': 'category',
    'update_datetime': 'string',
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    dataframes['init_carpark_info_df'] = pd.read_csv('carpark_information.csv',encoding='cp1252')
    dataframes['init_carpark_avail_df'] = pd.read_csv("raw_carpark_avail_020325_290325.csv", dtype=dtype_spec, parse_dates=['timestamp'])
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
    print(f'Processing starting with postal code {recommendationRequest.postal_code} and timestamp {recommendationRequest.prediction_timestamp}')
    print('Onemap token' + tokens['onemap_token'])

    carpark_info_df = dataframes['init_carpark_info_df']
    avail_df = dataframes['init_carpark_avail_df']
    my_latitude, my_longitude = getGeolocationByPostalCode(recommendationRequest.postal_code)
    carpark_info_df['distance'] = carpark_info_df.apply(getDistance, axis=1, my_latitude=my_latitude, my_longitude=my_longitude)
    carpark_info_df_sorted = carpark_info_df.sort_values('distance')

    carpark_info_top_n = carpark_info_df_sorted.head(TOP_N).to_json(orient="records")
    carpark_info_top_n_json = json.loads(carpark_info_top_n)

    # TODO: everything below can be refactored, too messy, needs comments

    top_n_with_walking_dist = []
    for record in carpark_info_top_n_json:
        total_time, total_distance = get_walking_distance_time(my_latitude, my_longitude, record['latitude'], record['longitude'], tokens['onemap_token'])
        record['total_time_in_min'] = total_time
        record['total_distance_in_km'] = total_distance
        top_n_with_walking_dist.append(record)
    
    top_n_carpark_codes = list(map(lambda n: n['carpark_id'], top_n_with_walking_dist))
    prediction_dict=predict_multiple_carparks_same_timestamp(carpark_ids=top_n_carpark_codes, ts_str=recommendationRequest.prediction_timestamp, carpark_info_df=carpark_info_df, avail_df=avail_df)

    top_n_with_walking_dist_and_predicted_avail = []
    for record in top_n_with_walking_dist:
        record['predicted_availability'] = prediction_dict.get(record['carpark_id'])
        top_n_with_walking_dist_and_predicted_avail.append(record)

    result_df = pd.DataFrame.from_records(top_n_with_walking_dist_and_predicted_avail)

    result_df['normalized_predicted_availability'] = result_df['predicted_availability'] / result_df['predicted_availability'].max()
    result_df['normalized_total_distance_in_km'] = result_df['total_distance_in_km'] / result_df['total_distance_in_km'].max()
    result_df['normalized_total_distance_in_km_inverse'] = 1 - result_df['normalized_total_distance_in_km']
    result_df['recommendation_score'] = (result_df['normalized_predicted_availability'] * 0.5) + (result_df['normalized_total_distance_in_km_inverse'] * 0.5)
    result_df = result_df.sort_values(by='recommendation_score', ascending=False)
    print(result_df)

    result = result_df.to_json(orient='records')
    return json.loads(result)