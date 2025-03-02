import requests
import json
import uuid
from datetime import datetime, timedelta
from pytz import timezone

def get_generated_datetimes(start, end, step=60):
   generated_datetimes = []
   cur = start
   while cur < end:
      cur += timedelta(minutes=step)
      generated_datetimes.append(cur)
   return generated_datetimes

def fetch_hdb_avail(datetime):
    # YYYY-MM-DD[T]HH:MM:SS
    datetime_str = datetime.isoformat()
    print('datetime_str: ' + datetime_str)
    url = 'https://api.data.gov.sg/v1/transport/carpark-availability?date_time=' + datetime_str
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(response.json())
        return {"Error": "Data not found"}

def get_file_name(dt):
    my_uuid = uuid.uuid4()
    now_sg_date = dt.date()
    now_sg_time_fmt = dt.strftime("%H:%M:%S").replace(":", "-")
    return f'hdb_avail_{now_sg_date}_{now_sg_time_fmt}_{my_uuid}.json'

# write file to local fs
def write_local(results, dt):
    file_name = get_file_name(dt)
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False)

def main():
    generated_datetimes = get_generated_datetimes(
        datetime(2025, 3, 2, 1, 30, 00, 00000), # exclusive of start datetime
        datetime(2025, 3, 2, 4, 30, 00, 00000),
        60
    )
    print(generated_datetimes)
    for dt in generated_datetimes:
        results = fetch_hdb_avail(dt)
        print(results)
        write_local(results, dt)

if __name__ == "__main__":
    main()