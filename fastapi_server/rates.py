import pandas as pd
from datetime import datetime, timedelta, time
import math

# Function to round the input timestamp to its nearest 00 or 30mins
# Will return the datetime object
def round_to_nearest_half_hour(dt):
    minute = dt.minute
    second = dt.second
    microsecond = dt.microsecond

    dt = dt.replace(second=0, microsecond=0)

    # Round to nearest 0 or 30
    if minute < 15:
        return dt.replace(minute=0)
    elif minute < 45:
        return dt.replace(minute=30)
    else:
        # round up to next hour
        return (dt + timedelta(hours=1)).replace(minute=0)
    
# Function to takes in carpark code and input timestamp string
# Will look up the df_master to check which type of carpark is it, then call the respective function
def calculate_parking_rate(carpark_code, input_timestamp):
  # Load the cleaned version of full set carpark rate data
  df_master = pd.read_csv('Final_Combined_Table_20250413.csv', encoding='latin1')

  # Filter to only keep records whose vehicle_category is 'Car' or 'All'
  df_master = df_master[df_master['vehicle_category'].isin(['Car', 'All'])]

  # Added this to convert LTA carpark code as it is integer
  df_master['carpark_code'] = df_master['carpark_code'].astype(str)

  # Find the carpark information in df_master
  carpark_data = df_master[df_master['carpark_code'] == carpark_code]

  if not carpark_data.empty:
    source_sys = carpark_data['source_sys'].iloc[0]

    # Call another function to parse input string to nearest half hour
    input_dt = datetime.strptime(input_timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
    input_timestamp = round_to_nearest_half_hour(input_dt).strftime('%Y-%m-%dT%H:%M:%S.%f%z')

    if source_sys == 'ura':
      return calculate_ura_parking_rate(carpark_data, carpark_code, input_timestamp)
    elif source_sys == 'hdb':
      return calculate_hdb_parking_rate(carpark_data, carpark_code, input_timestamp)
    elif source_sys == 'lta':
      return calculate_lta_parking_rate(carpark_data, carpark_code, input_timestamp)
    else:
      print(f"Unknown source system: {source_sys} for carpark {carpark_code}")
      return 9999.0  # return a big rate to indicate car park source cannot be found
  else:
    print(f"Carpark code {carpark_code} not found")
    return 9999.0  # return a big rate to indicate car park code cannot be found

def calculate_lta_parking_rate(carpark_data, carpark_code, input_timestamp):
    # Load data with only relevant columns
    col = ['rate_day', 'rate_first', 'duration_minutes_first', 'time_start_standardized', 'time_end_standardized', 'carpark_code']
    df = carpark_data[col].copy()

    # Filter by carpark_code first
    df = df[df['carpark_code'] == carpark_code]

    # Remove $ and convert rate_first to float type, convert the integer carpark_code
    df['rate_first'] = df['rate_first'].astype(str).str.replace('$', '', regex=False).astype(float)
    df['carpark_code'] = df['carpark_code'].astype(str).str.strip()

    # Parse input time and check weekday/weekend
    input_dt = datetime.strptime(input_timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
    day_type = 'weekdays' if input_dt.weekday() < 5 else \
               'saturday' if input_dt.weekday() == 5 else 'sunday'

    # Filter relevant entries
    filtered = df[df['rate_day'].str.startswith(day_type)].copy()

    if filtered.empty:
        return 9999.0  # return a big rate to indicate car park cannot be found

    # Convert time columns
    filtered['start_time'] = filtered['time_start_standardized'].apply(
        lambda x: datetime.strptime(x, '%I:%M %p').time()
    )
    filtered['end_time'] = filtered['time_end_standardized'].apply(
        lambda x: datetime.strptime(x, '%I:%M %p').time()
    )
    filtered = filtered.dropna(subset=['start_time', 'end_time'])

    # Split and sort time slots
    processed_slots = []
    for _, row in filtered.iterrows():
        start = row["start_time"]
        end = row["end_time"]
        rate = row["rate_first"]
        duration_unit = row["duration_minutes_first"]
        processed_slots.append(
            {"start": start, "end": end, "rate": rate, "duration_unit": duration_unit}
        )

    # Sort by start time then end time
    processed_slots.sort(key=lambda x: (x["start"], x["end"]))

    # Parking duration
    parking_start = input_dt
    parking_end = input_dt + timedelta(hours=1)
    total_cost = 0.0
    found_valid_slot = False

    # Check each time slot
    for slot in processed_slots:
        slot_start = slot["start"]
        slot_end = slot["end"]
        rate = slot["rate"]
        duration_unit = slot["duration_unit"]

        # Initialize overlap_start and overlap_end before the conditional blocks
        overlap_start = None
        overlap_end = None

        # Case 1: Normal time slot
        if slot_start <= slot_end:
            if parking_start.time() < slot_end and parking_end.time() > slot_start:
                # Partial overlap
                overlap_start = max(parking_start.time(), slot_start)
                overlap_end = min(parking_end.time(), slot_end)
                found_valid_slot = True

        # Case 2: Overnight slot
        else:
            if parking_start.time() >= slot_start or parking_end.time() <= slot_end:
                # Split into two periods
                if parking_start.time() >= slot_start:
                    overlap_start = parking_start.time()
                    overlap_end = time(23, 59, 59)
                else:
                    overlap_start = time(0, 0)
                    overlap_end = parking_end.time()
                found_valid_slot = True

        if overlap_start is not None and overlap_end is not None:
            # Calculate overlap duration
            start_dt = datetime.combine(parking_start.date(), overlap_start)
            end_dt = datetime.combine(parking_end.date(), overlap_end)
            if slot_start > slot_end and overlap_end < overlap_start:
                end_dt += timedelta(days=1)

            overlap_seconds = (end_dt - start_dt).total_seconds()
            if overlap_seconds <= 0:
                continue

            # Apply rates
            mins = overlap_seconds / 60
            rate = slot["rate"]
            duration_unit = slot["duration_unit"]

            if duration_unit == 0:
                continue
            elif duration_unit == 9999: # duration_unit = 9999 means one-time entry fee
                total_cost += rate
            else:
                total_cost += math.ceil(mins / duration_unit) * rate

    if not found_valid_slot:
        return 9999.0 # return a big rate to indicate the input timing of the carpark cannot be found

    return round(total_cost, 2)

def calculate_ura_parking_rate(carpark_data, carpark_code, input_timestamp):
    from datetime import datetime, timedelta
    # Load data with only relevant columns
    col = ['rate_day', 'rate_first', 'duration_minutes_first','time_start_standardized', 'time_end_standardized', 'carpark_code']
    df = carpark_data[col].copy()

    # Filter by carpark_code first
    df = df[df['carpark_code'] == carpark_code]

    #For rows with duration = 510mins, remove it from the dataset
    df = df[df['duration_minutes_first'] != 510]

    # Remove $ and convert rate_first to float type
    df['rate_first'] = df['rate_first'].astype(str).str.replace('$', '', regex=False).astype(float)

    # Parse input time and check weekday/weekend
    input_dt = datetime.strptime(input_timestamp, '%Y-%m-%dT%H:%M:%S.%f%z') # %Y-%m-%dT%H:%M:%S.%f%z
    day_type = 'Weekday' if input_dt.weekday() < 5 else \
              'Saturday' if input_dt.weekday() == 5 else 'Sunday'

    # Filter relevant entries
    filtered = df[df['rate_day'] == day_type].copy()

    if filtered.empty:
        return 9999.0 # return a big rate to indicate unable to find the rate

    # Convert time columns
    filtered['start_time'] = filtered['time_start_standardized'].apply(
        lambda x: datetime.strptime(x, '%I.%M %p').time()
    )
    filtered['end_time'] = filtered['time_end_standardized'].apply(
        lambda x: datetime.strptime(x, '%I.%M %p').time()
    )

    # Split and sort time slots
    processed_slots = []
    for _, row in filtered.iterrows():
        start = row['start_time']
        end = row['end_time']
        rate = row['rate_first']
        duration_unit = row['duration_minutes_first']
        processed_slots.append({'start': start,'end': end,'rate': rate,'duration_unit': duration_unit})

    # Sort by start time then end time
    processed_slots.sort(key=lambda x: (x['start'], x['end']))

    # Parking duration
    parking_start = input_dt
    parking_end = input_dt + timedelta(hours=1)
    total_cost = 0.0

    # Check each time slot
    for slot in processed_slots:
        slot_start = slot['start']
        slot_end = slot['end']
        rate = slot['rate']
        duration_unit = slot['duration_unit']

        # Initialize overlap_start and overlap_end before the conditional blocks
        overlap_start = None
        overlap_end = None

        # Case 1: Normal time slot
        if slot_start <= slot_end:
            if parking_start.time() >= slot_start and parking_end.time() <= slot_end:
                # Full duration in this slot
                duration = 60
                # Check for zero duration_unit before division
                if duration_unit == 0:
                    continue # Skip this slot if duration_unit is 0
                total_cost += (duration / duration_unit) * rate
                break

            elif parking_start.time() < slot_end and parking_end.time() > slot_start:
                # Partial overlap
                overlap_start = max(parking_start.time(), slot_start)
                overlap_end = min(parking_end.time(), slot_end)

        # Case 2: Overnight slot
        else:
            if (parking_start.time() >= slot_start or parking_end.time() <= slot_end):
                # Split into two periods
                if parking_start.time() >= slot_start:
                    overlap_start = parking_start.time()
                    overlap_end = time(23,59,59)
                else:
                    overlap_start = time(0,0)
                    overlap_end = parking_end.time()

        if overlap_start is not None and overlap_end is not None:
          # Calculate overlap duration
          start_dt = datetime.combine(parking_start.date(), overlap_start)
          end_dt = datetime.combine(parking_end.date(), overlap_end)
          if slot_start > slot_end and overlap_end < overlap_start:
              end_dt += timedelta(days=1)

          overlap_seconds = (end_dt - start_dt).total_seconds()
          if overlap_seconds <= 0:
              continue

          # Apply rates
          mins = overlap_seconds / 60
          rate = slot['rate']
          duration_unit = slot['duration_unit']

          if duration_unit == 0:
              continue

          total_cost += math.ceil(mins / duration_unit) * rate
    return round(total_cost, 2)

def calculate_hdb_parking_rate(carpark_data, carpark_id, input_timestamp):
    cols = [
        'rate_day','open_ind','rate_first', 'duration_minutes_first',
        'time_start_standardized', 'time_end_standardized','carpark_code','free_parking','night_parking'
    ]
    carpark_data = carpark_data[cols].copy()
    carpark_data['rate_first'] = carpark_data['rate_first'].astype(str).str.replace('$', '', regex=False).astype(float)

    # Parse input time
    try:
        input_dt = datetime.strptime(input_timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
        input_time = input_dt.time()
        end_dt = input_dt + timedelta(hours=1) # Parking duration is always 1 hour
        end_time = end_dt.time()
    except ValueError:
        return "Invalid timestamp format"

    # Step 1: Check whether the carpark is open
    # Have checked all HDB records that all records for 1 carpark only have 1 status: TRUE or FALSE
    if not carpark_data['open_ind'].all():
        return 9999.0 # return a big rate to indicate carpark is closed

    # Step 2: Determine day type
    weekday_num = input_dt.weekday()
    if weekday_num < 5:
        day_filter = ['Weekday', 'All Days']
    else:
        day_filter = ['Sunday', 'All Days']


    # Filter the DataFrame using boolean indexing
    filtered = carpark_data[carpark_data['rate_day'].isin(day_filter)]
    filtered.reset_index(drop=True, inplace=True) # reset the index
    filtered = filtered.copy()

    # Convert 'time_start_standardized' & 'time_end_standardized' to 'start_time'
    filtered.loc[:, 'start_time'] = filtered['time_start_standardized'].apply(
        lambda x: datetime.strptime(x, '%I:%M %p').time()
    )

    filtered.loc[:, 'end_time'] = filtered['time_end_standardized'].apply(
        lambda x: datetime.strptime(x, '%I:%M %p').time()
    )

    filtered = filtered.dropna(subset=['start_time', 'end_time'])

    # Process time slots
    total_cost = 0
    coverage = timedelta(0)
    processed_slots = []

    # Sort the filtered df by start time and end time
    filtered = filtered.sort_values(by=['start_time', 'end_time'])

    # Step 3. Handle car parks which don't allow night parking
    #(all records of carpark will only have 1 value for night parking, either YES or NO)
    if(filtered['night_parking'].iloc[0] == 'NO'):
      if(input_time<filtered['start_time'].iloc[0] or end_time>filtered['end_time'].iloc[-1]):
        return 9999.0 # return a big rate to indicate carpark does not allow night parking

    # Step 4. Handle car parks with free parking for Sunday from 1pm-10:30pm
    # these records don't have a rate = 0 row hence need special handling
    if(filtered['free_parking'].iloc[0] == 'SUN & PH FR 1PM-10.30PM'):
      start_time_free = time(13,0)
      end_time_free = time(22,30)
      # when input start is 13:00 or input end is 22:30, rate is 0
      if(input_time>=start_time_free and end_time<=end_time_free):
        return 0
      # when input start is 12:30 or input end is 23:00, rate is 0.6
      elif(input_time == time(12,30) or end_time == (23,00)):
        return 0.6

    # Step 5. Split overnight slots and store all time ranges
    # Overnight slots like 22:00 - 07:00 will be split into 2 slots:
    # 22:00 - 23:59, 00:00 - 07:00
    for _, row in filtered.iterrows():
        start = row['start_time']
        end = row['end_time']
        rate = row['rate_first']
        duration = row['duration_minutes_first']

        if start > end:  # Overnight slot
            processed_slots.append({
                'start': start,
                'end': time(23,59,59),
                'rate': rate,
                'duration': duration
            })
            processed_slots.append({
                'start': time(0,0),
                'end': end,
                'rate': rate,
                'duration': duration
            })
        else: # normal slot
            processed_slots.append({
                'start': start,
                'end': end,
                'rate': rate,
                'duration': duration
            })

    # Step 6. Sort slots by start time and end time, handle overlap & overnight cases
    processed_slots.sort(key=lambda x: (x['start'], x['end']))

    parking_start = input_dt
    parking_end = input_dt + timedelta(hours=1)

    # Check each time slot
    for slot in processed_slots:
        slot_start = slot['start']
        slot_end = slot['end']
        slot_rate = slot['rate']
        slot_duration = slot['duration']

        # Calculate overlap
        if slot_start <= slot_end:
            # Daytime slot
            overlap_start = max(input_time, slot_start)
            overlap_end = min(end_time, slot_end)
            if overlap_start >= overlap_end:
                continue
        else:
            # Overnight slot
            if input_time <= slot_end or end_time >= slot_start:
                overlap_start = max(input_time, slot_start)
                overlap_end = min(end_time, slot_end)
            else:
                continue

        # Handle date crossover
        start_date = parking_start.date()
        end_date = parking_end.date()

        start_dt = datetime.combine(start_date, overlap_start)
        end_dt = datetime.combine(end_date, overlap_end)

        # Calculate duration in minutes
        overlap_duration = end_dt - start_dt
        if overlap_duration.total_seconds() <= 0:
            continue

        coverage += overlap_duration
        minutes = overlap_duration.total_seconds() / 60

        # Step 7. Calculate parking rate
        if slot_duration == 0:  # Free parking
            continue
        units = math.ceil(minutes / slot_duration)
        total_cost += slot_rate * units


    # Validate full coverage
    required_coverage = timedelta(hours=1)
    if coverage < required_coverage - timedelta(seconds=1):
        return 9999.0 # return a big rate to indicate unable to find rate

    return round(total_cost, 2)