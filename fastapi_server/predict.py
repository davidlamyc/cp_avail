import pandas as pd
import numpy as np
import pickle
from datetime import timedelta
import time

##############################################
# 1) Load model & artifacts
##############################################

with open("xgb_carpark_best_model.pkl", "rb") as f:
    model = pickle.load(f)

with open("le_area.pkl", "rb") as f:
    le_area = pickle.load(f)

with open("le_agency.pkl", "rb") as f:
    le_agency = pickle.load(f)

with open("carpark_id_list.pkl", "rb") as f:
    distinct_carpark_ids = pickle.load(f)

# Also load the columns used during training
# so we can reindex at the end
with open("training_feature_columns.pkl", "rb") as f:
    training_feature_columns = pickle.load(f)


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

###############################################################################
# 1) Simple helper to check df_avail for (carpark_id, same date + hour).
###############################################################################
def check_df_for_availability(carpark_id: str, ts: pd.Timestamp, df_avail: pd.DataFrame) -> float:
    """
    Look in df_avail for the same *date* and *hour* as `ts`,
    ignoring minutes and seconds. If multiple rows exist in that hour,
    we pick the latest one (row with the maximum timestamp).
    """
    # Filter by carpark_id, year-month-day, and hour
    mask = (
        (df_avail['carpark_id'] == carpark_id)
        & (df_avail['timestamp'].dt.date == ts.date())
        & (df_avail['timestamp'].dt.hour == ts.hour)
    )
    subset = df_avail[mask]

    if not subset.empty:
        # We might pick the last row (highest timestamp) in that hour
        row = subset.loc[subset['timestamp'].idxmax()]
        return float(row['available_lots'])
    return None

###############################################################################
# 2) Carpark info lookup
###############################################################################
def get_carpark_info(carpark_id: str, carpark_info_df: pd.DataFrame):
    row = carpark_info_df[carpark_info_df['carpark_id'] == carpark_id]
    if row.empty:
        raise ValueError(f"No info found for carpark_id={carpark_id}")
    return (
        row.iloc[0]['area'],
        row.iloc[0]['agency'],
        row.iloc[0]['total_lots']
    )

def safe_label_transform(encoder, label):
    try:
        return encoder.transform([label])[0]
    except ValueError:
        return 0  # fallback

###############################################################################
# 3) Build single-row feature vector
###############################################################################
def build_feature_vector(
    carpark_id: str,
    ts: pd.Timestamp,
    lag_24: float,
    carpark_info_df: pd.DataFrame
) -> pd.DataFrame:
    hour = ts.hour
    day_of_week = ts.weekday()
    is_weekend = 1 if day_of_week in [5, 6] else 0

    area, agency, total_lots = get_carpark_info(carpark_id, carpark_info_df)
    area_encoded = safe_label_transform(le_area, area)
    agency_encoded = safe_label_transform(le_agency, agency)

    row_dict = {
        'hour': hour,
        'day_of_week': day_of_week,
        'is_weekend': is_weekend,
        'total_lots': total_lots,
        'area_encoded': area_encoded,
        'agency_encoded': agency_encoded,
        'lag_24': lag_24,
    }

    # One-hot for carpark_id
    for cp in distinct_carpark_ids:
        row_dict[f'carpark_{cp}'] = int(cp == carpark_id)

    X_row = pd.DataFrame([row_dict])
    # Reindex columns exactly as training
    X_row = X_row.reindex(columns=training_feature_columns, fill_value=0)
    return X_row

###############################################################################
# 4) Modified lag_24: only consider the same hour on the previous day
###############################################################################
def get_lag_24_value(
    carpark_id: str,
    ts: pd.Timestamp,
    carpark_info_df: pd.DataFrame,
    df_avail: pd.DataFrame,
    recursion_depth=0,
    max_depth=1
) -> float:
    """
    1) Round ts down to the hour (ignoring minute/second).
    2) Subtract 1 day to find 'same hour' on the previous day.
    3) If not found in df_avail, optionally predict that older time
       (recursion), else fallback 0.
    """
    # Rounding the current timestamp down to hour
    ts_hour = ts.replace(minute=0, second=0, microsecond=0)

    # Move 1 day back, keeping the same hour
    t_past = ts_hour - timedelta(days=1)

    val = check_df_for_availability(carpark_id, t_past, df_avail)
    if val is not None:
        return val

    if recursion_depth < max_depth:
        # Attempt to predict that older hour if not found
        return predict_availability(
            carpark_id,
            t_past.isoformat(),
            carpark_info_df,
            df_avail,
            recursion_depth=recursion_depth + 1,
            max_depth=max_depth
        )
    else:
        # Fallback if older hour not found or predicted
        return 0.0

###############################################################################
# 5) Final Predict Function (Single-Row)
###############################################################################
def predict_availability(
    carpark_id: str,
    ts_str: str,
    carpark_info_df: pd.DataFrame,
    df_avail: pd.DataFrame,
    recursion_depth=0,
    max_depth=1
) -> int:
    """
    Predict availability for one car park at a given timestamp,
    returning an integer (rounded) result.
    """
    ts = pd.to_datetime(ts_str)

    # 1) Compute lag_24
    lag_24_val = get_lag_24_value(
        carpark_id,
        ts,
        carpark_info_df,
        df_avail,
        recursion_depth=recursion_depth,
        max_depth=max_depth
    )

    # 2) Build one-row feature vector
    X_row = build_feature_vector(carpark_id, ts, lag_24_val, carpark_info_df)

    # 3) Call the model's predict method
    pred = model.predict(X_row)

    # 4) Round and convert to int
    return int(round(float(pred[0])))

def _build_row_for_carpark(
    carpark_id: str,
    ts: pd.Timestamp,
    carpark_info_df: pd.DataFrame,
    avail_df: pd.DataFrame,
    recursion_depth: int,
    max_depth: int
) -> pd.DataFrame:
    """
    Helper that:
      1) Computes lag_24 availability (possibly by recursion).
      2) Builds a single-row feature vector for the specified car park & timestamp.
    """
    lag_24_val = get_lag_24_value(
        carpark_id,
        ts,
        carpark_info_df,
        avail_df,
        recursion_depth=recursion_depth,
        max_depth=max_depth
    )
    return build_feature_vector(carpark_id, ts, lag_24_val, carpark_info_df)


def predict_multiple_carparks_same_timestamp(
    carpark_ids: list[str],
    ts_str: str,
    carpark_info_df: pd.DataFrame,
    avail_df: pd.DataFrame,
    recursion_depth: int = 0,
    max_depth: int = 1,
) -> dict[str, int]:
    """
    Predicts availability for multiple car parks at a single timestamp,
    in one batch, without writing an explicit loop.

    Parameters
    ----------
    carpark_ids : list of str
        Car park IDs for which to predict availability.
    ts_str : str
        Timestamp string (e.g., "2025-03-31 10:00:00").
    recursion_depth : int
        Current recursion depth for lag_24 lookups (default=0).
    max_depth : int
        Maximum recursion depth for lag_24 lookups (default=1).

    Returns
    -------
    dict
        Mapping of carpark_id -> predicted availability at the given timestamp.
    """
    start_time = time.time()  # Start timer

    ts = pd.to_datetime(ts_str)

    # 1) Build a list of single-row DataFrames (one per car park),
    #    using map instead of a manual loop.
    feature_dfs = list(
        map(
            lambda cp_id: _build_row_for_carpark(cp_id, ts, carpark_info_df, avail_df, recursion_depth, max_depth),
            carpark_ids
        )
    )

    # 2) Concatenate into one DataFrame for a single model.predict() call
    X_batch = pd.concat(feature_dfs, ignore_index=True)

    # 3) Batch prediction
    preds = model.predict(X_batch)
    
     # 4) Round to int
    rounded_preds = [int(round(float(x))) for x in preds]

    # 5) Build dict from carpark_ids & predictions
    end_time = time.time()  # End timer
    elapsed = end_time - start_time

    print(f"Prediction took {elapsed:.4f} seconds to process {len(carpark_ids)} carparks.")
    return dict(zip(carpark_ids, rounded_preds))