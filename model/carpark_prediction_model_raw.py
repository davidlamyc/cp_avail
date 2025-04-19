import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder
# import matplotlib.pyplot as plt
import pickle


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

# load carparkavail data
carpark_avail = pd.read_csv("raw_carpark_avail_020325_130425.csv",
                            dtype=dtype_spec, parse_dates=['timestamp'])

df_cp1 = carpark_avail[carpark_avail['carpark_id'] == 'A11']
df_cp1 = df_cp1.sort_values('timestamp')

# filter 'C' lot_type
carpark_avail = carpark_avail[(carpark_avail['lot_type']=='C')]

# drop total_lots and area
carpark_avail = carpark_avail.drop(columns=['total_lots'])
carpark_avail = carpark_avail.drop(columns=['area'])

# drop duplicates
carpark_avail = carpark_avail[~((carpark_avail['agency'] == 'HDB') & (carpark_avail['source'] == 'lta'))]
carpark_avail = carpark_avail.drop(columns=['source'])
carpark_avail = carpark_avail.drop(columns=['agency'])

carpark_avail['hour'] = carpark_avail['timestamp'].dt.hour
carpark_avail['minute'] = carpark_avail['timestamp'].dt.minute
carpark_avail['day_of_week'] = carpark_avail['timestamp'].dt.dayofweek
carpark_avail['is_weekend'] = carpark_avail['day_of_week'].isin([5, 6]).astype(int)

# Drop rows with missing target
carpark_avail = carpark_avail.dropna(subset=['available_lots'])

# Drop records with invalid available_lots values (<0 or >5000)
carpark_avail = carpark_avail[(carpark_avail['available_lots'] >= 0) & (carpark_avail['available_lots'] <= 5000)]

# load carparkinfo data
# carpark_info = pd.read_csv("carpark_information.csv")
carpark_info = pd.read_csv('carpark_information.csv',encoding='cp1252')
carpark_info['carpark_id'] = carpark_info['carpark_id'].astype(str)
carpark_info = carpark_info.dropna(subset=['area'])

df = pd.merge(carpark_avail, carpark_info[['carpark_id', 'area', 'agency', 'total_lots']],
              on='carpark_id', how='inner')

print(df.head())

# check time range
start_time = df['timestamp'].min()
end_time = df['timestamp'].max()
print(f"Time range in dataset: {start_time} → {end_time}")

def add_lag_features_per_carpark(df, target_col='available_lots', lags=[24]):
    """
    Sort by carpark_id & timestamp, then create lag features.
    Removes rows where lag_24 is NaN.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing 'carpark_id', 'timestamp', and the target_col.
    target_col : str, default='available_lots'
        The column we want to lag.
    lags : list, default=[24]
        A list of lag offsets in 'rows'. Typically, 24 means 24 rows prior
        for hourly data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new lag_{lag} column for each lag in `lags`.
        Rows with NaN lag values are dropped.
    """
    df = df.sort_values(['carpark_id', 'timestamp']).copy()

    for lag in lags:
        df[f'lag_{lag}'] = df.groupby('carpark_id')[target_col].shift(lag)

    # Drop rows where lag_24 is NaN
    df = df[df['lag_24'].notna()]
    return df

# Label encode 'area','agency'
le_area = LabelEncoder()
le_agency = LabelEncoder()
df['area_encoded'] = le_area.fit_transform(df['area'])
df['agency_encoded'] = le_agency.fit_transform(df['agency'])

# One-Hot Encode 'carpark_id'
carpark_dummies = pd.get_dummies(df['carpark_id'], prefix='carpark')

# concat df and carpark_dummies
df = pd.concat([df, carpark_dummies], axis=1)

df = add_lag_features_per_carpark(df=df)

# Sort by Timestamp
df = df.sort_values(by='timestamp').reset_index(drop=True)

feature_columns = [
    'hour', 'day_of_week', 'is_weekend','total_lots',
    'area_encoded', 'agency_encoded', 
    'lag_24'
] + list(carpark_dummies.columns) 

# feature_columns = ['hour', 'day_of_week', 'is_weekend','total_lots','area_encoded', 'agency_encoded', 'lag_24','rolling_mean_3', 'rolling_std_3'
# ] + list(carpark_dummies.columns) 
X = df[feature_columns]
y = df['available_lots']

# Time-based split: train, test
split_index = int(len(df) * 0.8)
X_train = X.iloc[:split_index]
y_train = y.iloc[:split_index]
X_test = X.iloc[split_index:]
y_test = y.iloc[split_index:]

## best model -- get the hyperparameter using optuna
best_model =  xgb.XGBRegressor(
    objective='reg:squarederror',
    n_estimators=163,
    max_depth=10,
    learning_rate=0.1030,
    subsample=0.9682,
    colsample_bytree=0.9914,
    random_state=42
)
best_model.fit(X_train, y_train)

# Evaluate performance
y_pred_best =best_model.predict(X_test)

best_mse = mean_squared_error(y_test, y_pred_best)
best_rmse = np.sqrt(best_mse)
print(f"Best Model RMSE: {best_rmse:.2f}")

best_r2 = r2_score(y_test, y_pred_best)
print(f"Best Model R²:", best_r2)

# Save the XGB best model
with open("xgb_carpark_best_model.pkl", "wb") as f:
    pickle.dump(best_model, f)

# Save the XGB model
# with open("xgb_carpark_model.pkl", "wb") as f:
#     pickle.dump(model, f)

# Save the fitted LabelEncoders
with open("le_area.pkl", "wb") as f:
    pickle.dump(le_area, f)
    
with open("le_agency.pkl", "wb") as f:
    pickle.dump(le_agency, f)

# 1) Create a list of distinct carpark IDs used in training
carpark_ids_list = df['carpark_id'].unique().tolist()

# 2) Save (pickle) this list
with open("carpark_id_list.pkl", "wb") as f:
    pickle.dump(carpark_ids_list, f)

print("Distinct carpark IDs saved:", len(carpark_ids_list))

with open("training_feature_columns.pkl", "wb") as f:
    pickle.dump(feature_columns, f)