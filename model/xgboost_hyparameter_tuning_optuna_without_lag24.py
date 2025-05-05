import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import root_mean_squared_error
from sklearn.metrics import r2_score
from sklearn.preprocessing import LabelEncoder
import time
import optuna
import holidays

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
carpark_avail = pd.read_csv(r"./dataset/raw_carpark_avail_020325_290325.csv",
                            dtype=dtype_spec, parse_dates=['timestamp'])

# Filter only 'C' lot_type
carpark_avail = carpark_avail[carpark_avail['lot_type'] == 'C']

# Drop total_lots and area from the raw availability data (we will re-add area from the info DataFrame)
carpark_avail = carpark_avail.drop(columns=['total_lots', 'area'])

# Drop duplicates: remove rows where 'agency' == 'HDB' and 'source' == 'lta'
carpark_avail = carpark_avail[~((carpark_avail['agency'] == 'HDB') & (carpark_avail['source'] == 'lta'))]

# Drop 'source' column
carpark_avail = carpark_avail.drop(columns=['source'])

# Extract time-based features
carpark_avail['hour'] = carpark_avail['timestamp'].dt.hour
carpark_avail['day_of_week'] = carpark_avail['timestamp'].dt.dayofweek
carpark_avail['is_weekend'] = carpark_avail['day_of_week'].isin([5, 6]).astype(int)

# add public holiday as feature
sg_holidays = holidays.Singapore()

carpark_avail['date'] = carpark_avail['timestamp'].dt.date
carpark_avail['is_holiday'] = carpark_avail['date'].apply(lambda d: int(d in sg_holidays))

# Drop rows with missing target
carpark_avail = carpark_avail.dropna(subset=['available_lots'])

# Drop records with invalid available_lots (<0 or >2870)
carpark_avail = carpark_avail[(carpark_avail['available_lots'] >= 0) & (carpark_avail['available_lots'] <= 2870)]

# load carparkinfo data
carpark_info = pd.read_csv(r"./dataset/carpark_information.csv")
carpark_info['carpark_id'] = carpark_info['carpark_id'].astype(str)
carpark_info = carpark_info.dropna(subset=['area'])

df = pd.merge(carpark_avail, carpark_info[['carpark_id', 'area', 'total_lots']],
              on='carpark_id', how='inner')

# check time range
start_time = df['timestamp'].min()
end_time = df['timestamp'].max()
print(f"Time range in dataset: {start_time} → {end_time}")


# # add lag features
# def add_lag_features_per_carpark(df, target_col='available_lots', lags=[24]):
#     df = df.sort_values(['carpark_id', 'timestamp']).copy()
#
#     for lag in lags:
#         df[f'lag_{lag}'] = df.groupby('carpark_id')[target_col].shift(lag)
#
#     # Drop rows where lag_24 is NaN
#     df = df[df['lag_24'].notna()]
#     return df
#
#
# df = add_lag_features_per_carpark(df, target_col='available_lots', lags=[24])

# Label encode 'area','agency','lot_type'
le_agency = LabelEncoder()
le_area = LabelEncoder()
le_carpark = LabelEncoder()

df['carpark_encoded'] = le_carpark.fit_transform(df['carpark_id'])
df['area_encoded'] = le_area.fit_transform(df['area'])
df['agency_encoded'] = le_agency.fit_transform(df['agency'])

# # One-Hot Encode 'carpark_id'
# carpark_dummies = pd.get_dummies(df['carpark_id'], prefix='carpark')

# # concat df and carpark_dummies
# df = pd.concat([df, carpark_dummies], axis=1)

# Sort by Timestamp
df = df.sort_values(by='timestamp').reset_index(drop=True)

# feature_columns = [
#                       'hour',
#                       'day_of_week',
#                       'is_weekend',
#                       'is_holiday',
#                       'lag_24',
#                       'total_lots',
#                       'area_encoded',
#                       'agency_encoded'
#                   ] + list(carpark_dummies.columns)

feature_columns = [
                      'hour',
                      'day_of_week',
                      'is_weekend',
                      'is_holiday',
                      # 'lag_24',
                      'total_lots',
                      'area_encoded',
                      'agency_encoded',
                      'carpark_encoded'
]

X = df[feature_columns]
y = df['available_lots']

# Train-test split
split_index = int(len(df) * 0.8)
X_train = X.iloc[:split_index]
y_train = y.iloc[:split_index]
X_test = X.iloc[split_index:]
y_test = y.iloc[split_index:]


def run_optuna_xgb_timeseries_tuning(X_train, y_train, X_test, y_test, n_trials=20):
    # Use time-aware split (80% train, 20% val)
    split_index = int(len(X_train) * 0.8)
    X_tune, X_val = X_train.iloc[:split_index], X_train.iloc[split_index:]
    y_tune, y_val = y_train.iloc[:split_index], y_train.iloc[split_index:]

    def objective(trial):
        params = {
            'objective': 'reg:squarederror',
            'eval_metric': 'rmse',
            'n_estimators': trial.suggest_int('n_estimators', 100, 200),
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
            'subsample': trial.suggest_float('subsample', 0.6, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
            'random_state': 42
        }

        model = xgb.XGBRegressor(**params)
        model.fit(X_tune, y_tune,
                  eval_set=[(X_val, y_val)],
                  verbose=False)

        y_pred = model.predict(X_val)
        rmse = root_mean_squared_error(y_val, y_pred)
        return rmse

    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

    print("\n✅ Best RMSE:", study.best_value)
    print("✅ Best Parameters:", study.best_params)

    best_model = xgb.XGBRegressor(**study.best_params)
    best_model.fit(X_train, y_train)

    # Evaluate on test set
    y_pred_test = best_model.predict(X_test)
    rmse_test = root_mean_squared_error(y_test, y_pred_test)
    r2_test = r2_score(y_test, y_pred_test)
    print(f"\n📈 Final Test RMSE: {rmse_test:.4f}")
    print(f"📊 Final Test R²: {r2_test:.4f}")

    return best_model


best_model = run_optuna_xgb_timeseries_tuning(X_train, y_train, X_test, y_test, n_trials=20)

y_pred = best_model.predict(X_test)

# 获取 feature importance 并配合特征名显示
importances = best_model.feature_importances_
importance_df = pd.DataFrame({
    'feature': feature_columns,
    'importance': importances
}).sort_values(by='importance', ascending=False)

# top 20 important feature
print(importance_df.head(20))
