# scripts/fit.py

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_percentage_error as mape
from sklearn.metrics import root_mean_squared_error as rmsd
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from category_encoders import CatBoostEncoder
import yaml
import os
import joblib

def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    data = pd.read_csv('part2_dvc/data/initial_data.csv')

    # Разбить квартиры на две категории: построенные до 2010 года и после
    def encode_year(x):
        if 0 < x <= 2010:
            return 0.0
        return 1.0
    data['build_year'] = data['build_year'].apply(encode_year)

    # Разбить квартиры на две категории: квартира находится в доме, где больше 50 квартир или меньше
    def encode_flats_count(x):
        if 0 < x <= 50:
            return 0.0
        return 1.0
    data['flats_count'] = data['flats_count'].apply(encode_flats_count)

    binary_cat_features = data[['is_apartment', 'has_elevator']]
    other_cat_features = data[['rooms', 'building_type_int', 'ceiling_height', 'floors_total', 'floor']]
    float_num_features = data[['kitchen_area', 'living_area', 'total_area', 'latitude', 'longitude']]
    other_num_features = data[['build_year', 'flats_count']]

    binary_cols = binary_cat_features.columns.tolist()
    non_binary_cat_cols = other_cat_features.columns.tolist()
    float_num_cols = float_num_features.columns.tolist()
    other_num_cols = other_num_features.columns.tolist()

    preprocessor = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_cols),
            ('categorical', CatBoostEncoder(), non_binary_cat_cols),
            ('scalar_float', StandardScaler(), float_num_cols),
            ('binary_derived', OneHotEncoder(drop=params['one_hot_drop']), other_num_cols),
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )
    model = GradientBoostingRegressor()

    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('model', model)
    ])

    pipeline.fit(data, data[params['target_col']])

    # сохраните обученную модель в models/fitted_model.pkl
    os.makedirs('part2_dvc/models', exist_ok=True)
    joblib.dump(pipeline, 'part2_dvc/models/fitted_model.pkl')

if __name__ == '__main__':
	fit_model()
