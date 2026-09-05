import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error

from snowflake.snowpark import Session
from snowflake.ml.registry import Registry
from snowflake.ml.model import task
from snowflake.ml.experiment import ExperimentTracking

import config

class DataFrameDictVectorizer(BaseEstimator, TransformerMixin):
    """Custom Transformer to convert pandas DataFrame rows into dictionary format for DictVectorizer."""
    def __init__(self, categorical_cols=None, numerical_cols=None):
        self.categorical_cols = categorical_cols or ['PULOCATIONID', 'DOLOCATIONID']
        self.numerical_cols = numerical_cols or ['TRIP_DISTANCE']
        self.dv = DictVectorizer(sparse=False)

    def fit(self, X, y=None):
        df = X.copy()
        for col in self.categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
        dicts = df[self.categorical_cols + self.numerical_cols].to_dict(orient='records')
        self.dv.fit(dicts)
        return self

    def transform(self, X):
        df = X.copy()
        for col in self.categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
        dicts = df[self.categorical_cols + self.numerical_cols].to_dict(orient='records')
        return self.dv.transform(dicts)

def train_and_register_model(session: Session, training_dataset=None):
    """
    Train Random Forest model pipeline, track experiments using Snowflake ML Experiment,
    and register model in Snowflake Model Registry (ML_DB.MODELS).
    """
    print("\n=== STAGE 2: MODEL TRAINING & REGISTRATION ===")

    # 1. Load Training Data from Snowflake
    if training_dataset is not None:
        print("Loading training dataset from Snowflake Feature Store...")
        training_sp_df = training_dataset.read.to_snowpark_dataframe()
    else:
        print(f"Loading engineered feature dataset from {config.ENGINEERED_FEATURES_TABLE}...")
        training_sp_df = session.table(config.ENGINEERED_FEATURES_TABLE)

    training_pdf = training_sp_df.to_pandas()
    print(f"Loaded {len(training_pdf)} rows for training.")

    # 2. Separate Features and Target
    feature_cols = ['PULOCATIONID', 'DOLOCATIONID', 'TRIP_DISTANCE']
    X_train = training_pdf[feature_cols]
    y_train = training_pdf['DURATION']

    # 3. Create Model Pipeline (Vectorizer + Regressor)
    print("Building Scikit-Learn training pipeline...")
    model_pipeline = Pipeline([
        ('vectorizer', DataFrameDictVectorizer(
            categorical_cols=['PULOCATIONID', 'DOLOCATIONID'],
            numerical_cols=['TRIP_DISTANCE']
        )),
        ('regressor', RandomForestRegressor(
            n_estimators=100,
            max_depth=20,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1
        ))
    ])

    print("Fitting Random Forest model...")
    model_pipeline.fit(X_train, y_train)

    # 4. Model Evaluation
    y_pred = model_pipeline.predict(X_train)
    rmse = float(root_mean_squared_error(y_train, y_pred))
    print(f"Training Complete! Root Mean Squared Error (RMSE): {rmse:.4f}")

    # 5. ML Experiment Tracking
    print(f"Tracking experiment run '{config.EXPERIMENT_NAME}'...")
    try:
        exp = ExperimentTracking(session=session, experiment_name=config.EXPERIMENT_NAME)
        with exp.start_run(run_name="RF_DURATION_RUN_V1") as run:
            run.log_metrics({
                "rmse": rmse,
                "training_samples": len(X_train),
                "num_features": X_train.shape[1]
            })
            print(f"Logged metrics to experiment run ID: {run.run_id}")
    except Exception as e:
        print(f"Experiment tracking log warning (proceeding with registry): {e}")

    # 6. Snowflake Model Registry Logging
    print(f"Registering model '{config.MODEL_NAME}' in Snowflake Model Registry ({config.DATABASE}.{config.MODEL_SCHEMA})...")
    registry = Registry(
        session=session,
        database_name=config.DATABASE,
        schema_name=config.MODEL_SCHEMA
    )

    registered_model = registry.log_model(
        model=model_pipeline,
        model_name=config.MODEL_NAME,
        version_name=config.MODEL_VERSION,
        sample_input_data=X_train.head(10),
        task=task.Task.REGRESSION,
        metrics={
            "rmse": rmse,
            "training_samples": len(X_train)
        },
        description="Random Forest Regressor pipeline predicting taxi trip duration in minutes",
        overwrite=True
    )

    print(f"Model '{config.MODEL_NAME}' version '{config.MODEL_VERSION}' logged successfully in Model Registry!")
    return registry, registered_model

if __name__ == "__main__":
    session = config.get_snowflake_session()
    train_and_register_model(session)

