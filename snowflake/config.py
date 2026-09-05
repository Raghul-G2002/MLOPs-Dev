import os
from typing import Dict, Any
from snowflake.snowpark import Session

# Database and Schema Configurations
DATABASE = "ML_DB"
FEATURE_SCHEMA = "FEATURES"
MODEL_SCHEMA = "MODELS"
OUTPUT_SCHEMA = "OUTPUTS"
WAREHOUSE = "COMPUTE_WH"

# Table & Stage Names
RAW_TABLE = f"{DATABASE}.{FEATURE_SCHEMA}.RAW_DATA"
ENGINEERED_FEATURES_TABLE = f"{DATABASE}.{FEATURE_SCHEMA}.ENGINEERED_FEATURES"
PREDICTIONS_TABLE = f"{DATABASE}.{OUTPUT_SCHEMA}.MODEL_PREDICTIONS"

# Feature Store Names
ENTITY_NAME = "VENDOR_ID_ENTITY"
FEATURE_VIEW_NAME = "TAXI_TRIP_FEATURES"
FEATURE_VIEW_VERSION = "V1"
TRAINING_DATASET_NAME = "TAXI_TRIP_TRAINING_DATASET"
TRAINING_DATASET_VERSION = "V1"

# Model Registry & SPCS Configurations
MODEL_NAME = "TAXI_DURATION_PREDICTOR"
MODEL_VERSION = "V1"
EXPERIMENT_NAME = "Taxi Duration Prediction"
SPCS_COMPUTE_POOL = "ML_COMPUTE_POOL"
SPCS_IMAGE_REPO = f"{DATABASE}.{MODEL_SCHEMA}.SPCS_IMAGE_REPO"
SERVICE_NAME = "TAXI_PREDICTION_SERVICE"

def get_connection_parameters() -> Dict[str, Any]:
    """Retrieve Snowflake connection parameters from environment or default template."""
    return {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT", "your_organization-your_account"),
        "user": os.environ.get("SNOWFLAKE_USER", "your_username@example.com"),
        "authenticator": os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", WAREHOUSE),
        "database": os.environ.get("SNOWFLAKE_DATABASE", DATABASE),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", FEATURE_SCHEMA),
        "role": os.environ.get("SNOWFLAKE_ROLE", "ML_ROLE")
    }

def get_snowflake_session(connection_params: Dict[str, Any] = None) -> Session:
    """Initialize and return active Snowpark Session."""
    params = connection_params or get_connection_parameters()
    print(f"Connecting to Snowflake account: {params['account']} using role: {params['role']}...")
    session = Session.builder.configs(params).create()
    print(f"Successfully connected to Snowflake! Active Database: {session.get_current_database()}")
    return session

