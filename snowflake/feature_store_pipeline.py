import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.ml.feature_store import FeatureStore, FeatureView, Entity, CreationMode

import config

def perform_feature_engineering(session: Session) -> pd.DataFrame:
    """Read raw taxi data, compute DURATION target variable, and filter invalid records."""
    print("Reading raw taxi data from Snowflake table...")
    raw_df = session.table(config.RAW_TABLE)
    pdf = raw_df.to_pandas()

    print("Executing feature engineering (calculating DURATION target variable in minutes)...")
    pdf['DURATION'] = (pdf['LPEP_DROPOFF_DATETIME'] - pdf['LPEP_PICKUP_DATETIME']).dt.total_seconds() / 60.0
    filtered_pdf = pdf[(pdf['DURATION'] >= 1) & (pdf['DURATION'] <= 60)].copy()

    engineered_df = filtered_pdf[["VENDORID", "PULOCATIONID", "DOLOCATIONID", "TRIP_DISTANCE", "DURATION"]]
    
    print(f"Saving engineered features ({len(engineered_df)} rows) back to Snowflake table {config.ENGINEERED_FEATURES_TABLE}...")
    session.use_database(config.DATABASE)
    session.use_schema(config.FEATURE_SCHEMA)
    
    snowpark_engineered_df = session.create_dataframe(engineered_df)
    snowpark_engineered_df.write.save_as_table(config.ENGINEERED_FEATURES_TABLE, mode="overwrite")
    return engineered_df

def setup_feature_store(session: Session):
    """
    Initialize Snowflake Feature Store, register VENDOR_ID_ENTITY and TAXI_TRIP_FEATURES FeatureView,
    and generate the TAXI_TRIP_TRAINING_DATASET with point-in-time correctness & lineage.
    """
    print("\n=== STAGE 1: FEATURE STORE SETUP ===")
    
    # 1. First engineer features and persist engineered feature table
    perform_feature_engineering(session)

    # 2. Initialize Feature Store
    fs = FeatureStore(
        session=session,
        database=config.DATABASE,
        name=config.FEATURE_SCHEMA,
        default_warehouse=config.WAREHOUSE,
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST
    )
    print(f"Feature Store initialized on schema {config.DATABASE}.{config.FEATURE_SCHEMA}")

    # 3. Get or Register Entity
    try:
        vendor_entity = fs.get_entity(config.ENTITY_NAME)
        print(f"Retrieved existing entity: '{config.ENTITY_NAME}'")
    except Exception:
        vendor_entity = Entity(
            name=config.ENTITY_NAME,
            join_keys=["VENDORID"],
            desc="Vendor ID entity representing taxi service providers"
        )
        fs.register_entity(vendor_entity)
        print(f"Registered new entity: '{config.ENTITY_NAME}'")

    # 4. Define and Register Feature View
    features_snowpark = session.table(config.ENGINEERED_FEATURES_TABLE)
    feature_view = FeatureView(
        name=config.FEATURE_VIEW_NAME,
        entities=[vendor_entity],
        feature_df=features_snowpark,
        desc="Feature view containing trip distance, pickup/dropoff locations, and trip duration."
    )
    feature_view = fs.register_feature_view(
        feature_view=feature_view,
        version=config.FEATURE_VIEW_VERSION,
        overwrite=True
    )
    print(f"Feature View '{config.FEATURE_VIEW_NAME}' version '{config.FEATURE_VIEW_VERSION}' registered successfully.")

    # 5. Generate Training Dataset
    try:
        training_dataset = fs.generate_dataset(
            name=config.TRAINING_DATASET_NAME,
            spine_df=features_snowpark.select(col("VENDORID")),
            features=[feature_view],
            version=config.TRAINING_DATASET_VERSION
        )
        print(f"Training Dataset '{config.TRAINING_DATASET_NAME}' generated successfully.")
    except Exception as e:
        print(f"Retrieving pre-existing Training Dataset '{config.TRAINING_DATASET_NAME}': {e}")
        training_dataset = fs.get_dataset(config.TRAINING_DATASET_NAME, version=config.TRAINING_DATASET_VERSION)

    return fs, training_dataset

if __name__ == "__main__":
    session = config.get_snowflake_session()
    setup_feature_store(session)

