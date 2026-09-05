import pandas as pd
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, current_timestamp
from snowflake.ml.registry import Registry

import config

def run_batch_inference_and_lineage(session: Session, limit: int = 100):
    """
    Run batch inference on Snowflake data using the registered model,
    save the predictions to ML_DB.OUTPUTS.MODEL_PREDICTIONS, and write ML Lineage metadata.
    """
    print("\n=== STAGE 4: BATCH INFERENCE & ML LINEAGE TRACKING ===")

    # 1. Fetch Registered Model
    registry = Registry(
        session=session,
        database_name=config.DATABASE,
        schema_name=config.MODEL_SCHEMA
    )
    print(f"Loading registered model '{config.MODEL_NAME}' version '{config.MODEL_VERSION}'...")
    loaded_model = registry.get_model(config.MODEL_NAME).version(config.MODEL_VERSION)

    # 2. Fetch Input Data for Batch Inference
    print(f"Fetching input data from {config.ENGINEERED_FEATURES_TABLE} (Limit: {limit})...")
    input_snowpark_df = session.table(config.ENGINEERED_FEATURES_TABLE).limit(limit)
    input_pdf = input_snowpark_df.to_pandas()

    feature_cols = ['PULOCATIONID', 'DOLOCATIONID', 'TRIP_DISTANCE']
    X_batch = input_pdf[feature_cols]

    # 3. Perform Prediction
    print("Executing batch predictions via registered model...")
    predictions = loaded_model.run(X_batch, function_name="predict")

    # 4. Construct Output Dataframe with Model Lineage Metadata
    print("Building output dataset with ML lineage tracking columns...")
    results_pdf = pd.DataFrame({
        "VENDORID": input_pdf["VENDORID"],
        "PULOCATIONID": input_pdf["PULOCATIONID"],
        "DOLOCATIONID": input_pdf["DOLOCATIONID"],
        "TRIP_DISTANCE": input_pdf["TRIP_DISTANCE"],
        "ACTUAL_DURATION_MINUTES": input_pdf["DURATION"],
        "PREDICTED_DURATION_MINUTES": predictions,
        "MODEL_NAME": config.MODEL_NAME,
        "MODEL_VERSION": config.MODEL_VERSION,
        "FEATURE_VIEW": f"{config.FEATURE_VIEW_NAME}:{config.FEATURE_VIEW_VERSION}",
        "TRAINING_DATASET": f"{config.TRAINING_DATASET_NAME}:{config.TRAINING_DATASET_VERSION}",
        "INFERENCE_TIMESTAMP": datetime.now().isoformat()
    })

    print(f"Generated predictions for {len(results_pdf)} records. Sample predictions:")
    print(results_pdf[["PULOCATIONID", "DOLOCATIONID", "TRIP_DISTANCE", "PREDICTED_DURATION_MINUTES"]].head(5))

    # 5. Save Output Table in ML_DB.OUTPUTS
    session.use_database(config.DATABASE)
    session.use_schema(config.OUTPUT_SCHEMA)
    output_snowpark_df = session.create_dataframe(results_pdf)

    print(f"Saving predictions table to Snowflake table {config.PREDICTIONS_TABLE}...")
    output_snowpark_df.write.save_as_table(
        config.PREDICTIONS_TABLE,
        mode="overwrite"
    )

    print("\n--- ML Lineage Graph Summary ---")
    print(f"Raw Source Data     : {config.RAW_TABLE}")
    print(f"Engineered Features : {config.ENGINEERED_FEATURES_TABLE}")
    print(f"Feature Store View  : {config.DATABASE}.{config.FEATURE_SCHEMA}.{config.FEATURE_VIEW_NAME}")
    print(f"Training Dataset    : {config.DATABASE}.{config.FEATURE_SCHEMA}.{config.TRAINING_DATASET_NAME}")
    print(f"Registered Model    : {config.DATABASE}.{config.MODEL_SCHEMA}.{config.MODEL_NAME}:{config.MODEL_VERSION}")
    print(f"Output Table        : {config.PREDICTIONS_TABLE}")

    return results_pdf

if __name__ == "__main__":
    session = config.get_snowflake_session()
    run_batch_inference_and_lineage(session)

