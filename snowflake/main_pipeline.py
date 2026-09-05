"""
Main Orchestrator for Snowflake End-to-End MLOps Pipeline
Executes Feature Store setup -> Model Training & Registry -> SPCS Deployment -> Batch Inference & Lineage.
"""

import sys
import config
from feature_store_pipeline import setup_feature_store
from model_training_pipeline import train_and_register_model
from spcs_deployment import deploy_to_spcs
from inference_lineage_pipeline import run_batch_inference_and_lineage

def main():
    print("=" * 70)
    print("      SNOWFLAKE END-TO-END MLOPS PIPELINE ORCHESTRATOR      ")
    print("=" * 70)

    # Step 1: Connect to Snowflake
    session = config.get_snowflake_session()

    # Step 2: Feature Store & Dataset Generation
    fs, training_dataset = setup_feature_store(session)

    # Step 3: Model Training, Experiment Tracking & Registry
    registry, registered_model = train_and_register_model(session, training_dataset)

    # Step 4: SPCS Container Deployment (optional / condition based on setup)
    deploy_to_spcs(session)

    # Step 5: Batch Inference & Output Table Lineage
    results_pdf = run_batch_inference_and_lineage(session, limit=100)

    print("\n" + "=" * 70)
    print("      END-TO-END MLOPS PIPELINE EXECUTED SUCCESSFULLY!       ")
    print("=" * 70)

if __name__ == "__main__":
    main()

