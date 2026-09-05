from snowflake.snowpark import Session
from snowflake.ml.registry import Registry
import config

def deploy_to_spcs(session: Session, service_name: str = None, compute_pool: str = None):
    """
    Deploy the registered Snowflake model to Snowpark Container Services (SPCS).
    Requires a valid Compute Pool and Image Repository defined in Snowflake.
    """
    print("\n=== STAGE 3: SNOWPARK CONTAINER SERVICES (SPCS) DEPLOYMENT ===")
    
    srv_name = service_name or config.SERVICE_NAME
    c_pool = compute_pool or config.SPCS_COMPUTE_POOL

    registry = Registry(
        session=session,
        database_name=config.DATABASE,
        schema_name=config.MODEL_SCHEMA
    )

    print(f"Retrieving model '{config.MODEL_NAME}' version '{config.MODEL_VERSION}' from registry...")
    mv = registry.get_model(config.MODEL_NAME).version(config.MODEL_VERSION)

    print(f"Deploying model to SPCS Compute Pool '{c_pool}' as Service '{srv_name}'...")
    try:
        deployment_info = mv.deploy(
            service_name=srv_name,
            compute_pool=c_pool,
            image_repo=config.SPCS_IMAGE_REPO,
            mode="or_replace",
            options={"target_method": "predict"}
        )
        print(f"Model successfully deployed to SPCS!")
        print(f"Deployment info: {deployment_info}")
        return deployment_info
    except Exception as e:
        print(f"\n[SPCS Deployment Notice] Could not deploy to SPCS automatically: {e}")
        print("Note: Ensure SPCS is enabled on your Snowflake account and COMPUTE POOL privileges are granted to ML_ROLE.")
        print("Fallback: The model remains fully available for in-database Snowpark UDF / batch inference.")
        return None

if __name__ == "__main__":
    session = config.get_snowflake_session()
    deploy_to_spcs(session)

