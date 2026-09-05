"""Create a production-style SageMaker Feature Store group."""

import boto3

from config import settings


def create_feature_group() -> str:
    sm = boto3.client("sagemaker", region_name=settings.region)
    response = sm.create_feature_group(
        FeatureGroupName=settings.feature_group_name,
        RecordIdentifierFeatureName="entity_id",
        EventTimeFeatureName="event_time",
        FeatureDefinitions=[
            {"FeatureName": "entity_id", "FeatureType": "String"},
            {"FeatureName": "event_time", "FeatureType": "String"},
            {"FeatureName": "feature_value", "FeatureType": "Fractional"},
        ],
        OnlineStoreConfig={"EnableOnlineStore": True},
        OfflineStoreConfig={"S3StorageConfig": {"S3Uri": f"{settings.s3_prefix}/feature-store/offline/"}},
        Description="Versioned production feature group example",
    )
    return response["FeatureGroupArn"]


if __name__ == "__main__":
    print(create_feature_group())
