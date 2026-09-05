"""Register a model artifact after evaluation has produced metrics."""

import argparse
import boto3

from config import settings


def register(model_data_url: str, metrics_url: str, image_uri: str) -> str:
    sm = boto3.client("sagemaker", region_name=settings.region)
    response = sm.create_model_package(
        ModelPackageGroupName=settings.model_package_group,
        ModelApprovalStatus="PendingManualApproval",
        ModelPackageDescription="Production candidate; approve after review",
        ModelMetrics={"ModelQuality": {"Statistics": {"ContentType": "application/json", "S3Uri": metrics_url}}},
        InferenceSpecification={
            "Containers": [{"Image": image_uri, "ModelDataUrl": model_data_url}],
            "SupportedContentTypes": ["text/csv"],
            "SupportedResponseMIMETypes": ["text/csv"],
        },
    )
    return response["ModelPackageArn"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-data-url", required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--image-uri", default=settings.inference_image)
    args = parser.parse_args()
    print(register(args.model_data_url, args.metrics_url, args.image_uri))
