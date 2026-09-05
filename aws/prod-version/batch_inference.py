"""Submit a SageMaker batch transform job; predictions remain in S3."""

import argparse
import time
import boto3

from config import settings


def run(model_name: str, input_s3_uri: str, output_s3_uri: str) -> str:
    sm = boto3.client("sagemaker", region_name=settings.region)
    job_name = f"batch-inference-{int(time.time())}"
    sm.create_transform_job(
        TransformJobName=job_name,
        ModelName=model_name,
        TransformInput={"DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": input_s3_uri}}},
        TransformOutput={"S3OutputPath": output_s3_uri, "AssembleWith": "Line"},
        TransformResources={"InstanceType": "ml.m5.large", "InstanceCount": 1},
    )
    return f"{output_s3_uri.rstrip('/')}/{job_name}/"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--input-s3-uri", required=True)
    parser.add_argument("--output-s3-uri", default=f"{settings.s3_prefix}/inference/production/")
    args = parser.parse_args()
    print(run(args.model_name, args.input_s3_uri, args.output_s3_uri))
