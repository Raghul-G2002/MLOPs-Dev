"""Configuration shared by the production-oriented AWS examples."""

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    region: str = os.getenv("AWS_REGION", "us-east-1")
    bucket: str = os.getenv("MLOPS_BUCKET", "replace-with-prod-bucket")
    feature_group_name: str = os.getenv("FEATURE_GROUP_NAME", "taxi-features-prod")
    model_package_group: str = os.getenv("MODEL_PACKAGE_GROUP", "taxi-models-prod")
    inference_image: str = os.getenv("INFERENCE_IMAGE", "replace-with-ecr-image")

    @property
    def s3_prefix(self) -> str:
        return f"s3://{self.bucket}"


settings = Settings()
