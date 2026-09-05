# AWS MLOps Development

These notebooks are educational examples. They assume AWS credentials are
available through the normal boto3 credential chain and that you have an S3
bucket and SageMaker execution permissions.

## Notebook sequence

1. [`01_feature_store_dev.ipynb`](01_feature_store_dev.ipynb): create a feature group, ingest records, and inspect offline/online reads.
2. [`02_train_registry_lineage_dev.ipynb`](02_train_registry_lineage_dev.ipynb): submit a SageMaker training job, register its artifact, and inspect lineage.
3. [`03_batch_inference_outputs_dev.ipynb`](03_batch_inference_outputs_dev.ipynb): run batch inference and write final predictions and metrics to S3.

## Before running

Update the bucket, region, role ARN, image URI, and sample data values in the
first cells. Feature groups are not instantly available after creation, so
the notebooks include a wait step. Use a disposable dev bucket/prefix while
learning; SageMaker jobs and endpoints can incur AWS charges.

The snippets favor boto3 because it makes the AWS API boundaries visible. The
same flow can later be moved into SageMaker Pipelines or infrastructure as
code.
