# AWS MLOps Production Building Blocks

This folder is a starting point for a repeatable, containerized workflow. It
is not a deployable production system yet. The scripts keep environment values
in configuration and make the major boundaries explicit:

`data -> features -> training artifact -> model registry -> inference output`

## Scripts

- [`config.py`](config.py): environment-specific names, prefixes, and safety defaults.
- [`feature_store.py`](feature_store.py): create a feature group and ingest records.
- [`register_model.py`](register_model.py): register an immutable model artifact and evaluation metrics.
- [`batch_inference.py`](batch_inference.py): submit a batch transform job and record the output location.

## Future container path

1. Build a pinned inference/training image.
2. Push the image to ECR with a commit or release tag, never only `latest`.
3. Register the image plus model artifact in SageMaker Model Registry.
4. Add approval gates and deploy the approved package.
5. Choose the serving target: SageMaker endpoint for managed ML serving, ECS
   for a simpler container service, or EKS when Kubernetes integration is a
   real requirement.

Production hardening still needed: IAM least privilege, KMS encryption,
private networking, retries/idempotency, schema validation, data-quality
checks, monitoring, alerting, CI/CD, and infrastructure as code.
