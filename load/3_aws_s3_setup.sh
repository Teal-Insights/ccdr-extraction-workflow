# TODO: Script bucket, IAM user, user group, and permissions setup for production app

# Run `source .env` to set the S3_BUCKET_NAME environment variable before running this script
# ~670 GB of data, which, at AWS storage and usage rates, likely amounts to ~$20 per month
aws s3 sync extract/data s3://$S3_BUCKET_NAME