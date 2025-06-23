#!/bin/bash

# Verify required environment variables
if [ -z "$S3_BUCKET_NAME" ] || [ -z "$AWS_REGION" ]; then
    echo "ERROR: S3_BUCKET_NAME and AWS_REGION must be set"
    exit 1
fi

# Check AWS authentication
if ! aws sts get-caller-identity >/dev/null 2>&1; then
    echo "ERROR: AWS CLI not authenticated. Run 'aws sso login' first."
    exit 1
fi

# Create bucket if it doesn't exist
if ! aws s3api head-bucket --bucket "$S3_BUCKET_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    aws s3 mb "s3://$S3_BUCKET_NAME" --region "$AWS_REGION"
    echo "Created bucket $S3_BUCKET_NAME"
else
    echo "Bucket $S3_BUCKET_NAME already exists"
fi

# Create IAM user
USER_NAME="${S3_BUCKET_NAME}-user"
if ! aws iam get-user --user-name "$USER_NAME" >/dev/null 2>&1; then
    aws iam create-user --user-name "$USER_NAME"
    echo "Created IAM user: $USER_NAME"
fi

# Create user group
GROUP_NAME="${S3_BUCKET_NAME}-group"
if ! aws iam get-group --group-name "$GROUP_NAME" >/dev/null 2>&1; then
    aws iam create-group --group-name "$GROUP_NAME"
    echo "Created IAM group: $GROUP_NAME"
fi

# Add user to group
aws iam add-user-to-group --user-name "$USER_NAME" --group-name "$GROUP_NAME"

# Create and attach S3 read policy
POLICY_ARN=$(aws iam create-policy --policy-name "${S3_BUCKET_NAME}-read" \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetObject"],
            "Resource": [
                "arn:aws:s3:::'"$S3_BUCKET_NAME"'",
                "arn:aws:s3:::'"$S3_BUCKET_NAME"'/*"
            ]
        }]
    }' --query 'Policy.Arn' --output text)

aws iam attach-group-policy --group-name "$GROUP_NAME" --policy-arn "$POLICY_ARN"

# Create and display access keys
CREDENTIALS=$(aws iam create-access-key --user-name "$USER_NAME")
AWS_ACCESS_KEY_ID=$(echo "$CREDENTIALS" | jq -r '.AccessKey.AccessKeyId')
AWS_SECRET_ACCESS_KEY=$(echo "$CREDENTIALS" | jq -r '.AccessKey.SecretAccessKey')

echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"

# Check if .env file exists
if [ -f ".env" ]; then
    # Check for non-empty AWS_ACCESS_KEY_ID
    if grep -q "AWS_ACCESS_KEY_ID=" .env; then
        CURRENT_ACCESS_KEY_ID=$(grep "AWS_ACCESS_KEY_ID=" .env | cut -d '=' -f2)
        if [ -n "$CURRENT_ACCESS_KEY_ID" ]; then
            echo "ERROR: .env file already contains a non-empty AWS_ACCESS_KEY_ID; value will not be overwritten."
            echo "Please manually add the following to your .env file:"
            echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
        else
            sed -i "s|AWS_ACCESS_KEY_ID=.*|AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID|" .env
        fi
    else
        echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> .env
    fi

    # Check for non-empty AWS_SECRET_ACCESS_KEY
    if grep -q "AWS_SECRET_ACCESS_KEY=" .env; then
        CURRENT_SECRET_ACCESS_KEY=$(grep "AWS_SECRET_ACCESS_KEY=" .env | cut -d '=' -f2)
        if [ -n "$CURRENT_SECRET_ACCESS_KEY" ]; then
            echo "ERROR: .env file already contains a non-empty AWS_SECRET_ACCESS_KEY; value will not be overwritten."
            echo "Please manually add the following to your .env file:"
            echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"
        else
            sed -i "s|AWS_SECRET_ACCESS_KEY=.*|AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY|" .env
        fi
    else
        echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> .env
    fi
else
    # If .env does not exist, create it and add the keys
    echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> .env
    echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> .env
fi

# ~670 GB of data, which, at AWS storage and usage rates, likely amounts to ~$20 per month
aws s3 sync extract/data "s3://$S3_BUCKET_NAME"