# AWS S3 Bucket Setup Instructions

## Prerequisites: AWS Account Setup

1. Create an AWS Account:
   - Go to [AWS Console](https://aws.amazon.com/)
   - Click "Create an AWS Account"
   - Follow the signup process (requires email, credit card)
   - Choose the "Basic Support - Free" tier

2. Set up AWS IAM Identity Center (formerly AWS SSO):
   - In AWS Console, search for "IAM Identity Center"
   - Click "Enable" if not already enabled
   - Choose "Create organization" if prompted
   - Set up your identity source (can use built-in AWS directory to start)
   - Create a permission set (e.g., "AdministratorAccess")
   - Add users and assign the permission set
   - Note the SSO start URL (will look like `https://d-xxxxxxxxxx.awsapps.com/start`)

3. Access the AWS Access Portal:
   - Go to your SSO start URL
   - Create your user credentials
   - You can now use these credentials for AWS CLI access

## Installation

1. Follow the official AWS CLI installation instructions for Linux x86 (64-bit) at:
   https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

2. Verify installation:
   ```bash
   aws --version
   ```
   Should show something like: `aws-cli/2.25.8 Python/3.12.9 Linux/6.8.0-56-generic`

## Configuration with AWS IAM Identity Center (SSO)

1. Configure SSO:
   ```bash
   aws configure sso
   ```

2. When prompted, enter:
   - SSO start URL: `https://d-xxxxxxxxxx.awsapps.com/start`
   - SSO Region: `us-east-1`
   - Default CLI profile name: (press Enter for default, or choose a name)

3 The command will open your browser for authentication. Log in through your organization's portal.

4. After successful authentication, note the profile name from the configuration output (e.g., `AdministratorAccess-xxxxxxxxxxxx`)

## Setting Up Default Profile

To avoid having to specify `--profile` with every command:

1. Add the AWS_PROFILE environment variable to your `.bashrc`:
   ```bash
   echo 'export AWS_PROFILE=AdministratorAccess-xxxxxxxxxxxx' >> ~/.bashrc
   ```

2. Reload your `.bashrc`:
   ```bash
   source ~/.bashrc
   ```

## Verify Setup

Test that everything is working:
```bash
aws s3 ls
```

This should list all S3 buckets you have access to without any authentication errors.

## Create Bucket and Upload files

### Create a Bucket

To create a bucket, run:

```bash
aws s3 mb s3://nature-finance-rag-api
```

### Upload PDFs

To upload all PDFs from the `extract/data` directory, preserving the folder structure of that directory:

```bash
aws s3 sync extract/data s3://nature-finance-rag-api
```

This amounts to about 670 GB of data, which, at AWS storage and usage rates, likely amounts to ~$20 per month.

Strictly speaking, there may not actually be any good reason to store the PDFs. Instead, we can link users directly to where the PDFs are stored on the IMF website.

### Upload Images



## Common AWS S3 Commands

```bash
# List buckets
aws s3 ls

# List contents of a specific bucket
aws s3 ls s3://bucket-name

# Copy file to S3
aws s3 cp local_file.pdf s3://bucket-name/

# Copy file from S3
aws s3 cp s3://bucket-name/file.pdf ./

# Sync directory to S3
aws s3 sync local_dir s3://bucket-name/remote_dir

# Remove file from S3
aws s3 rm s3://bucket-name/file.pdf

# Create new bucket
aws s3 mb s3://bucket-name

# Remove bucket and all contents
aws s3 rb s3://bucket-name --force
```

## Troubleshooting

If you get authentication errors:

1. Ensure your SSO session hasn't expired (run `aws configure sso` again if needed)
2. Verify your AWS_PROFILE is set correctly: `echo $AWS_PROFILE`
3. Check available profiles: `aws configure list-profiles` 