# CCDR Extraction Workflow

This directory contains the complete workflow for extracting World Bank Country and Climate Development Reports (CCDRs). It is part of the data preparation pipeline for the [CCDR Explorer](https://github.com/Teal-Insights/ccdr-explorer-client), created by Teal Insights for Nature Finance.

## Automated Pipeline

This workflow runs automatically **once a month** via GitHub Actions to discover and process newly published CCDRs. The automated pipeline:

- Runs on the 1st day of each month at 2:00 AM UTC
- Identifies new publications not yet in the database
- Downloads and processes only new content
- Uploads PDFs to AWS S3 and metadata to PostgreSQL
- Includes OpenAI vector store integration for AI-powered search
- Can also be triggered manually when needed

The pipeline is designed to be incremental - it only processes new CCDRs that haven't been seen before, making monthly runs efficient and cost-effective.

## Quick Start

To run the complete extraction workflow locally:

```bash
uv run extract_ccdrs.py
```

## Workflow Architecture

The extraction workflow uses a **two-stage architecture**:

### Stage 1: Metadata Ingestion & Persistence

- Scrapes publication links from the World Bank repository
- Extracts detailed metadata from each publication page
- Classifies download links and file types
- Persists publication and document metadata to PostgreSQL database

### Stage 2: File Processing & Record Enrichment

- Downloads PDF files for documents stored in the database
- Converts file formats when necessary (e.g., .bin to .pdf)
- Uploads files to AWS S3 storage
- Updates database records with S3 URLs and file metadata

The database serves as the handoff point between stages, enabling better error recovery and allowing stages to be run independently.

## Running Specific Stages

```bash
# Run only metadata ingestion
uv run extract_ccdrs.py --stage1

# Run only file processing
uv run extract_ccdrs.py --stage2

# Include OpenAI upload
uv run extract_ccdrs.py --openai

# Clean up local files after processing
uv run extract_ccdrs.py --cleanup
```



## Configuration

The workflow requires several environment variables. For local development, create an `.env` file in the project root:

```bash
# Database Configuration
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=5432
POSTGRES_DB=your_database_name

# AWS S3 Configuration
S3_BUCKET_NAME=your_s3_bucket_name
AWS_REGION=your_aws_region
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key

# OpenAI Configuration (optional)
OPENAI_API_KEY=your_openai_api_key_here
ASSISTANT_ID=your_openai_assistant_id_here
```

## Output

After running the complete workflow, you'll have:

- Publication and document metadata stored in PostgreSQL database
- PDF files uploaded to AWS S3 storage with organized naming
- PDF files uploaded to OpenAI vector store for AI-powered search (if configured)
- Local temporary files cleaned up automatically
- Currently processes 67+ CCDRs comprising 198+ PDF files, with new publications added monthly 