# CCDR Extraction Workflow

This directory contains the complete workflow for extracting World Bank Country and Climate Development Reports (CCDRs). It is part of the data preparation pipeline for the [CCDR Explorer](https://github.com/Teal-Insights/ccdr-explorer-client), created by Teal Insights for Nature Finance.

## Quick Start

To run the complete extraction workflow:

```bash
uv run extract_ccdrs.py
```

## Individual Steps

The workflow consists of 9 steps that can also be run individually:

1. **Extract Publication Links** (`extract_publication_links.py`)
   - Scrapes publication links from the World Bank repository
   - Output: `data/publication_links.json`

2. **Extract Publication Details** (`extract_publication_details.py`)
   - Extracts detailed information from each publication page
   - Output: `data/publication_details.json`

3. **Add IDs** (`add_ids.py`)
   - Adds unique IDs to publications and download links
   - Modifies: `data/publication_details.json`

4. **Classify File Types** (`classify_file_types.py`)
   - Classifies file types for each download link
   - Modifies: `data/publication_details.json`

5. **Filter Download Links** (`filter_download_links.py`)
   - Filters and classifies which links to download
   - Modifies: `data/publication_details.json`

6. **Download Files** (`download_files.py`)
   - Downloads the selected PDF files
   - Output: `data/pub_*/dl_*.pdf`

7. **Convert BIN Files** (`convert_bin_files.py`)
   - Converts .bin files to .pdf if they are PDF documents
   - Modifies: Files in `data/` directory

8. **Upload to Database** (`upload_pubs_to_db.py`)
   - Uploads publications and documents to the PostgreSQL database
   - Uses: `data/publication_details.json`

9. **Upload PDFs to OpenAI** (`upload_pdfs_to_openai.py`)
   - Uploads PDF files to OpenAI vector store for AI-powered search
   - Uses: PDF files from `data/pub_*/` directories
   - Includes deduplication to avoid uploading existing files

## Running Individual Steps

```bash
# Run a specific step
uv run -m extract.extract_publication_links
uv run -m extract.extract_publication_details
uv run -m extract.add_ids
uv run -m extract.classify_file_types
uv run -m extract.filter_download_links
uv run -m extract.download_files
uv run -m extract.convert_bin_files
uv run -m extract.upload_pubs_to_db
uv run -m extract.upload_pdfs_to_openai
```

## Configuration

For the OpenAI upload step, you'll need to set up environment variables in `extract/.env`:

```bash
OPENAI_API_KEY=your_openai_api_key_here
ASSISTANT_ID=your_openai_assistant_id_here
```

## Output

After running the complete workflow, you'll have:
- JSON files with publication metadata in `data/`
- Downloaded PDF files organized in `data/pub_*/` directories
- Publications and documents uploaded to the PostgreSQL database
- PDF files uploaded to OpenAI vector store for AI-powered search
- Currently processes 61 CCDRs comprising 126+ PDF files 