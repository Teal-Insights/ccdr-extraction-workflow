# Ultimately, we will use our custom pgvector store, but for now we use OpenAI's vector store infrastructure

import asyncio
import os
import logging
import re
from pathlib import Path
from typing import List, Set, Optional
from dotenv import load_dotenv
from openai import AsyncOpenAI
from sqlmodel import Session, select
import requests
from tqdm import tqdm

from load.db import engine
from load.schema import Document
from load.upload_pdfs_to_aws_s3 import get_s3_client
from extract.convert_bin_files import analyze_and_prepare_file

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv(override=True)


# Helper function to get or create a vector store
async def get_vector_store(assistant_id: str, client: AsyncOpenAI) -> str | None:
    """Retrieves the vector store ID associated with an assistant. Creates one if it doesn't exist."""
    try:
        assistant = await client.beta.assistants.retrieve(assistant_id)
        if (
            assistant.tool_resources
            and assistant.tool_resources.file_search
            and assistant.tool_resources.file_search.vector_store_ids
        ):
            vector_store_id = assistant.tool_resources.file_search.vector_store_ids[0]
            logger.info(f"Found existing vector store ID: {vector_store_id}")
            return vector_store_id
        else:
            logger.warning(
                f"Assistant {assistant_id} does not have an associated file search vector store. Creating one."
            )
            try:
                vector_store = await client.vector_stores.create(
                    name="imf-ccdrs-vector-store"
                )
                vector_store_id = vector_store.id
                logger.info(f"Created new vector store with ID: {vector_store_id}")

                # Update the assistant to use the new vector store
                await client.beta.assistants.update(
                    assistant_id=assistant_id,
                    tool_resources={
                        "file_search": {"vector_store_ids": [vector_store_id]}
                    },
                )
                logger.info(
                    f"Updated assistant {assistant_id} to use the new vector store."
                )
                return vector_store_id
            except Exception as e_create:
                logger.error(
                    f"Error creating or updating vector store for assistant {assistant_id}: {e_create}"
                )
                return None

    except Exception as e_retrieve:
        logger.error(f"Error retrieving assistant {assistant_id}: {e_retrieve}")
        return None


async def get_existing_files_by_doc_id(vector_store_id: str, client: AsyncOpenAI) -> Set[int]:
    """Get a set of existing document IDs in the vector store by parsing filenames.
    Only considers files with 'completed' or 'in_progress' status - failed files will be retried.
    """
    existing_doc_ids = set()
    try:
        # Extract document IDs from filenames using the pattern doc_{id}.pdf
        doc_id_pattern = re.compile(r"doc_(\d+)\.pdf$")
        
        # Paginate through all files in the vector store
        after = None
        total_files_checked = 0
        
        while True:
            # List files with pagination
            if after is None:
                files_response = await client.vector_stores.files.list(
                    vector_store_id=vector_store_id,
                    limit=100  # Maximum allowed per page
                )
            else:
                files_response = await client.vector_stores.files.list(
                    vector_store_id=vector_store_id,
                    limit=100,  # Maximum allowed per page
                    after=after
                )
            
            if not files_response.data:
                break
            
            total_files_checked += len(files_response.data)
            logger.debug(f"Processing batch of {len(files_response.data)} files (total checked: {total_files_checked})")
            
            for file_obj in files_response.data:
                # Only consider files that are successfully processed or in progress
                if file_obj.status not in ['completed', 'in_progress']:
                    logger.debug(f"Skipping file {file_obj.id} with status: {file_obj.status}")
                    continue
                    
                # Get the file details to access the filename
                try:
                    file_details = await client.files.retrieve(file_obj.id)
                    if file_details.filename:
                        match = doc_id_pattern.search(file_details.filename)
                        if match:
                            doc_id = int(match.group(1))
                            existing_doc_ids.add(doc_id)
                            logger.debug(f"Found existing file for Document ID: {doc_id} (status: {file_obj.status})")
                        else:
                            logger.debug(f"Filename doesn't match expected pattern: {file_details.filename}")
                except Exception as e:
                    logger.warning(
                        f"Could not retrieve details for file {file_obj.id}: {e}"
                    )
                    continue
            
            # Check if there are more pages
            if not files_response.has_more:
                break
            
            # Set the cursor for the next page
            after = files_response.data[-1].id

        logger.info(f"Found {len(existing_doc_ids)} successfully processed document files in vector store (checked {total_files_checked} total files)")
        return existing_doc_ids

    except Exception as e:
        logger.error(f"Error retrieving existing files from vector store: {e}")
        return set()


def get_all_document_ids() -> List[int]:
    """Fetch all Document IDs from the database."""
    with Session(engine) as session:
        statement = select(Document.id).where(Document.id != None)
        doc_ids = session.exec(statement).all()
        logger.info(f"Found {len(doc_ids)} documents in database")
        return [doc_id for doc_id in doc_ids if doc_id is not None]


def get_document_by_id(doc_id: int) -> Optional[Document]:
    """Fetch a Document by ID from the database."""
    with Session(engine) as session:
        statement = select(Document).where(Document.id == doc_id)
        doc = session.exec(statement).first()
        return doc


def ensure_local_file(doc: Document, base_data_dir: str = "extract/data") -> Optional[str]:
    """
    Ensure a local file exists for a document. Downloads from S3 or World Bank if needed.
    
    Returns:
        Path to the local file if successful, None if failed.
    """
    # First, check if file already exists locally
    pub_dir = Path(base_data_dir) / f"pub_{doc.publication_id}"
    
    # Look for existing files with this document ID
    if pub_dir.exists():
        for file_path in pub_dir.iterdir():
            if file_path.name.startswith(f"doc_{doc.id}") and file_path.suffix in [".pdf", ".bin"]:
                logger.info(f"  -> Found existing local file: {file_path}")
                return str(file_path)
    
    # Create directory if it doesn't exist
    pub_dir.mkdir(parents=True, exist_ok=True)
    
    # Try to download from S3 first if storage_url is available
    if doc.storage_url:
        try:
            logger.info(f"  -> Downloading from S3: {doc.storage_url}")
            local_path = download_from_s3(doc, str(pub_dir))
            if local_path:
                return local_path
        except Exception as e:
            logger.warning(f"  -> Failed to download from S3: {e}")
    
    # Fall back to downloading from World Bank
    try:
        logger.info(f"  -> Downloading from World Bank: {doc.download_url}")
        local_path = download_from_world_bank(doc, str(pub_dir))
        return local_path
    except Exception as e:
        logger.error(f"  -> Failed to download from World Bank: {e}")
        return None


def download_from_s3(doc: Document, local_dir: str) -> Optional[str]:
    """Download a file from S3 using the storage_url."""
    if not doc.storage_url:
        return None
    
    # Parse S3 URL to extract bucket and key
    # Format: https://bucket.s3.region.amazonaws.com/key
    s3_url_pattern = re.compile(r"https://([^.]+)\.s3\.([^.]+)\.amazonaws\.com/(.+)")
    match = s3_url_pattern.match(doc.storage_url)
    
    if not match:
        logger.error(f"Invalid S3 URL format: {doc.storage_url}")
        return None
    
    bucket_name = match.group(1)
    region = match.group(2)
    s3_key = match.group(3)
    
    try:
        # Get S3 client
        s3_client = get_s3_client()
        
        # Determine local filename from S3 key
        local_filename = Path(s3_key).name
        if not local_filename.startswith(f"doc_{doc.id}"):
            # Fallback to creating filename from doc ID
            extension = Path(s3_key).suffix or ".pdf"
            local_filename = f"doc_{doc.id}{extension}"
        
        local_path = Path(local_dir) / local_filename
        
        # Download from S3
        s3_client.download_file(bucket_name, s3_key, str(local_path))
        logger.info(f"  -> Downloaded from S3 to: {local_path}")
        return str(local_path)
        
    except Exception as e:
        logger.error(f"  -> Error downloading from S3: {e}")
        return None


def download_from_world_bank(doc: Document, local_dir: str) -> Optional[str]:
    """Download a file from World Bank using the download_url."""
    session = requests.Session()
    max_retries = 3
    
    for attempt in range(1, max_retries + 1):
        try:
            # Make the request with streaming enabled
            response = session.get(doc.download_url, allow_redirects=True, stream=True)
            response.raise_for_status()
            
            # Get extension from headers or default to .pdf
            content_type = response.headers.get("content-type", "").lower()
            if "pdf" in content_type:
                ext = ".pdf"
            else:
                ext = ".bin"  # We'll convert this later if needed
            
            filename = f"doc_{doc.id}{ext}"
            local_path = Path(local_dir) / filename
            
            # Get total file size for progress bar
            total_size = int(response.headers.get("content-length", 0))
            
            # Download with progress bar
            with (
                open(local_path, "wb") as f,
                tqdm(
                    desc=f"doc_{doc.id}",
                    total=total_size,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar,
            ):
                for data in response.iter_content(chunk_size=8192):
                    size = f.write(data)
                    pbar.update(size)
            
            logger.info(f"  -> Downloaded from World Bank to: {local_path}")
            return str(local_path)
            
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"  -> Failed to download after {max_retries} attempts: {str(e)}")
                return None
            
            logger.warning(f"  -> Download attempt {attempt} failed, retrying: {str(e)}")
    
    return None


async def main():
    """Main function to synchronize documents between database and OpenAI vector store."""
    assistant_id = os.getenv("ASSISTANT_ID")
    openai_api_key = os.getenv("OPENAI_API_KEY")

    if not assistant_id:
        logger.error("ASSISTANT_ID environment variable not set.")
        return
    if not openai_api_key:
        logger.error("OPENAI_API_KEY environment variable not set.")
        return

    client = AsyncOpenAI(api_key=openai_api_key)

    vector_store_id = await get_vector_store(assistant_id, client)
    if not vector_store_id:
        logger.error("Could not retrieve or find vector store ID. Exiting.")
        return

    logger.info(f"Using vector store ID: {vector_store_id} for assistant {assistant_id}")

    # Step 1: Get all Document IDs from database
    all_doc_ids = get_all_document_ids()
    if not all_doc_ids:
        logger.info("No documents found in database. Nothing to upload.")
        return

    # Step 2: Get existing document IDs from OpenAI vector store
    existing_doc_ids = await get_existing_files_by_doc_id(vector_store_id, client)

    # Step 3: Find missing document IDs
    missing_doc_ids = set(all_doc_ids) - existing_doc_ids
    
    logger.info(f"Total documents in database: {len(all_doc_ids)}")
    logger.info(f"Documents already in OpenAI: {len(existing_doc_ids)}")
    logger.info(f"Documents missing from OpenAI: {len(missing_doc_ids)}")

    if not missing_doc_ids:
        logger.info("All documents are already uploaded to OpenAI vector store.")
        return

    # Step 4: Process missing documents
    files_to_upload = []
    temp_files_to_cleanup = []

    for doc_id in sorted(missing_doc_ids):
        logger.info(f"\nProcessing missing Document ID: {doc_id}")
        
        # Get document details from database
        doc = get_document_by_id(doc_id)
        if not doc:
            logger.warning(f"  -> Document ID {doc_id} not found in database")
            continue
        
        # Step 5: Ensure local file exists (download if needed)
        local_path = ensure_local_file(doc)
        if not local_path:
            logger.error(f"  -> Failed to obtain local file for Document ID {doc_id}")
            continue
        
        # Convert file if necessary (e.g., .bin to .pdf)
        try:
            final_path, file_size = analyze_and_prepare_file(local_path)
            
            # If the file was converted, mark the original for cleanup
            if final_path != local_path and Path(local_path).exists():
                temp_files_to_cleanup.append(local_path)
            
            # Ensure the final file has the correct name pattern for OpenAI
            final_path_obj = Path(final_path)
            expected_filename = f"doc_{doc_id}.pdf"
            
            if final_path_obj.name != expected_filename:
                # Rename to expected pattern
                new_path = final_path_obj.parent / expected_filename
                final_path_obj.rename(new_path)
                final_path = str(new_path)
                logger.info(f"  -> Renamed file to: {expected_filename}")
            
            files_to_upload.append(Path(final_path))
            logger.info(f"  -> Prepared file for upload: {final_path}")
            
        except Exception as e:
            logger.error(f"  -> Failed to prepare file for Document ID {doc_id}: {e}")
            continue

    if not files_to_upload:
        logger.info("No files successfully prepared for upload.")
        return

    # Step 6: Upload to OpenAI vector store
    try:
        logger.info(f"\nUploading {len(files_to_upload)} files to OpenAI vector store...")
        
        # Use upload_and_poll to upload files concurrently and wait for completion
        file_batch = await client.vector_stores.file_batches.upload_and_poll(
            vector_store_id=vector_store_id,
            files=files_to_upload,
        )

        logger.info("File batch processing completed.")
        logger.info(f"Batch ID: {file_batch.id}")
        logger.info(f"Status: {file_batch.status}")
        logger.info(
            f"File Counts: Total={file_batch.file_counts.total}, "
            f"Completed={file_batch.file_counts.completed}, "
            f"Failed={file_batch.file_counts.failed}, "
            f"Cancelled={file_batch.file_counts.cancelled}, "
            f"InProgress={file_batch.file_counts.in_progress}"
        )

        if file_batch.file_counts.failed > 0:
            logger.warning("Some files failed to process. Check the OpenAI dashboard for details.")
            # List the files in the batch to see individual statuses
            files_in_batch = await client.vector_stores.file_batches.list_files(
                vector_store_id=vector_store_id, batch_id=file_batch.id
            )
            for file_detail in files_in_batch.data:
                logger.info(f"  File ID: {file_detail.id}, Status: {file_detail.status}")

    except Exception as e:
        logger.error(f"Error during file batch upload and polling: {e}")
    
    finally:
        # Clean up temporary files
        for temp_file in temp_files_to_cleanup:
            try:
                if Path(temp_file).exists():
                    os.remove(temp_file)
                    logger.info(f"  -> Cleaned up temp file: {temp_file}")
            except Exception as e:
                logger.warning(f"  -> Failed to clean up temp file {temp_file}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
