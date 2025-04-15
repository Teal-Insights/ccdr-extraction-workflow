import asyncio
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from openai import AsyncOpenAI

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

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
            logger.warning(f"Assistant {assistant_id} does not have an associated file search vector store. Creating one.")
            try:
                vector_store = await client.vector_stores.create(name="imf-ccdrs-vector-store")
                vector_store_id = vector_store.id
                logger.info(f"Created new vector store with ID: {vector_store_id}")

                # Update the assistant to use the new vector store
                await client.beta.assistants.update(
                    assistant_id=assistant_id,
                    tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}},
                )
                logger.info(f"Updated assistant {assistant_id} to use the new vector store.")
                return vector_store_id
            except Exception as e_create:
                logger.error(f"Error creating or updating vector store for assistant {assistant_id}: {e_create}")
                return None

    except Exception as e_retrieve:
        logger.error(f"Error retrieving assistant {assistant_id}: {e_retrieve}")
        return None

async def main():
    """Main function to find PDFs and upload them to the OpenAI vector store."""
    assistant_id = os.getenv("ASSISTANT_ID")
    openai_api_key = os.getenv("OPENAI_API_KEY")

    if not assistant_id:
        logger.error("ASSISTANT_ID environment variable not set.")
        return
    if not openai_api_key:
        # The OpenAI client automatically checks for OPENAI_API_KEY,
        # but explicit check can be helpful for clearer error messages.
        logger.error("OPENAI_API_KEY environment variable not set.")
        return

    client = AsyncOpenAI(api_key=openai_api_key)

    vector_store_id = await get_vector_store(assistant_id, client)

    if not vector_store_id:
        logger.error("Could not retrieve or find vector store ID. Exiting.")
        return

    logger.info(f"Found vector store ID: {vector_store_id} for assistant {assistant_id}")

    # Find all PDF files in subdirectories of extract/data
    data_dir = Path("extract/data")
    pdf_files = list(data_dir.rglob("*.pdf"))

    if not pdf_files:
        logger.info("No PDF files found in extract/data subdirectories. Nothing to upload.")
        return

    logger.info(f"Found {len(pdf_files)} PDF files to upload.")

    # Prepare file paths for upload
    # upload_and_poll expects an iterable of FileTypes (e.g., Path objects or file-like objects)
    files_to_upload = [p for p in pdf_files] # Pass Path objects directly


    try:
        logger.info("Starting file batch upload and polling...")
        # Use upload_and_poll to upload files concurrently and wait for completion
        file_batch = await client.vector_stores.file_batches.upload_and_poll(
            vector_store_id=vector_store_id,
            files=files_to_upload,
            # max_concurrency can be adjusted if needed
        )

        logger.info("File batch processing completed.")
        logger.info(f"Batch ID: {file_batch.id}")
        logger.info(f"Status: {file_batch.status}")
        logger.info(f"File Counts: Total={file_batch.file_counts.total}, Completed={file_batch.file_counts.completed}, Failed={file_batch.file_counts.failed}, Cancelled={file_batch.file_counts.cancelled}, InProgress={file_batch.file_counts.in_progress}")

        if file_batch.file_counts.failed > 0:
            logger.warning("Some files failed to process. Check the OpenAI dashboard for details.")
            # List the files in the batch to see individual statuses
            files_in_batch = await client.vector_stores.file_batches.list_files(
                vector_store_id=vector_store_id,
                batch_id=file_batch.id
            )
            for file_detail in files_in_batch.data:
                logger.info(f"  File ID: {file_detail.id}, Status: {file_detail.status}")


    except Exception as e:
        logger.error(f"Error during file batch upload and polling: {e}")


if __name__ == "__main__":
    asyncio.run(main())
