#!/usr/bin/env python3
"""
Main script to run the complete CCDR extraction workflow.

This script implements a two-stage architecture:
- Stage 1: Metadata Ingestion & Persistence
- Stage 2: File Processing & Record Enrichment

The database serves as the handoff point between stages.

Usage:
    uv run extract_ccdrs.py [--stage1] [--stage2] [--openai]

Arguments:
    --stage1    Run only Stage 1: Metadata Ingestion
    --stage2    Run only Stage 2: File Processing
    --openai    Run OpenAI upload after other stages
"""

import os
import time
import random
from typing import List, Optional
from pathlib import Path
from sqlmodel import Session, select
from dotenv import load_dotenv

from load.db import engine, check_schema_sync
from load.schema import Publication, Document
from extract.extract_publication_links import get_all_publication_links, PublicationLink
from extract.extract_publication_details import scrape_publication_details_with_retry, PublicationDetails
from extract.classify_mime_types import get_file_type_from_url, PublicationDetailsWithFileInfo
from extract.classify_document_types import classify_download_links, PublicationDetailsWithClassification
from load.upload_pubs_to_db import persist_publication
from extract.download_files import download_document_file
from extract.convert_bin_files import analyze_and_prepare_file
from load.upload_pdfs_to_aws_s3 import get_s3_client, upload_file_to_s3

load_dotenv(override=True)


def identify_new_publications(scraped_links: List[PublicationLink], session: Session) -> List[PublicationLink]:
    """Queries the DB and filters for publication links that are not yet present."""
    existing_uris = set(session.exec(select(Publication.uri)).all())
    new_links = [link for link in scraped_links if str(link.url) not in existing_uris]
    print(f"Found {len(scraped_links)} total publications, {len(new_links)} are new.")
    return new_links


def run_stage_1_metadata_ingestion() -> None:
    """
    Orchestrates the scraping and persistence of publication metadata.
    - Scrapes all publication links.
    - Identifies new publications not yet in the database.
    - For each new publication:
        - Scrapes detailed metadata.
        - Classifies download links using rules.
        - Persists the new publication and its documents to the DB.
    """
    print("--- Running Stage 1: Metadata Ingestion ---")

    # 1. Scrape All Publication Links
    base_url = "https://openknowledge.worldbank.org/collections/5cd4b6f6-94bb-5996-b00c-58be279093de"
    all_links: List[PublicationLink] = get_all_publication_links(base_url)

    with Session(engine) as session:
        # 2. Identify New Publications
        new_links_to_process: List[PublicationLink] = identify_new_publications(all_links, session)

        if not new_links_to_process:
            print("No new publications to process.")
            print("--- Stage 1 Complete ---")
            return

        # 3. Process Each New Publication
        for idx, link_info in enumerate(new_links_to_process):
            print(f"\nProcessing new publication: {link_info.title}")

            # Add a polite delay between requests (except for the first one)
            if idx > 0:
                delay = random.uniform(3.0, 7.0)  # 3-7 seconds between requests
                print(f"Waiting {delay:.1f}s before next request to be respectful...")
                time.sleep(delay)

            # a. Scrape Details
            pub_details: Optional[PublicationDetails] = scrape_publication_details_with_retry(link_info.url)
            if not pub_details or not pub_details.download_links:
                print(
                    f"  -> Failed to scrape details or no download links found. Skipping."
                )
                continue

            # b. Get MIME types (lightweight HEAD/GET request)
            pub_details_with_info: PublicationDetailsWithFileInfo = PublicationDetailsWithFileInfo(
                title=pub_details.title,
                source_url=pub_details.source_url,
                abstract=pub_details.abstract,
                citation=pub_details.citation,
                uri=pub_details.uri,
                metadata=pub_details.metadata,
                download_links=[get_file_type_from_url(link) for link in pub_details.download_links]
            )

            # c. Classify Links
            pub_details_with_classification: PublicationDetailsWithClassification = PublicationDetailsWithClassification(
                title=pub_details_with_info.title,
                source_url=pub_details_with_info.source_url,
                abstract=pub_details_with_info.abstract,
                citation=pub_details_with_info.citation,
                uri=pub_details_with_info.uri,
                metadata=pub_details_with_info.metadata,
                download_links=classify_download_links(pub_details_with_info.download_links, True)
            )

            # d. Validate that there's at least one download link to process
            if not pub_details_with_classification.download_links:
                print(f"  -> No downloadable documents found. Skipping publication.")
                continue

            # 4. Persist to Database in a transaction
            try:
                persist_publication(pub_details_with_classification, session)
                session.commit()
                print(
                    f"  -> Successfully saved to database with {len(pub_details_with_classification.download_links)} downloadable documents."
                )
            except Exception as e:
                print(
                    f"  -> ERROR: Failed to save to database. Rolling back. Error: {e}"
                )
                session.rollback()

    print("--- Stage 1 Complete ---")


def run_stage_2_file_processing() -> None:
    """
    Orchestrates the processing of binary files for documents.
    - Queries the database for unprocessed documents.
    - For each document:
        - Downloads the file.
        - Converts the file if necessary (e.g., .bin to .pdf).
        - Measures file size.
        - Uploads the final file to S3.
        - Updates the document's database record with the S3 URL and file size.
        - Cleans up the local file.
    """
    print("--- Running Stage 2: File Processing ---")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        print("ERROR: S3_BUCKET_NAME environment variable not set. Aborting.")
        return

    # Initialize S3 client once
    s3_client = get_s3_client()

    with Session(engine) as session:
        # 1. Query for Unprocessed Documents
        statement = select(Document).where(Document.storage_url == None)
        unprocessed_docs = session.exec(statement).all()

        if not unprocessed_docs:
            print("No new documents to process.")
            print("--- Stage 2 Complete ---")
            return

        print(f"Found {len(unprocessed_docs)} documents to process.")

        # 2. Process Each Document
        for doc in unprocessed_docs:
            local_path_final = None  # To ensure we can clean up even if a step fails
            local_path_initial = None
            print(
                f"\nProcessing Document ID: {doc.id} for Publication ID: {doc.publication_id}"
            )
            try:
                # a. Download File
                local_path_initial = download_document_file(doc)

                # b. Convert & Get File Size
                local_path_final, file_size = analyze_and_prepare_file(
                    local_path_initial
                )

                # c. Upload to S3
                s3_url = upload_file_to_s3(
                    local_path_final, doc, s3_client, bucket_name
                )

                # d. Update Database Record
                doc.storage_url = s3_url
                doc.file_size = file_size
                session.add(doc)
                session.commit()
                session.refresh(doc)  # Refresh to confirm the changes
                print(f"  -> Successfully updated DB for Document ID: {doc.id}")

            except Exception as e:
                print(f"  -> ERROR processing Document ID {doc.id}: {e}")
                print("  -> Rolling back transaction and skipping to next document.")
                session.rollback()
                continue  # Move to the next document

            finally:
                # e. Cleanup local files
                if local_path_final and Path(local_path_final).exists():
                    os.remove(local_path_final)
                    print(f"  -> Cleaned up local file: {local_path_final}")
                # Also clean up the initial .bin if it's different and still exists
                if (
                    local_path_initial
                    and local_path_initial != local_path_final
                    and Path(local_path_initial).exists()
                ):
                    os.remove(local_path_initial)
                    print(f"  -> Cleaned up temp file: {local_path_initial}")

    print("\n--- Stage 2 Complete ---")


def run_openai_upload() -> None:
    """
    Runs the OpenAI upload step as a separate post-processing step.
    Uploads all PDF files to the OpenAI vector store associated with the assistant.
    """
    print("--- Running OpenAI Upload ---")

    try:
        import asyncio
        from load.upload_pdfs_to_openai import main as openai_upload_main

        # Run the existing OpenAI upload logic
        asyncio.run(openai_upload_main())
        print("--- OpenAI Upload Complete ---")

    except Exception as e:
        print(f"--- OpenAI Upload Failed: {e} ---")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="CCDR Data Pipeline Orchestrator")
    parser.add_argument(
        "--stage1", action="store_true", help="Run only Stage 1: Metadata Ingestion."
    )
    parser.add_argument(
        "--stage2", action="store_true", help="Run only Stage 2: File Processing."
    )
    parser.add_argument(
        "--openai", action="store_true", help="Run OpenAI upload after other stages."
    )
    args = parser.parse_args()

    # Check schema synchronization first
    if not check_schema_sync():
        print("\n❌ Schema synchronization failed. Please resolve schema differences before proceeding.")
        exit(1)

    # Determine which stages to run
    run_s1 = args.stage1 or (not args.stage1 and not args.stage2)
    run_s2 = args.stage2 or (not args.stage1 and not args.stage2)

    if run_s1:
        run_stage_1_metadata_ingestion()

    if run_s2:
        run_stage_2_file_processing()

    if args.openai:
        # OpenAI upload runs after the primary stages are complete
        run_openai_upload()

    print("\nWorkflow finished.")
