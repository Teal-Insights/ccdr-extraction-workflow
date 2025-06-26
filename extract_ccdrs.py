#!/usr/bin/env python3
"""
Main script to run the complete CCDR extraction workflow.

This script runs all extraction steps in sequence:
1. Check schema synchronization with master
2. Check AWS credentials and S3 access (if S3 upload is enabled)
3. Extract publication links from the World Bank repository
4. Extract detailed information from each publication page
5. Add unique IDs to publications and download links
6. Classify file types for each download link
7. Filter and classify which links to download
8. Download the selected files
9. Convert .bin files to .pdf if they are PDF documents
10. Upload publications and documents to the database
11. Upload PDFs to S3 (optional, use --upload-s3 flag)
12. Upload PDFs to OpenAI (optional, use --use-openai flag)

Usage:
    uv run extract_ccdrs.py [--use-openai] [--upload-s3] [--skip-schema-check]
    
Arguments:
    --use-openai           Include the OpenAI upload step in the workflow
    --upload-s3            Include the S3 upload step in the workflow
    --skip-schema-check    Skip the schema synchronization check
"""

import asyncio
import importlib
import argparse
import sys
from dotenv import load_dotenv

from extract.db import check_schema_sync
from extract.upload_pdfs_to_aws_s3 import check_aws_authentication

# Load environment variables
load_dotenv()


def identify_new_publications(scraped_links: list, session: Session) -> list:
    """Queries the DB and filters for publication links that are not yet present."""
    existing_uris = set(session.exec(select(Publication.uri)).all())
    new_links = [link for link in scraped_links if link['url'] not in existing_uris]
    print(f"Found {len(scraped_links)} total publications, {len(new_links)} are new.")
    return new_links


def run_stage_1_metadata_ingestion():
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
    all_links = get_all_publication_links(base_url)

    with Session(engine) as session:
        # 2. Identify New Publications
        new_links_to_process = identify_new_publications(all_links, session)
        
        if not new_links_to_process:
            print("No new publications to process.")
            print("--- Stage 1 Complete ---")
            return
        
        # 3. Process Each New Publication
        for link_info in new_links_to_process:
            print(f"\nProcessing new publication: {link_info['title']}")
            
            # a. Scrape Details
            pub_details = scrape_publication_details(link_info['url'])
            if not pub_details or not pub_details.get("downloadLinks"):
                print(f"  -> Failed to scrape details or no download links found. Skipping.")
                continue

            # b. Get MIME types (lightweight HEAD/GET request)
            valid_links = []
            for link in pub_details["downloadLinks"]:
                file_info = get_file_type_from_url(link['url'], link['text'])
                link['file_info'] = file_info
                
                # Only keep links with valid file info
                if file_info and file_info.get('mime_type') != 'error':
                    valid_links.append(link)
                else:
                    print(f"  -> Skipping link with failed file detection: {link['url']}")
            
            # Check if we have any valid download links
            if not valid_links:
                print(f"  -> No valid download links found. Skipping publication.")
                continue
                
            pub_details["downloadLinks"] = valid_links

            # c. Classify Links
            pub_details["downloadLinks"] = classify_download_links(
                pub_details["downloadLinks"], 
                pub_details.get("title", "Unknown Title"),
                pub_details.get("source_url", "Unknown URL")
            )
            
            # Add metadata from the original link info
            pub_details["source"] = link_info.get("source", "World Bank Open Knowledge Repository")
            pub_details["page_found"] = link_info.get("page_found", 1)
            
            # d. Validate required fields before saving
            if not pub_details.get("title") or not pub_details.get("citation") or not pub_details.get("uri"):
                print(f"  -> Missing required fields (title, citation, or uri). Skipping publication.")
                continue
            
            # e. Validate that we have at least one document to download
            downloadable_links = [link for link in pub_details["downloadLinks"] if link.get("to_download", False)]
            if not downloadable_links:
                print(f"  -> No downloadable documents found. Skipping publication.")
                continue
            
            # 4. Persist to Database in a transaction
            try:
                persist_publication(pub_details, session)
                session.commit()
                print(f"  -> Successfully saved to database with {len(downloadable_links)} downloadable documents.")
            except Exception as e:
                print(f"  -> ERROR: Failed to save to database. Rolling back. Error: {e}")
                session.rollback()

    print("--- Stage 1 Complete ---")


def run_stage_2_file_processing():
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
            print(f"\nProcessing Document ID: {doc.id} for Publication ID: {doc.publication_id}")
            try:
                # a. Download File
                local_path_initial = download_document_file(doc)
                
                # b. Convert & Get File Size
                local_path_final, file_size = analyze_and_prepare_file(local_path_initial)

                # c. Upload to S3
                s3_url = upload_file_to_s3(local_path_final, doc, s3_client, bucket_name)
                
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
                if local_path_initial and local_path_initial != local_path_final and Path(local_path_initial).exists():
                    os.remove(local_path_initial)
                    print(f"  -> Cleaned up temp file: {local_path_initial}")

    print("\n--- Stage 2 Complete ---")


def run_openai_upload():
    """
    Runs the OpenAI upload step as a separate post-processing step.
    Uploads all PDF files to the OpenAI vector store associated with the assistant.
    """
    print("--- Running OpenAI Upload ---")
    
    try:
        # Import module from extract package
        module = importlib.import_module(f"extract.{module_name}")
        
        if is_async:
            # Run async module
            asyncio.run(module.main())
        else:
            # Run sync module
            if hasattr(module, 'main'):
                module.main()
            else:
                # If no main function, the module should have run on import
                pass
        
        print(f"✅ {step_name} completed successfully")
        
    except Exception as e:
        print(f"❌ {step_name} failed: {str(e)}")
        print(f"Error occurred in module: extract.{module_name}")
        raise

def main():
    """Run the complete CCDR extraction workflow."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run the complete CCDR extraction workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run extract_ccdrs.py                                    # Run with schema check
    uv run extract_ccdrs.py --upload-s3                        # Run with S3 upload (includes AWS check)
    uv run extract_ccdrs.py --use-openai                       # Run with OpenAI upload
    uv run extract_ccdrs.py --upload-s3 --use-openai           # Run with both S3 and OpenAI uploads
    uv run extract_ccdrs.py --skip-schema-check                # Skip schema sync check
        """
    )
    parser.add_argument(
        '--use-openai', 
        action='store_true',
        help='Include the OpenAI upload step in the workflow'
    )
    parser.add_argument(
        '--upload-s3',
        action='store_true', 
        help='Include the S3 upload step in the workflow'
    )
    parser.add_argument(
        '--skip-schema-check',
        action='store_true', 
        help='Skip the schema synchronization check'
    )
    
    args = parser.parse_args()
    
    print("Starting CCDR Extraction Workflow")
    if args.use_openai:
        print("OpenAI upload step is ENABLED")
    else:
        print("OpenAI upload step is DISABLED (use --use-openai to enable)")
    
    if args.upload_s3:
        print("S3 upload step is ENABLED")
    else:
        print("S3 upload step is DISABLED (use --upload-s3 to enable)")
    
    if args.skip_schema_check:
        print("Schema sync check is DISABLED (--skip-schema-check flag used)")
    else:
        print("Schema sync check is ENABLED")
    
    print("This will run all extraction steps in sequence...")
    
    try:
        # Check schema synchronization first (unless skipped)
        if not args.skip_schema_check:
            if not check_schema_sync():
                print(f"\n{'='*60}")
                print("💥 WORKFLOW STOPPED!")
                print(f"{'='*60}")
                print("Schema is out of sync with master branch.")
                print("Please sync your schema before running the workflow.")
                print("Use --skip-schema-check to bypass this check if needed.")
                sys.exit(1)
        
        # Check AWS credentials only if S3 upload is enabled
        if args.upload_s3:
            if not check_aws_authentication():
                print(f"\n{'='*60}")
                print("💥 WORKFLOW STOPPED!")
                print(f"{'='*60}")
                print("AWS credentials are not properly configured.")
                print("Please configure your AWS credentials before running the workflow.")
                print("S3 upload requires valid AWS credentials.")
                sys.exit(1)
        
        # Base steps that always run
        steps = [
            ("Extract Publication Links", "extract_publication_links", False),
            ("Extract Publication Details", "extract_publication_details", False),
            ("Add IDs", "add_ids", False),
            ("Classify File Types", "classify_file_types", False),
            ("Filter Download Links", "filter_download_links", True),  # This one is async
            ("Download Files", "download_files", False),
            ("Convert BIN Files", "convert_bin_files", False),
            ("Upload to Database", "upload_pubs_to_db", False),
        ]
        
        # Add S3 upload step if requested
        if args.upload_s3:
            steps.append(("Upload PDFs to S3", "upload_pdfs_to_aws_s3", False))
        
        # Add OpenAI step if requested
        if args.use_openai:
            steps.append(("Upload PDFs to OpenAI", "upload_pdfs_to_openai", True))
        
        for step_name, module_name, is_async in steps:
            run_step(step_name, module_name, is_async)
        
        print(f"\n{'='*60}")
        print("🎉 CCDR EXTRACTION WORKFLOW COMPLETED SUCCESSFULLY!")
        print(f"{'='*60}")
        print("\nAll steps completed. Check the extract/data/ directory for results.")
        print("Publications and documents have been uploaded to the database.")
        
        if args.upload_s3:
            print("PDFs have been uploaded to S3.")
        else:
            print("S3 upload was skipped (use --upload-s3 to enable).")
        
        if args.use_openai:
            print("PDFs have been uploaded to OpenAI vector store.")
        else:
            print("OpenAI upload was skipped (use --use-openai to enable).")
        
    except Exception as e:
        print(f"\n{'='*60}")
        print("💥 WORKFLOW FAILED!")
        print(f"{'='*60}")
        print(f"Error: {str(e)}")
        print("\nWorkflow stopped. Fix the error and try again.")
        exit(1)

if __name__ == "__main__":
    main() 