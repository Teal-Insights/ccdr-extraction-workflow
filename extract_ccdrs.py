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

def run_step(step_name: str, module_name: str, is_async: bool = False):
    """Run a single step of the extraction workflow."""
    print(f"\n{'='*60}")
    print(f"STEP: {step_name}")
    print(f"{'='*60}")
    
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