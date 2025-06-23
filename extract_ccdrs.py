#!/usr/bin/env python3
"""
Main script to run the complete CCDR extraction workflow.

This script runs all extraction steps in sequence:
1. Extract publication links from the World Bank repository
2. Extract detailed information from each publication page
3. Add unique IDs to publications and download links
4. Classify file types for each download link
5. Filter and classify which links to download
6. Download the selected files
7. Convert .bin files to .pdf if they are PDF documents
8. Upload publications and documents to the database
9. Upload PDFs to OpenAI (optional, use --use-openai flag)

Usage:
    uv run extract_ccdrs.py [--use-openai]
    
Arguments:
    --use-openai    Include the OpenAI upload step in the workflow
"""

import asyncio
import importlib
import argparse

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
    uv run extract_ccdrs.py                # Run without OpenAI upload
    uv run extract_ccdrs.py --use-openai   # Run with OpenAI upload
        """
    )
    parser.add_argument(
        '--use-openai', 
        action='store_true',
        help='Include the OpenAI upload step in the workflow'
    )
    
    args = parser.parse_args()
    
    print("Starting CCDR Extraction Workflow")
    if args.use_openai:
        print("OpenAI upload step is ENABLED")
    else:
        print("OpenAI upload step is DISABLED (use --use-openai to enable)")
    print("This will run all extraction steps in sequence...")
    
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
    
    # Add OpenAI step if requested
    if args.use_openai:
        steps.append(("Upload PDFs to OpenAI", "upload_pdfs_to_openai", True))
    
    try:
        for step_name, module_name, is_async in steps:
            run_step(step_name, module_name, is_async)
        
        print(f"\n{'='*60}")
        print("🎉 CCDR EXTRACTION WORKFLOW COMPLETED SUCCESSFULLY!")
        print(f"{'='*60}")
        print("\nAll steps completed. Check the extract/data/ directory for results.")
        print("Publications and documents have been uploaded to the database.")
        
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