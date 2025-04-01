#!/usr/bin/env python3

"""
Simple script to extract raw text from a PDF page using pypdf.
Outputs both the raw text and a pymupdf version for comparison.
"""

import argparse
from pathlib import Path
from pypdf import PdfReader
import pymupdf
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_text_pypdf(pdf_path: str, page_num: int) -> str:
    """Extract text from a specific page using pypdf."""
    try:
        reader = PdfReader(pdf_path)
        if page_num >= len(reader.pages):
            raise ValueError(f"Page number {page_num} out of range. PDF has {len(reader.pages)} pages.")
        return reader.pages[page_num].extract_text()
    except Exception as e:
        logger.error(f"Error extracting text with pypdf: {e}")
        raise

def extract_text_pymupdf(pdf_path: str, page_num: int) -> str:
    """Extract text from a specific page using pymupdf."""
    try:
        doc = pymupdf.open(pdf_path)
        if page_num >= len(doc):
            raise ValueError(f"Page number {page_num} out of range. PDF has {len(doc)} pages.")
        return doc[page_num].get_text("text")
    except Exception as e:
        logger.error(f"Error extracting text with pymupdf: {e}")
        raise
    finally:
        if 'doc' in locals():
            doc.close()

def main():
    parser = argparse.ArgumentParser(description='Extract raw text from a PDF page using different libraries')
    parser.add_argument('pdf_path', help='Path to the PDF file')
    parser.add_argument('page_num', type=int, help='Page number to extract (0-based)')
    args = parser.parse_args()

    # Create artifacts directory if it doesn't exist
    artifacts_dir = Path('artifacts')
    artifacts_dir.mkdir(exist_ok=True)

    try:
        # Extract text using both methods
        pypdf_text = extract_text_pypdf(args.pdf_path, args.page_num)
        pymupdf_text = extract_text_pymupdf(args.pdf_path, args.page_num)

        # Save outputs
        pdf_name = Path(args.pdf_path).stem
        pypdf_output = artifacts_dir / f"{pdf_name}_page{args.page_num}_pypdf.txt"
        pymupdf_output = artifacts_dir / f"{pdf_name}_page{args.page_num}_pymupdf.txt"

        pypdf_output.write_text(pypdf_text)
        pymupdf_output.write_text(pymupdf_text)

        logger.info(f"Extracted text saved to:")
        logger.info(f"  pypdf output: {pypdf_output}")
        logger.info(f"  pymupdf output: {pymupdf_output}")

    except Exception as e:
        logger.error(f"Failed to extract text: {e}")
        raise

if __name__ == "__main__":
    main()
