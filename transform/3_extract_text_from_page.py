#!/usr/bin/env python3

"""
Script to extract raw text from all PDFs in extract/data subdirectories.
Outputs the extracted text in a structured JSON format.
"""

import json
from pathlib import Path
import pymupdf
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_text_from_pdf(pdf_path: Path) -> list:
    """Extract text from all pages in a PDF using pymupdf."""
    pages = []
    try:
        doc: pymupdf.Document = pymupdf.open(str(pdf_path))
        for page_num in range(len(doc)):
            text_content = doc[page_num].get_text("text")
            pages.append({
                "page_number": page_num + 1,  # Convert to 1-based page numbers
                "text_content": text_content
            })
        return pages
    except Exception as e:
        logger.error(f"Error extracting text from {pdf_path}: {e}")
        raise
    finally:
        if 'doc' in locals():
            doc.close()

def process_pdfs():
    """Process all PDFs in extract/data subdirectories."""
    data_dir = Path('extract/data')
    output_dir = Path('transform/text')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    publications = []
    
    # Get all pub_* directories
    pub_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir() and d.name.startswith('pub_')])
    
    for pub_dir in pub_dirs:
        # Extract pub_id from directory name
        pub_id = pub_dir.name
        
        # Get all PDF files in the publication directory
        pdf_files = sorted([f for f in pub_dir.glob('*.pdf')])
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {pub_dir}")
            continue
            
        documents = []
        for pdf_file in pdf_files:
            # Extract doc_id from filename (e.g., 'dl_001.pdf' -> 'dl_001')
            doc_id = pdf_file.stem
            
            try:
                pages = extract_text_from_pdf(pdf_file)
                documents.append({
                    "doc_id": doc_id,
                    "pages": pages
                })
                logger.info(f"Processed {pdf_file}")
            except Exception as e:
                logger.error(f"Failed to process {pdf_file}: {e}")
                continue
        
        publications.append({
            "pub_id": pub_id,
            "documents": documents
        })
    
    # Save the results
    output_file = output_dir / 'text_content.json'
    with output_file.open('w', encoding='utf-8') as f:
        json.dump(publications, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Results saved to {output_file}")

def main():
    try:
        process_pdfs()
    except Exception as e:
        logger.error(f"Failed to process PDFs: {e}")
        raise

if __name__ == "__main__":
    main()
