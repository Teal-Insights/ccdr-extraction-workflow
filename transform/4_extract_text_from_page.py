#!/usr/bin/env python3

"""
1. Recursively processes all PDF files in `extract/data/pub_*` directories.
2. Extracts all text from each page of every PDF.
3. Saves the results to `transform/text/text_content.json`.

Output:
- A JSON file (`text_content.json`) with the following structure:
  - `publications`: List of publications (each identified by `pub_id`).
    - `documents`: List of documents (each identified by `doc_id`).
      - `pages`: List of pages (1-based numbering) with extracted text content.
"""

import json
from pathlib import Path
import pymupdf
import logging
from pydantic import BaseModel, Field
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pydantic models
class Page(BaseModel):
    """Represents a single page from a PDF document."""
    page_number: int = Field(..., description="1-based page number")
    text_content: str = Field(..., description="Extracted text content from the page")

class Document(BaseModel):
    """Represents a document with multiple pages."""
    doc_id: str = Field(..., description="Unique document identifier")
    pages: List[Page] = Field(default_factory=list, description="List of pages in the document")

class Publication(BaseModel):
    """Represents a publication containing multiple documents."""
    pub_id: str = Field(..., description="Unique publication identifier")
    documents: List[Document] = Field(default_factory=list, description="List of documents in the publication")

class ExtractedContent(BaseModel):
    """Root model containing all publications."""
    publications: List[Publication] = Field(default_factory=list, description="List of all publications")

def extract_text_from_pdf(pdf_path: Path) -> List[Page]:
    """Extract text from all pages in a PDF using pymupdf."""
    pages = []
    try:
        doc: pymupdf.Document = pymupdf.open(str(pdf_path))
        for page_num in range(len(doc)):
            text_content = doc[page_num].get_text("text")
            pages.append(Page(
                page_number=page_num + 1,  # Convert to 1-based page numbers
                text_content=text_content
            ))
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
    
    extracted_content = ExtractedContent()
    
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
        
        publication = Publication(pub_id=pub_id)
        
        for pdf_file in pdf_files:
            # Extract doc_id from filename (e.g., 'dl_001.pdf' -> 'dl_001')
            doc_id = pdf_file.stem
            
            try:
                pages = extract_text_from_pdf(pdf_file)
                document = Document(doc_id=doc_id, pages=pages)
                publication.documents.append(document)
                logger.info(f"Processed {pdf_file}")
            except Exception as e:
                logger.error(f"Failed to process {pdf_file}: {e}")
                continue
        
        extracted_content.publications.append(publication)
    
    # Save the results
    output_file = output_dir / 'text_content.json'
    with output_file.open('w', encoding='utf-8') as f:
        json.dump(extracted_content.model_dump(), f, ensure_ascii=False, indent=2)
    
    logger.info(f"Results saved to {output_file}")

def main():
    try:
        process_pdfs()
    except Exception as e:
        logger.error(f"Failed to process PDFs: {e}")
        raise

if __name__ == "__main__":
    main()
