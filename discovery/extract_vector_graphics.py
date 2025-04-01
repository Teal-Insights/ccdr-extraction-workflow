#!/usr/bin/env python3

"""
Script to extract vector graphics content from a PDF page.
Particularly useful for charts, diagrams, and other vector-based content.
"""

import argparse
from pathlib import Path
import logging
import json
from typing import List, Dict, Any
import pymupdf
from pypdf import PdfReader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_vector_content_pymupdf(pdf_path: str, page_num: int) -> List[Dict[str, Any]]:
    """
    Extract vector graphics content from a page using pymupdf.
    Returns a list of drawing commands and their properties.
    """
    graphics = []
    try:
        doc = pymupdf.open(pdf_path)
        if page_num >= len(doc):
            raise ValueError(f"Page number {page_num} out of range. PDF has {len(doc)} pages.")
        
        page = doc[page_num]
        
        # Method 1: Get all drawing commands
        logger.info("Getting drawing commands...")
        drawings = page.get_drawings()
        logger.info(f"Found {len(drawings)} drawing commands")
        
        for idx, drawing in enumerate(drawings):
            draw_type = drawing.get('type', 'unknown')
            if draw_type != 'image':  # We're only interested in vector content
                graphics.append({
                    'index': idx,
                    'type': draw_type,
                    'rect': drawing.get('rect', None),  # Bounding box
                    'color': drawing.get('color', None),
                    'stroke_width': drawing.get('width', None),
                    'fill_opacity': drawing.get('fill_opacity', None),
                    'stroke_opacity': drawing.get('stroke_opacity', None),
                    'items': drawing.get('items', [])  # Actual drawing commands
                })
        
        # Method 2: Get text blocks that might be part of charts
        logger.info("Getting text blocks...")
        blocks = page.get_text("dict")["blocks"]
        text_blocks = [b for b in blocks if b["type"] == 0]  # type 0 = text
        
        # Try to identify text that might be part of charts (like axis labels)
        # This is a simple heuristic - we look for short text blocks that might be numbers
        potential_chart_text = []
        for block in text_blocks:
            text = block.get('lines', [{}])[0].get('spans', [{}])[0].get('text', '').strip()
            try:
                # If it's a number or a short label, it might be an axis label
                float(text)
                is_likely_label = True
            except ValueError:
                # If it's not a number, check if it's short enough to be a label
                is_likely_label = len(text) < 20
            
            if is_likely_label:
                potential_chart_text.append({
                    'text': text,
                    'bbox': block.get('bbox'),
                    'font': block.get('lines', [{}])[0].get('spans', [{}])[0].get('font', 'unknown'),
                    'size': block.get('lines', [{}])[0].get('spans', [{}])[0].get('size', 0)
                })
        
        return {
            'vector_graphics': graphics,
            'potential_labels': potential_chart_text
        }
    
    except Exception as e:
        logger.error(f"Error extracting vector content with pymupdf: {e}")
        raise
    finally:
        if 'doc' in locals():
            doc.close()

def extract_vector_content_pypdf(pdf_path: str, page_num: int) -> Dict[str, Any]:
    """
    Extract vector graphics content from a page using pypdf.
    Returns content stream and resource information.
    """
    try:
        reader = PdfReader(pdf_path)
        if page_num >= len(reader.pages):
            raise ValueError(f"Page number {page_num} out of range. PDF has {len(reader.pages)} pages.")
        
        page = reader.pages[page_num]
        
        # Get the content stream
        content = page.get_contents()
        if content is None:
            logger.warning("No content stream found")
            return {}
        
        # Get the resources dictionary
        resources = page['/Resources'] if '/Resources' in page else {}
        
        # Look for Form XObjects (reusable graphics)
        xobjects = {}
        if '/XObject' in resources:
            for key, obj in resources['/XObject'].items():
                if obj['/Subtype'] == '/Form':
                    xobjects[key] = {
                        'type': 'form',
                        'bbox': obj.get('/BBox', None),
                        'matrix': obj.get('/Matrix', None),
                        'resources': obj.get('/Resources', None)
                    }
        
        return {
            'xobjects': xobjects,
            'has_content_stream': bool(content)
        }
    
    except Exception as e:
        logger.error(f"Error extracting vector content with pypdf: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Extract vector graphics from a PDF page')
    parser.add_argument('pdf_path', help='Path to the PDF file')
    parser.add_argument('page_num', type=int, help='Page number to extract (0-based)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create output directory
    artifacts_dir = Path('artifacts')
    artifacts_dir.mkdir(exist_ok=True)
    
    # Create output report file
    pdf_name = Path(args.pdf_path).stem
    report_path = artifacts_dir / f"{pdf_name}_page{args.page_num}_vector_graphics_report.txt"
    
    try:
        # Extract vector content using both methods
        pymupdf_content = extract_vector_content_pymupdf(args.pdf_path, args.page_num)
        pypdf_content = extract_vector_content_pypdf(args.pdf_path, args.page_num)
        
        # Generate report
        with open(report_path, 'w') as f:
            f.write(f"Vector Graphics Report for {pdf_name}, page {args.page_num}\n")
            f.write("=" * 80 + "\n\n")
            
            # PyMuPDF Results
            f.write("PyMuPDF Results:\n")
            f.write("-" * 40 + "\n")
            
            f.write("\nVector Graphics Commands:\n")
            if pymupdf_content['vector_graphics']:
                for i, graphic in enumerate(pymupdf_content['vector_graphics']):
                    f.write(f"\nGraphic {i+1}:\n")
                    for k, v in graphic.items():
                        if k != 'items':  # Don't dump the full items list as it can be very long
                            f.write(f"  {k}: {v}\n")
                    f.write(f"  number of drawing commands: {len(graphic.get('items', []))}\n")
            else:
                f.write("No vector graphics detected\n")
            
            f.write("\nPotential Chart Labels:\n")
            if pymupdf_content['potential_labels']:
                for i, label in enumerate(pymupdf_content['potential_labels']):
                    f.write(f"\nLabel {i+1}:\n")
                    for k, v in label.items():
                        f.write(f"  {k}: {v}\n")
            else:
                f.write("No potential chart labels detected\n")
            
            # PyPDF Results
            f.write("\nPyPDF Results:\n")
            f.write("-" * 40 + "\n")
            f.write(f"\nContent Stream Present: {pypdf_content['has_content_stream']}\n")
            
            f.write("\nForm XObjects (Reusable Graphics):\n")
            if pypdf_content['xobjects']:
                for name, xobject in pypdf_content['xobjects'].items():
                    f.write(f"\nXObject {name}:\n")
                    for k, v in xobject.items():
                        f.write(f"  {k}: {v}\n")
            else:
                f.write("No Form XObjects detected\n")
        
        logger.info(f"Vector graphics report saved to: {report_path}")
        
    except Exception as e:
        logger.error(f"Failed to extract vector graphics: {e}")
        raise

if __name__ == "__main__":
    main() 