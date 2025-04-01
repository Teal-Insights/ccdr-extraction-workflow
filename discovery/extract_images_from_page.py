#!/usr/bin/env python3

"""
Script to extract images from a PDF page using both pypdf and pymupdf.
Note: pypdf has limited image extraction capabilities compared to pymupdf.
"""

import argparse
from pathlib import Path
import logging
from typing import List, Dict, Any
import pymupdf
from pypdf import PdfReader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_images_pypdf(pdf_path: str, page_num: int) -> List[Dict[str, Any]]:
    """
    Attempt to extract images from a specific page using pypdf.
    Uses direct page images, stamp annotation images, and attachments.
    """
    images = []
    try:
        reader = PdfReader(pdf_path)
        if page_num >= len(reader.pages):
            raise ValueError(f"Page number {page_num} out of range. PDF has {len(reader.pages)} pages.")
        
        page = reader.pages[page_num]
        
        # Method 1: Direct page images
        logger.info("Trying PyPDF Method 1: Direct page images")
        for img_idx, image in enumerate(page.images):
            try:
                image_data = {
                    'method': 'direct_image',
                    'name': image.name,
                    'index': img_idx,
                }
                images.append(image_data)
                
                # Save the image
                img_filename = f"page_{page_num}_pypdf_img_{img_idx}_{image.name}"
                img_path = Path('artifacts/images') / img_filename
                
                with open(img_path, "wb") as fp:
                    fp.write(image.data)
                    image_data['saved_path'] = str(img_path)
                
            except Exception as e:
                logger.warning(f"Failed to extract direct image {img_idx}: {e}")
        
        # Method 2: Stamp annotation images
        logger.info("Trying PyPDF Method 2: Stamp annotations")
        if "/Annots" in page:
            try:
                annotations = page["/Annots"]
                if annotations:
                    for annot_idx, annotation in enumerate(annotations):
                        annot_obj = annotation.get_object()
                        # Check if it's a stamp annotation
                        if annot_obj.get("/Subtype") == "/Stamp":
                            try:
                                # Navigate through the annotation structure
                                if "/AP" in annot_obj:
                                    ap = annot_obj["/AP"]
                                    if "/N" in ap:
                                        n = ap["/N"]
                                        if "/Resources" in n and "/XObject" in n["/Resources"]:
                                            x_objects = n["/Resources"]["/XObject"]
                                            for key, x_obj in x_objects.items():
                                                try:
                                                    # Attempt to decode image
                                                    img = x_obj.decode_as_image()
                                                    if img:
                                                        image_data = {
                                                            'method': 'stamp_annotation',
                                                            'name': f'stamp_{annot_idx}_{key}',
                                                            'index': annot_idx,
                                                            'width': img.width,
                                                            'height': img.height,
                                                            'mode': img.mode,
                                                        }
                                                        images.append(image_data)
                                                        
                                                        # Save the image
                                                        img_filename = f"page_{page_num}_pypdf_stamp_{annot_idx}_{key}.png"
                                                        img_path = Path('artifacts/images') / img_filename
                                                        img.save(str(img_path))
                                                        image_data['saved_path'] = str(img_path)
                                                        
                                                except Exception as e:
                                                    logger.warning(f"Failed to decode stamp image {key}: {e}")
                            except Exception as e:
                                logger.warning(f"Failed to process stamp annotation {annot_idx}: {e}")
            except Exception as e:
                logger.warning(f"Failed to process annotations: {e}")

        # Method 3: Check for attachments
        logger.info("Trying PyPDF Method 3: Attachments")
        try:
            # First try the object-oriented approach for more details
            for attachment in reader.attachment_list:
                try:
                    # Try to determine if this is an image by checking name extension
                    name = attachment.name.lower()
                    if any(name.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']):
                        image_data = {
                            'method': 'attachment',
                            'name': attachment.name,
                            'alternative_name': attachment.alternative_name,
                            'type': 'image_attachment'
                        }
                        images.append(image_data)
                        
                        # Save the attachment
                        img_filename = f"attachment_{attachment.name}"
                        img_path = Path('artifacts/images') / img_filename
                        
                        with open(img_path, "wb") as fp:
                            fp.write(attachment.content)
                            image_data['saved_path'] = str(img_path)
                            
                except Exception as e:
                    logger.warning(f"Failed to extract attachment {attachment.name}: {e}")
            
            # Also try the dictionary approach as backup
            for name, content_list in reader.attachments.items():
                try:
                    # Only process if it looks like an image
                    if any(name.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']):
                        for i, content in enumerate(content_list):
                            image_data = {
                                'method': 'attachment_dict',
                                'name': name,
                                'index': i,
                                'type': 'image_attachment'
                            }
                            images.append(image_data)
                            
                            # Save the attachment
                            img_filename = f"attachment_{name}_{i}"
                            img_path = Path('artifacts/images') / img_filename
                            
                            with open(img_path, "wb") as fp:
                                fp.write(content)
                                image_data['saved_path'] = str(img_path)
                                
                except Exception as e:
                    logger.warning(f"Failed to extract attachment {name}: {e}")
                    
        except Exception as e:
            logger.warning(f"Failed to process attachments: {e}")
        
        logger.info(f"PyPDF found total of {len(images)} images/attachments")
        return images
                        
    except Exception as e:
        logger.error(f"Error extracting images with pypdf: {e}")
        raise

def extract_images_pymupdf(pdf_path: str, page_num: int, output_dir: Path) -> List[Dict[str, Any]]:
    """
    Extract images from a specific page using pymupdf.
    Returns list of image info and saves images to output directory.
    Uses multiple methods to try to find all images.
    """
    images = []
    try:
        # Open document and convert to PDF if it's not already one
        doc = pymupdf.open(pdf_path)
        if doc.is_pdf:
            logger.info("Document is already PDF")
        else:
            logger.info("Converting document to PDF")
            pdf_bytes = doc.convert_to_pdf()
            doc.close()
            doc = pymupdf.open("pdf", pdf_bytes)
        
        if page_num >= len(doc):
            raise ValueError(f"Page number {page_num} out of range. PDF has {len(doc)} pages.")
        
        page = doc[page_num]
        
        # Method 1: get_images() - finds images in PDF's /Resources and handles masks
        logger.info("Trying PyMuPDF Method 1: get_images()")
        image_list = page.get_images(full=True)
        logger.info(f"Method 1 found {len(image_list)} images")
        
        for img_idx, img_info in enumerate(image_list):
            xref = img_info[0]  # Cross-reference number
            smask = img_info[1]  # Mask reference number (if any)
            
            try:
                base_image = doc.extract_image(xref)
                if not base_image:
                    logger.warning(f"Could not extract image {xref}. Skipping.")
                    continue
                
                # Handle image masks if present
                if smask > 0:
                    logger.info(f"Processing image mask for image {xref}")
                    try:
                        # Create pixmap from base image
                        pix1 = pymupdf.Pixmap(base_image["image"])
                        # Create pixmap from mask
                        mask = pymupdf.Pixmap(doc.extract_image(smask)["image"])
                        # Combine image with mask
                        pix = pymupdf.Pixmap(pix1, mask)
                        # Get the image data with alpha channel
                        img_data = pix.tobytes()
                        has_mask = True
                    except Exception as e:
                        logger.warning(f"Failed to process mask for image {xref}: {e}")
                        img_data = base_image["image"]
                        has_mask = False
                else:
                    img_data = base_image["image"]
                    has_mask = False
                
                # Get image metadata
                image_data = {
                    'method': 'get_images',
                    'index': img_idx,
                    'width': img_info[2],
                    'height': img_info[3],
                    'color_space': base_image.get('colorspace', 'unknown'),
                    'bits_per_component': base_image.get('bpc', 'unknown'),
                    'extension': base_image['ext'],
                    'has_mask': has_mask,
                    'size': len(img_data)
                }
                
                # Save the image
                ext = 'png' if has_mask else base_image['ext']
                img_filename = f"page_{page_num}_method1_img_{img_idx}.{ext}"
                img_path = output_dir / img_filename
                
                with open(img_path, 'wb') as img_file:
                    img_file.write(img_data)
                
                image_data['saved_path'] = img_path
                images.append(image_data)
                
            except Exception as e:
                logger.warning(f"Failed to extract image {xref}: {e}")
        
        # Method 2: get_drawings() - can find images in vector graphics
        logger.info("Trying PyMuPDF Method 2: get_drawings()")
        drawings = page.get_drawings()
        drawing_images = [d for d in drawings if d.get('type') == 'image']
        logger.info(f"Method 2 found {len(drawing_images)} images in drawings")
        
        for draw_idx, drawing in enumerate(drawing_images):
            try:
                xref = drawing.get('xref')
                if not xref:
                    continue
                    
                base_image = doc.extract_image(xref)
                if not base_image:
                    continue
                
                image_data = {
                    'method': 'get_drawings',
                    'index': draw_idx,
                    'rect': drawing.get('rect', 'unknown'),
                    'color_space': base_image.get('colorspace', 'unknown'),
                    'bits_per_component': base_image.get('bpc', 'unknown'),
                    'extension': base_image['ext'],
                    'size': len(base_image['image'])
                }
                
                img_filename = f"page_{page_num}_method2_img_{draw_idx}.{base_image['ext']}"
                img_path = output_dir / img_filename
                
                with open(img_path, 'wb') as img_file:
                    img_file.write(base_image['image'])
                
                image_data['saved_path'] = img_path
                images.append(image_data)
                
            except Exception as e:
                logger.warning(f"Failed to extract drawing image {draw_idx}: {e}")
        
        # Method 3: get_text("dict") - finds all images shown on the page
        logger.info("Trying PyMuPDF Method 3: get_text('dict')")
        blocks = page.get_text("dict")["blocks"]
        image_blocks = [b for b in blocks if b["type"] == 1]  # type 1 = image
        logger.info(f"Method 3 found {len(image_blocks)} image blocks")
        
        for block_idx, block in enumerate(image_blocks):
            try:
                # Image blocks contain the actual image data
                image_data = {
                    'method': 'image_blocks',
                    'index': block_idx,
                    'bbox': block.get('bbox', 'unknown'),
                    'width': block.get('width', 'unknown'),
                    'height': block.get('height', 'unknown'),
                    'bpc': block.get('bpc', 'unknown'),
                    'colorspace': block.get('colorspace', 'unknown'),
                    'size': block.get('size', 'unknown'),
                    'extension': block.get('ext', 'unknown'),
                }
                
                if 'image' in block:
                    img_filename = f"page_{page_num}_method3_img_{block_idx}.{block['ext']}"
                    img_path = output_dir / img_filename
                    
                    with open(img_path, 'wb') as img_file:
                        img_file.write(block['image'])
                    
                    image_data['saved_path'] = img_path
                
                images.append(image_data)
                
            except Exception as e:
                logger.warning(f"Failed to process image block {block_idx}: {e}")
        
        return images
    
    except Exception as e:
        logger.error(f"Error extracting images with pymupdf: {e}")
        raise
    finally:
        if 'doc' in locals():
            doc.close()

def main():
    parser = argparse.ArgumentParser(description='Extract images from a PDF page using different libraries')
    parser.add_argument('pdf_path', help='Path to the PDF file')
    parser.add_argument('page_num', type=int, help='Page number to extract (0-based)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create output directories
    artifacts_dir = Path('artifacts')
    images_dir = artifacts_dir / 'images'
    images_dir.mkdir(parents=True, exist_ok=True)
    
    # Create output report file
    pdf_name = Path(args.pdf_path).stem
    report_path = artifacts_dir / f"{pdf_name}_page{args.page_num}_image_extraction_report.txt"
    
    try:
        # Extract images using both methods
        pypdf_images = extract_images_pypdf(args.pdf_path, args.page_num)
        pymupdf_images = extract_images_pymupdf(args.pdf_path, args.page_num, images_dir)
        
        # Generate report
        with open(report_path, 'w') as f:
            f.write(f"Image Extraction Report for {pdf_name}, page {args.page_num}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write("PyPDF Results:\n")
            f.write("-" * 40 + "\n")
            if pypdf_images:
                for i, img in enumerate(pypdf_images):
                    f.write(f"\nImage {i+1}:\n")
                    for k, v in img.items():
                        f.write(f"  {k}: {v}\n")
            else:
                f.write("No images detected or extracted\n")
            
            f.write("\nPyMuPDF Results:\n")
            f.write("-" * 40 + "\n")
            if pymupdf_images:
                for i, img in enumerate(pymupdf_images):
                    f.write(f"\nImage {i+1}:\n")
                    for k, v in img.items():
                        f.write(f"  {k}: {v}\n")
            else:
                f.write("No images detected or extracted\n")
        
        logger.info(f"Extraction report saved to: {report_path}")
        if pymupdf_images:
            logger.info(f"Extracted images saved to: {images_dir}")
        
    except Exception as e:
        logger.error(f"Failed to extract images: {e}")
        raise

if __name__ == "__main__":
    main() 