"""
1. Converts all PDF pages to images.
2. Detects and extracts regions of interest (ROIs) from the images.
3. Validates and refines the extracted regions (optional, currently disabled).
4. Saves the extracted regions as individual images and metadata (bounding boxes, labels, descriptions) in a structured JSON format.

Output:
- A directory for each processed document containing:
  - Extracted image regions (PNG files).
  - `visualization.png` of the original page with detected regions shown as a bounding box.
  - Metadata (JSON) describing the regions and their properties.
- A summary JSON file (`document_regions.json`) for each document, listing all extracted regions.
- A processing status file (`processing_status.json`) to track progress and handle failures.
"""

import os
import sys
from pathlib import Path
from typing import Literal, Optional
import pymupdf
from litellm import completion
from litellm.exceptions import RateLimitError
import base64
from PIL import Image, ImageDraw
from dotenv import load_dotenv
import io
from pydantic import BaseModel
from typing import List
import json
import random
import asyncio
from datetime import datetime
from functools import partial
from asyncio import Semaphore
import aiofiles


# Feature flags
ENABLE_FIXUP = False  # Set to True to enable the fixup validation step

class BoundingBox(BaseModel):
    label: Literal["chart", "graph", "diagram", "map", "photo", "table", "text_box"]
    description: str
    bbox_2d: List[int]

class ImageRegions(BaseModel):
    regions: List[BoundingBox]

class PageRegion(BaseModel):
    image_path: str
    label: Literal["chart", "graph", "diagram", "map", "photo"]
    bbox_normalized: List[int]
    bbox_pixels: List[int]
    description: str

class PageData(BaseModel):
    page_number: int
    regions: List[PageRegion]

class DocumentData(BaseModel):
    dl_id: str
    pages: List[PageData]

class ProcessingStatus(BaseModel):
    """Tracks the processing status of a document"""
    dl_id: str
    last_processed_page: int
    total_pages: int
    completed: bool
    last_updated: datetime
    error: Optional[str] = None
    failed_pages: List[int] = []  # Track pages that failed processing

class FixupResult(BaseModel):
    """Results from the fixup validation of an extracted region"""
    matches_description: bool
    is_contentful: Literal["contentful", "decorative"]
    revised_bbox: Optional[List[int]]


def get_pdf_paths(base_dir: str = "extract/data") -> List[str]:
    """
    Get relative paths to all PDF files in the specified directory and its subdirectories.
    
    Args:
        base_dir (str): Base directory to search for PDFs. Defaults to "extract/data".
        
    Returns:
        List[str]: List of relative paths to PDF files, including the base_dir in the paths.
    """
    pdf_paths = []
    base_path = Path(base_dir)
    
    # Check if the directory exists
    if not base_path.exists():
        return pdf_paths
    
    # Walk through the directory and its subdirectories
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.lower().endswith('.pdf'):
                # Get the full path and convert it to relative path
                full_path = Path(root) / file
                relative_path = str(full_path)
                pdf_paths.append(relative_path)
    
    return sorted(pdf_paths)  # Sort paths for consistent ordering 


def convert_page_to_image(doc: pymupdf.Document, page_num: int, dpi: int = 300) -> Image.Image:
    """Convert a PDF page to a PIL Image with specified DPI."""
    page = doc[page_num]
    pix = page.get_pixmap(dpi=dpi)

    # Convert PyMuPDF pixmap to PIL Image
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return img


def load_or_create_status(output_dir: Path) -> ProcessingStatus:
    """Load existing processing status or create new one"""
    status_path = output_dir / "processing_status.json"
    if status_path.exists():
        with open(status_path) as f:
            data = json.load(f)
            # Convert string to datetime
            data['last_updated'] = datetime.fromisoformat(data['last_updated'])
            return ProcessingStatus(**data)
    return None


def save_status(status: ProcessingStatus, output_dir: Path):
    """Save current processing status"""
    status_path = output_dir / "processing_status.json"
    with open(status_path, "w") as f:
        # Convert datetime to string for JSON serialization
        status_dict = status.model_dump()
        status_dict['last_updated'] = status_dict['last_updated'].isoformat()
        json.dump(status_dict, f, indent=2)


async def save_status_async(status: ProcessingStatus, output_dir: Path):
    """Save current processing status asynchronously"""
    status_path = output_dir / "processing_status.json"
    status_dict = status.model_dump()
    status_dict['last_updated'] = status_dict['last_updated'].isoformat()
    async with aiofiles.open(status_path, "w") as f:
        await f.write(json.dumps(status_dict, indent=2))


async def save_document_data_async(document_data: DocumentData, json_path: Path):
    """Save document data asynchronously"""
    async with aiofiles.open(json_path, "w") as f:
        await f.write(json.dumps(document_data.model_dump(), indent=2))


async def detect_content_regions_with_retry(
    image: Image.Image,
    api_key: str,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    semaphore: Semaphore = None
) -> list:
    """Use Gemini to detect contentful regions in the image with async retry logic."""
    # Set environment variable for LiteLLM
    os.environ["GEMINI_API_KEY"] = api_key

    prompt = """Analyze this page, identify figures, and provide bounding box coordinates (y1,x1,y2,x2) for each figure normalized to 0-1000.

    For each figure, include:
    1. A label from: "chart", "graph", "diagram", "map", "photo", "table", or "text_box"
    2. A context-aware description of what the figure shows/communicates"""

    retry_count = 0
    last_exception = None

    while retry_count < max_retries:
        try:
            # Convert PIL Image to base64
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            img_base64 = base64.b64encode(img_byte_arr.getvalue()).decode('utf-8')

            # Create message content with text and image
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{img_base64}"
                            }
                        }
                    ]
                }
            ]

            # Use semaphore to limit concurrent API calls
            async with semaphore:
                # Convert synchronous completion call to async using run_in_executor
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    partial(
                        completion,
                        model="gemini/gemini-2.5-flash-preview-05-20",
                        messages=messages,
                        response_format={"type": "json_object", "response_schema": ImageRegions.model_json_schema()}
                    )
                )

            # Parse the response
            print("Raw Gemini response:")
            print(response.choices[0].message.content)
            
            # Parse JSON response into Pydantic model
            regions_data = ImageRegions.model_validate_json(response.choices[0].message.content)
            return regions_data.regions

        except RateLimitError as e:
            last_exception = e
            retry_count += 1
            if retry_count < max_retries:
                # Calculate exponential backoff with jitter
                delay = min(max_delay, base_delay * (2 ** retry_count) + random.uniform(0, 1))
                print(f"Rate limit hit. Retrying in {delay:.2f} seconds... (Attempt {retry_count}/{max_retries})")
                await asyncio.sleep(delay)
            else:
                print(f"Max retries ({max_retries}) exceeded. Rate limit error: {str(e)}")
                raise last_exception
        except Exception as e:
            print(f"Error during Gemini API call or response parsing: {str(e)}")
            if hasattr(e, 'response'):
                print(f"API Response: {e.response}")
            return []

    return []


async def fixup_crop(
    image: Image.Image,
    label: str,
    description: str,
    original_bbox: List[int],
    api_key: str,
    semaphore: Semaphore,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
) -> FixupResult:
    """
    Validate and potentially fix up an extracted region using Gemini.
    """
    os.environ["GEMINI_API_KEY"] = api_key

    # Convert image to base64
    img_bytes = io.BytesIO()
    image.save(img_bytes, format="PNG")
    img_base64 = base64.b64encode(img_bytes.getvalue()).decode("utf-8")

    prompt = f"""
You are evaluating an extracted figure from a PDF.

1. Does this figure match the description: "{description}"? Return true or false.
2. Is it contentful (like an illustrative chart, graph, diagram, map, or photo) in the context of an economic report, or purely decorative (like a logo, watermark, or other non-informative image)? Return "contentful" or "decorative".
3. If needed, revise the bounding box [ymin, xmin, ymax, xmax] (normalized 0–1000) to improve the crop. A common problem is that some of the figure is cut off by an overzealous crop, in which case you should enlarge the bounding box to include the missing parts.
   Original: {original_bbox}. Return null if no change is needed.

Respond with a JSON object like:
{{"matches_description": true, "is_contentful": "contentful", "revised_bbox": [100,200,700,800]}}
"""

    retry_count = 0
    last_exception = None

    while retry_count < max_retries:
        try:
            messages = [{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_base64}"}}
                ]
            }]

            async with semaphore:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None,
                    partial(
                        completion,
                        model="gemini/gemini-2.5-flash-preview-05-20",
                        messages=messages,
                        response_format={"type": "json_object", "response_schema": FixupResult.model_json_schema()}
                    )
                )

            return FixupResult.model_validate_json(response.choices[0].message.content)

        except RateLimitError as e:
            last_exception = e
            retry_count += 1
            if retry_count < max_retries:
                delay = min(max_delay, base_delay * (2 ** retry_count) + random.uniform(0, 1))
                print(f"Rate limit hit in fixup. Retrying in {delay:.2f} seconds... (Attempt {retry_count}/{max_retries})")
                await asyncio.sleep(delay)
            else:
                print(f"Max retries ({max_retries}) exceeded in fixup. Rate limit error: {str(e)}")
                raise last_exception
        except Exception as e:
            print(f"Error during fixup validation: {str(e)}")
            if hasattr(e, 'response'):
                print(f"API Response: {e.response}")
            # Return a default result that accepts the region as-is
            return FixupResult(matches_description=True, is_contentful="contentful", revised_bbox=None)

    return FixupResult(matches_description=True, is_contentful="contentful", revised_bbox=None)


def normalize_coordinates(coords: List[int], image_width: int, image_height: int) -> List[int]:
    """Convert normalized coordinates (0-1000) to pixel coordinates."""
    ymin, xmin, ymax, xmax = coords
    return [
        int((ymin / 1000.0) * image_height),
        int((xmin / 1000.0) * image_width),
        int((ymax / 1000.0) * image_height),
        int((xmax / 1000.0) * image_width)
    ]


async def extract_and_save_regions(
    image: Image.Image,
    regions: List[BoundingBox],
    output_dir: Path,
    page_num: int,
    api_key: str,
    semaphore: Semaphore
) -> PageData:
    """Extract and save each detected region as a separate image."""
    # Create page-specific directory
    page_dir = output_dir / f"page_{page_num:03d}"
    page_dir.mkdir(parents=True, exist_ok=True)

    # Create a copy for visualization
    viz_image = image.copy()
    draw = ImageDraw.Draw(viz_image)

    # Initialize page data
    page_data = PageData(
        page_number=page_num,
        regions=[]
    )

    # Get event loop once at the start
    loop = asyncio.get_running_loop()

    for i, region in enumerate(regions):
        # Convert normalized coordinates to pixel coordinates
        pixel_coords = normalize_coordinates(
            region.bbox_2d,
            image.width,
            image.height
        )
        ymin, xmin, ymax, xmax = pixel_coords
        
        # Extract region
        region_img = image.crop((xmin, ymin, xmax, ymax))

        if ENABLE_FIXUP:
            # Validate and potentially fix up the region
            fixup_result = await fixup_crop(
                region_img,
                region.label,
                region.description,
                region.bbox_2d,
                api_key,
                semaphore
            )

            # Save fixup results
            fixup_path = page_dir / f"region_{i:03d}_fixup.json"
            async with aiofiles.open(fixup_path, "w") as f:
                await f.write(json.dumps(fixup_result.model_dump(), indent=2))

            # Skip if region is not valid or decorative
            if not fixup_result.matches_description or fixup_result.is_contentful == "decorative":
                print(f"  Skipping region {i}: {'description mismatch' if not fixup_result.matches_description else 'decorative'}")
                continue

            # Use revised bbox if provided
            if fixup_result.revised_bbox:
                print(f"  Updating bbox for region {i}")
                pixel_coords = normalize_coordinates(
                    fixup_result.revised_bbox,
                    image.width,
                    image.height
                )
                ymin, xmin, ymax, xmax = pixel_coords
                region_img = image.crop((xmin, ymin, xmax, ymax))
                region.bbox_2d = fixup_result.revised_bbox

        # Save the region image
        region_filename = f"region_{i:03d}.png"
        region_path = page_dir / region_filename
        await loop.run_in_executor(None, region_img.save, region_path)

        # Draw rectangle and add label on visualization image
        draw.rectangle([xmin, ymin, xmax, ymax], outline="red", width=3)
        draw.text((xmin + 5, ymin + 5), f"{i}:{region.label}", fill="red")

        # Add region data to page data
        page_data.regions.append(
            PageRegion(
                image_path=str(region_path.relative_to(output_dir)),
                label=region.label,
                bbox_normalized=region.bbox_2d,
                bbox_pixels=pixel_coords,
                description=region.description
            )
        )

    # Save visualization asynchronously
    await loop.run_in_executor(None, viz_image.save, page_dir / "visualization.png")
    
    return page_data


async def process_page(
    doc: pymupdf.Document,
    page_num: int,
    api_key: str,
    output_dir: Path,
    semaphore: Semaphore
) -> Optional[PageData]:
    """Process a single page asynchronously"""
    try:
        print(f"\nProcessing page {page_num + 1}")
        
        # Convert page to image
        loop = asyncio.get_running_loop()
        page_image = await loop.run_in_executor(None, convert_page_to_image, doc, page_num)
        
        # Detect regions with retry logic
        regions = await detect_content_regions_with_retry(page_image, api_key, semaphore=semaphore)
        print(f"  Found {len(regions)} content regions")
        
        # Extract and save regions
        page_data = await extract_and_save_regions(
            page_image,
            regions,
            output_dir,
            page_num,
            api_key,
            semaphore
        )
        print(f"  Saved to {output_dir}/page_{page_num:03d}/")
        
        return page_data
    except Exception as e:
        print(f"Error processing page {page_num + 1}: {str(e)}")
        return None


async def process_document(pdf_path: str, api_key: str, max_concurrent: int = 3) -> DocumentData:
    """Process a single document with concurrent page processing"""
    dl_id = pdf_path.split('/')[-1].split('.')[0]
    output_dir = Path("transform/images/" + dl_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check for existing progress
    status = load_or_create_status(output_dir)
    document_data = None

    # Try to load existing document data
    json_path = output_dir / "document_regions.json"
    if json_path.exists():
        async with aiofiles.open(json_path) as f:
            content = await f.read()
            document_data = DocumentData(**json.loads(content))
    else:
        document_data = DocumentData(dl_id=dl_id, pages=[])

    # Open the PDF document
    doc: pymupdf.Document = pymupdf.open(pdf_path)
    try:
        num_pages = doc.page_count
        start_page = 0 if not status else status.last_processed_page + 1

        if start_page >= num_pages:
            print(f"No pages to process for {pdf_path}")
            return document_data
        if start_page > 0:
            print(f"Resuming processing from page {start_page + 1}")

        status = ProcessingStatus(
            dl_id=dl_id,
            last_processed_page=start_page - 1,
            total_pages=num_pages,
            completed=False,
            last_updated=datetime.now(),
            failed_pages=[]
        )

        # Create semaphore for concurrent API calls
        semaphore = Semaphore(max_concurrent)
        
        # Process pages concurrently in batches
        current_page = start_page
        while current_page < num_pages:
            batch_size = min(max_concurrent, num_pages - current_page)
            batch_pages = range(current_page, current_page + batch_size)

            # Skip already processed pages
            tasks = [
                process_page(doc, page_num, api_key, output_dir, semaphore)
                for page_num in batch_pages
                if not any(p.page_number == page_num for p in document_data.pages)
            ]

            if not tasks:
                current_page += batch_size
                continue

            # Wait for all pages in batch to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results and update status
            for page_num, result in zip(batch_pages, results):
                if isinstance(result, Exception):
                    print(f"Error processing page {page_num + 1}: {str(result)}")
                    status.failed_pages.append(page_num)
                elif result is not None:
                    document_data.pages.append(result)
                    status.last_processed_page = page_num

            # Update status and save progress
            status.last_updated = datetime.now()
            await save_status_async(status, output_dir)
            await save_document_data_async(document_data, json_path)
            
            current_page += batch_size

        # Mark as completed if no failed pages
        status.completed = len(status.failed_pages) == 0
        status.last_updated = datetime.now()
        await save_status_async(status, output_dir)
        
        return document_data

    finally:
        doc.close()


async def main_async():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY not found in environment variables")
        sys.exit(1)

    # Set up paths
    pdf_paths = get_pdf_paths()
    for pdf_path in pdf_paths:
        try:
            await process_document(pdf_path, api_key)
        except Exception as e:
            print(f"Error processing document {pdf_path}: {str(e)}")
            continue

def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()