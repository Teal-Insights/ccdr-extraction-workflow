import os
import sys
from pathlib import Path
import pymupdf
from litellm import completion
import base64
from PIL import Image, ImageDraw
from dotenv import load_dotenv
import io
from pydantic import BaseModel
from typing import List

class BoundingBox(BaseModel):
    bbox_2d: List[int]
    label: str

class ImageRegions(BaseModel):
    regions: List[BoundingBox]

def convert_page_to_image(pdf_path: str, page_num: int, dpi: int = 300) -> Image.Image:
    """Convert a PDF page to a PIL Image with specified DPI."""
    doc = pymupdf.open(pdf_path)
    page = doc[page_num]
    pix = page.get_pixmap(dpi=dpi)

    # Convert PyMuPDF pixmap to PIL Image
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return img

def detect_content_regions(image: Image.Image, api_key: str) -> list:
    """Use Qwen to detect contentful regions in the image."""
    # Set environment variable for OpenRouter
    os.environ["OPENROUTER_API_KEY"] = api_key

    prompt = f"""Analyze this {image.width}x{image.height} image and identify all charts, diagrams, illustrations, or other contentful images.
    For each image, provide the bounding box coordinates and label in the following format:
    {{
        "regions": [
            {{"bbox_2d": [x1, y1, x2, y2], "label": "chart"}},
            {{"bbox_2d": [x1, y1, x2, y2], "label": "diagram"}},
            ...
        ]
    }}
    Use actual pixel coordinates. Do not include decorative elements or non-contentful images."""

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

        # Make API call with error handling
        try:
            response = completion(
                model="openrouter/qwen/qwen2.5-vl-72b-instruct",
                messages=messages,
                response_format=ImageRegions
            )

            # Parse the response
            print("Raw Qwen response:")
            print(response.choices[0].message.content)
            
            # Extract JSON content between first and last curly braces
            content = response.choices[0].message.content
            start = content.find('{')
            end = content.rfind('}') + 1
            if start >= 0 and end > start:
                content = content[start:end]
            
            # Parse JSON response into Pydantic model
            regions_data = ImageRegions.model_validate_json(content)
            # Extract just the bbox coordinates for compatibility with existing code
            return [region.bbox_2d for region in regions_data.regions]

        except Exception as e:
            print(f"Error during Qwen API call or response parsing: {str(e)}")
            if hasattr(e, 'response'):
                print(f"API Response: {e.response}")
            return []

    except Exception as e:
        print(f"Error converting image to bytes: {str(e)}")
        return []

def extract_and_save_regions(image: Image.Image, coords: list, output_dir: Path):
    """Extract and save each detected region as a separate image."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create a copy for visualization
    viz_image = image.copy()
    draw = ImageDraw.Draw(viz_image)

    for i, (xmin, ymin, xmax, ymax) in enumerate(coords):
        # Extract region
        region = image.crop((xmin, ymin, xmax, ymax))
        region.save(output_dir / f"region_{i}.png")

        # Draw rectangle on visualization image
        draw.rectangle([xmin, ymin, xmax, ymax], outline="red", width=3)

    # Save visualization
    viz_image.save(output_dir / "visualization.png")

def main():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("Error: OPENROUTER_API_KEY not found in environment variables")
        sys.exit(1)

    # Set up paths
    pdf_path = "discovery/data/pub_001/dl_001.pdf"
    output_dir = Path("artifacts/extracted_images")
    page_num = 29  # 0-based index for page 30

    # Convert PDF page to image
    print(f"Converting page {page_num + 1} to image...")
    page_image = convert_page_to_image(pdf_path, page_num)

    # Detect content regions
    print("Detecting content regions...")
    pixel_coords = detect_content_regions(page_image, api_key)

    # Extract and save regions
    print("Extracting and saving regions...")
    extract_and_save_regions(page_image, pixel_coords, output_dir)
    print(f"Done! Results saved to {output_dir}")

if __name__ == "__main__":
    main()
