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
    description: str

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
    """Use Gemini to detect contentful regions in the image."""
    # Set environment variable for LiteLLM
    os.environ["GEMINI_API_KEY"] = api_key

    prompt = """Analyze this image and identify all charts, diagrams, illustrations, or other contentful images.
    For each image, provide the bounding box coordinates (y1,x1,y2,x2) normalized to 0-1000, plus a context-aware description of the image.
    Do not include decorative elements or non-contentful images."""

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
                model="gemini/gemini-2.0-flash-001",
                messages=messages,
                response_format={"type": "json_object", "response_schema": ImageRegions.model_json_schema()}
            )

            # Parse the response
            print("Raw Gemini response:")
            print(response.choices[0].message.content)
            
            # Parse JSON response into Pydantic model
            regions_data = ImageRegions.model_validate_json(response.choices[0].message.content)
            return regions_data.regions

        except Exception as e:
            print(f"Error during Gemini API call or response parsing: {str(e)}")
            if hasattr(e, 'response'):
                print(f"API Response: {e.response}")
            return []

    except Exception as e:
        print(f"Error converting image to bytes: {str(e)}")
        return []

def normalize_coordinates(coords: List[int], image_width: int, image_height: int) -> List[int]:
    """Convert normalized coordinates (0-1000) to pixel coordinates."""
    ymin, xmin, ymax, xmax = coords
    return [
        int((ymin / 1000.0) * image_height),
        int((xmin / 1000.0) * image_width),
        int((ymax / 1000.0) * image_height),
        int((xmax / 1000.0) * image_width)
    ]

def extract_and_save_regions(image: Image.Image, regions: List[BoundingBox], output_dir: Path):
    """Extract and save each detected region as a separate image."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create a copy for visualization
    viz_image = image.copy()
    draw = ImageDraw.Draw(viz_image)

    # Create a markdown file for descriptions
    descriptions_file = output_dir / "region_descriptions.md"
    with open(descriptions_file, "w") as f:
        f.write("# Image Region Descriptions\n\n")

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
        region_filename = f"region_{i}.png"
        region_img.save(output_dir / region_filename)

        # Draw rectangle and add label on visualization image
        draw.rectangle([xmin, ymin, xmax, ymax], outline="red", width=3)
        # Add region number near the top-left corner of the box
        draw.text((xmin + 5, ymin + 5), str(i), fill="red")

        # Write description to markdown file
        with open(descriptions_file, "a") as f:
            f.write(f"## Region {i}\n")
            f.write(f"![Region {i}]({region_filename})\n\n")
            f.write(f"{region.description}\n\n")
            f.write(f"Coordinates (normalized): {region.bbox_2d}\n")
            f.write(f"Coordinates (pixels): {pixel_coords}\n\n")

    # Save visualization
    viz_image.save(output_dir / "visualization.png")

def main():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY not found in environment variables")
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
    regions = detect_content_regions(page_image, api_key)

    # Extract and save regions
    print("Extracting and saving regions...")
    extract_and_save_regions(page_image, regions, output_dir)
    print(f"Done! Results saved to {output_dir}")

if __name__ == "__main__":
    main()
