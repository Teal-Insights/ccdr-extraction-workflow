import os
import sys
from pathlib import Path
import pymupdf
import google.generativeai as genai
from PIL import Image, ImageDraw
from dotenv import load_dotenv
import io

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
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash-001')
    
    prompt = """Analyze this image and identify all charts, diagrams, illustrations, or other contentful images. 
    For each one, return its bounding box coordinates in [ymin, xmin, ymax, xmax] format (normalized to 0-1000).
    Only return the coordinates in a list format like this: [[y1,x1,y2,x2], [y1,x1,y2,x2], ...].
    Do not include decorative elements or non-contentful images."""
    
    try:
        # Convert PIL Image to bytes for Gemini
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='PNG')
        img_byte_arr = img_byte_arr.getvalue()
        
        # Make API call with error handling
        try:
            response = model.generate_content([prompt, img_byte_arr])
            
            # Parse the response to extract coordinates
            coords_str = response.text.strip()
            coords = eval(coords_str)  # Convert string representation to actual list
            return coords
            
        except Exception as e:
            print(f"Error during Gemini API call or response parsing: {str(e)}")
            if hasattr(e, 'response'):
                print(f"API Response: {e.response}")
            return []
            
    except Exception as e:
        print(f"Error converting image to bytes: {str(e)}")
        return []

def normalize_coordinates(coords: list, image_width: int, image_height: int) -> list:
    """Convert normalized coordinates (0-1000) to pixel coordinates."""
    normalized = []
    for box in coords:
        ymin, xmin, ymax, xmax = box
        normalized.append([
            int((ymin / 1000.0) * image_height),
            int((xmin / 1000.0) * image_width),
            int((ymax / 1000.0) * image_height),
            int((xmax / 1000.0) * image_width)
        ])
    return normalized

def extract_and_save_regions(image: Image.Image, coords: list, output_dir: Path):
    """Extract and save each detected region as a separate image."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create a copy for visualization
    viz_image = image.copy()
    draw = ImageDraw.Draw(viz_image)
    
    for i, (ymin, xmin, ymax, xmax) in enumerate(coords):
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
    normalized_coords = detect_content_regions(page_image, api_key)
    
    # Convert coordinates to pixel values
    pixel_coords = normalize_coordinates(
        normalized_coords, 
        page_image.width, 
        page_image.height
    )
    
    # Extract and save regions
    print("Extracting and saving regions...")
    extract_and_save_regions(page_image, pixel_coords, output_dir)
    print(f"Done! Results saved to {output_dir}")

if __name__ == "__main__":
    main()
