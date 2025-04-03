#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "litellm",
#     "pydantic",
#     "tenacity",
#     "pymupdf",
#     "python-dotenv",
#     "PyYAML"
# ]
# ///

from logging import getLogger
from dotenv import load_dotenv
import os
import json
import asyncio
from typing import Optional, Type, Dict, List, Tuple
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential
import pymupdf
from litellm import acompletion
import argparse
import logging
import yaml
import pathlib  # Added for path manipulation

logger = getLogger(__name__)
load_dotenv(override=True)

# Add logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

############################################
# Pydantic model(s) for the LLM output
############################################
class LLMPageOutput(BaseModel):
    """A Pydantic model that represents the JSON structure expected from the LLM."""
    markdown: str
    doc_page_number: Optional[str | int] = None

############################################
# Utility functions for parsing JSON
############################################
def extract_json_from_markdown(content: str) -> str:
    """Extract JSON content from markdown code fence if present."""
    if '```json' in content:
        # Take whatever is between ```json ... ```
        return content.split('```json')[1].split('```')[0].strip()
    return content.strip().strip('"\'')  # Fallback to raw string if no JSON code fence

def parse_llm_json_response(content: str, model_class: Type[BaseModel]) -> BaseModel:
    """Parse JSON from LLM response, handling both direct JSON and markdown-fenced output."""
    try:
        return model_class.model_validate(json.loads(content))
    except json.JSONDecodeError:
        # If direct parse fails, try to extract from code fences
        json_str = extract_json_from_markdown(content)
        return model_class.model_validate(json.loads(json_str))

############################################
# LLM call with retry
############################################
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def classify_text(prompt: str, output_format: Type[BaseModel]) -> Optional[BaseModel]:
    """Classify a single text using the LLM with retry logic."""
    try:
        logger.debug("Sending request to LLM")
        response = await acompletion(
            model="deepseek/deepseek-chat", 
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            api_key=os.getenv('DEEPSEEK_API_KEY')
        )
        logger.debug("Received response from LLM")
        content = response['choices'][0]['message']['content']
        return parse_llm_json_response(content, output_format)
    except Exception as e:
        logger.error(f"Error during classification: {str(e)}")
        return None

############################################
# Main PDF ingestion logic
############################################
async def process_page(
    doc: pymupdf.Document,
    page_num: int,
    page: pymupdf.Page,
    image_dir_path: pathlib.Path,
    image_output_dir_name: str  # The *relative* name for markdown links
) -> Tuple[int, str]:
    """
    Process a single page: extract text, extract images, send text to LLM,
    combine results, and return page index and formatted markdown.
    """
    logger.info(f"Processing page {page_num + 1}/{len(doc)}")

    # --- 1. Extract Text ---
    text_content = page.get_text("text") or ""

    # --- 2. Extract and Save Images ---
    image_markdown_tags: List[str] = []
    image_list = page.get_images(full=True)
    if image_list:
        logger.info(f"Found {len(image_list)} image(s) on page {page_num + 1}")
        for img_index, img_info in enumerate(image_list):
            xref = img_info[0]
            try:
                base_image = doc.extract_image(xref)
                if not base_image:
                    logger.warning(f"Could not extract image xref {xref} on page {page_num + 1}. Skipping.")
                    continue

                image_bytes = base_image["image"]
                image_ext = base_image["ext"]

                # Generate filename and path
                img_filename = f"page_{page_num + 1}_img_{img_index}.{image_ext}"
                img_save_path = image_dir_path / img_filename
                md_image_path = pathlib.Path(image_output_dir_name) / img_filename  # Relative path for markdown

                # Save the image
                logger.info(f"Saving image to: {img_save_path}")
                with open(img_save_path, "wb") as img_file:
                    img_file.write(image_bytes)

                # Create Markdown tag
                alt_text = f"Image from PDF page {page_num + 1}, index {img_index}"
                # Use forward slashes for Markdown/HTML paths, even on Windows
                markdown_tag = f"![{alt_text}]({md_image_path.as_posix()})"
                image_markdown_tags.append(markdown_tag)

            except Exception as e:
                logger.error(f"Failed to process or save image xref {xref} on page {page_num + 1}: {e}", exc_info=True)

    # --- 3. Process Text with LLM (skip if minimal text) ---
    llm_markdown = ""
    doc_page_number_text = "NA"  # Default if LLM fails or text is skipped

    # Check if extracted text is minimal (likely blank or just header/footer)
    if len(text_content.strip()) < 50 and not image_markdown_tags:  # Skip only if NO images AND minimal text
        logger.info(f"Skipping LLM processing for page {page_num + 1} due to minimal text content and no images.")
        # Keep images if they exist, even if text is minimal
    elif len(text_content.strip()) < 10:  # If text is *really* minimal, don't bother LLM
        logger.info(f"Skipping LLM processing for page {page_num + 1} due to very minimal text content.")
    else:
        prompt = f"""
You are a helpful assistant. Take the following PDF page text and return a JSON with two fields:
1. 'markdown': the text formatted nicely in Markdown, preserving structure like paragraphs, lists, and headings where possible.
Try to clean up artifacts from PDF extraction like spurious line breaks within sentences. Do NOT include any image references, they will be added later.
2. 'doc_page_number': page number if visible as text (e.g., at the top or bottom of the page); otherwise null

JSON schema:
{LLMPageOutput.model_json_schema()}

PDF page content:
```text
{text_content}
```
"""
        result = await classify_text(prompt, LLMPageOutput)
        if result is None or "Error:" in result.markdown:  # Check for parsing errors too
            logger.warning(f"Failed to process text for page {page_num + 1}. Using raw text as fallback.")
            # Fallback: use raw text, try to preserve paragraphs roughly
            llm_markdown = "\n\n".join(p.strip() for p in text_content.split('\n\n') if p.strip())
        else:
            logger.debug(f"Successfully processed text for page {page_num + 1}")
            llm_markdown = result.markdown.strip()
            if result.doc_page_number:
                doc_page_number_text = str(result.doc_page_number)

    # --- 4. Combine Header, LLM Markdown, and Image Tags ---
    page_header = f"<!-- PDF page {page_num + 1} --><!-- Document page {doc_page_number_text} -->"

    # Combine the text markdown and the image tags
    # Place images *after* the text content for simplicity
    final_page_content = llm_markdown
    if image_markdown_tags:
        final_page_content += "\n\n" + "\n".join(image_markdown_tags)  # Add images at the end

    # Only add content if there's text or images
    if final_page_content.strip():
        return page_num, f"{page_header}\n{final_page_content.strip()}"
    else:
        logger.info(f"Page {page_num+1} resulted in no text or image content after processing. Skipping output for this page.")
        return page_num, ""  # Return empty string if page becomes empty

async def ingest_pdf_to_markdown(pdf_path: str, output_md_path: str, image_output_dir_name: str) -> str:
    """
    Reads PDF, extracts text and images, processes text via LLM, saves images,
    and concatenates into a single Markdown string with frontmatter and references.
    """
    logger.info(f"Starting PDF ingestion for file: {pdf_path}")
    logger.info(f"Output Markdown will be: {output_md_path}")
    logger.info(f"Image directory name: {image_output_dir_name}")

    output_md_filepath = pathlib.Path(output_md_path).resolve()
    # Image dir path is relative to the *output markdown file's directory*
    image_dir_path = output_md_filepath.parent / image_output_dir_name
    logger.info(f"Absolute image save path: {image_dir_path}")

    # Create the image directory
    try:
        image_dir_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured image directory exists: {image_dir_path}")
    except OSError as e:
        logger.error(f"Could not create image directory {image_dir_path}: {e}")

    all_pages_markdown_content: List[str] = []  # Use list to store content temporarily
    yaml_frontmatter = ""

    try:
        # Use pymupdf (PyMuPDF) to open the PDF
        with pymupdf.open(pdf_path) as doc:
            logger.info(f"PDF loaded successfully using PyMuPDF. Total pages: {len(doc)}")

            # --- Metadata Extraction ---
            metadata = doc.metadata
            metadata_dict: Dict[str, Optional[str]] = {}
            if metadata:
                metadata_dict["title"] = metadata.get("title")
                metadata_dict["author"] = metadata.get("author")
                raw_creation_date = metadata.get("creationDate")
                if raw_creation_date and raw_creation_date.startswith("D:"):
                    metadata_dict["creation_date"] = raw_creation_date[2:10]  # YYYYMMDD
                else:
                    metadata_dict["creation_date"] = raw_creation_date

                frontmatter_data = {k: v for k, v in metadata_dict.items() if v}
                if frontmatter_data:
                    yaml_frontmatter = "---\n" + yaml.dump(frontmatter_data, indent=2, allow_unicode=True) + "---\n\n"
            else:
                logger.warning("No metadata found in the PDF.")

            # --- Process Pages Concurrently ---
            batch_size = 5  # Reduced batch size due to potential image processing load
            total_pages = len(doc)
            # Pre-allocate results list with None placeholders
            page_results: List[Optional[str]] = [None] * total_pages

            for i in range(0, total_pages, batch_size):
                logger.info(f"Processing batch starting from page {i+1}")
                batch_tasks = [
                    process_page(
                        doc=doc,
                        page_num=page_num,
                        page=doc[page_num],  # Pass the page object
                        image_dir_path=image_dir_path,  # Absolute path for saving
                        image_output_dir_name=image_output_dir_name  # Relative name for linking
                    )
                    for page_num in range(i, min(i + batch_size, total_pages))
                ]
                try:
                    batch_results_tuples: List[Tuple[int, str]] = await asyncio.gather(*batch_tasks)

                    # Store results in the correct order using the returned index
                    for idx, content in batch_results_tuples:
                        if content:  # Only store non-empty results
                            page_results[idx] = content
                        else:
                            # Ensure empty pages are marked as processed (with empty string)
                            page_results[idx] = ""  # Mark as empty string instead of None
                except Exception as batch_error:
                    logger.error(f"Error processing batch starting at page {i+1}: {batch_error}", exc_info=True)

            # Filter out None values (if any somehow remain) and join
            all_pages_markdown_content = [page for page in page_results if page is not None]

    except pymupdf.pymupdf.FileNotFoundError:
        logger.error(f"PDF file not found at {pdf_path}")
        return ""  # Return empty string if PDF doesn't exist
    except Exception as e:
        logger.error(f"Failed to open or process PDF {pdf_path}: {e}", exc_info=True)
        return ""  # Return empty string on failure

    logger.info("PDF processing completed. Combining pages.")
    # Filter out empty strings and join
    final_markdown = "\n\n".join(page for page in all_pages_markdown_content if page.strip())
    return yaml_frontmatter + final_markdown

############################################
# Example usage
############################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert PDF to formatted Markdown with image extraction')
    parser.add_argument('pdf_path', help='Path to the PDF file to convert')
    parser.add_argument('--output', '-o', default='output.md',
                       help='Output markdown file path (default: output.md)')
    parser.add_argument('--images', '-i', default='images',
                       help='Directory name relative to the output file to save extracted images (default: images)')
    # Add log level argument
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set the logging level (default: INFO)')

    args = parser.parse_args()

    # Update logging level based on argument
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    # Get root logger and set level - affects all loggers unless they have specific levels
    logging.getLogger().setLevel(log_level)
    logger.info(f"Logging level set to: {args.log_level.upper()}")

    async def main():
        logger.info("Starting PDF to Markdown conversion with image extraction")
        cleaned_markdown = await ingest_pdf_to_markdown(
            args.pdf_path,
            args.output,
            args.images  # Pass the relative image directory name
        )

        if cleaned_markdown:
            try:
                output_filepath = pathlib.Path(args.output)
                # Ensure the output directory exists
                output_filepath.parent.mkdir(parents=True, exist_ok=True)

                with open(output_filepath, "w", encoding="utf-8") as f:
                    f.write(cleaned_markdown)
                logger.info(f"Markdown successfully saved to {output_filepath}")
            except Exception as e:
                logger.error(f"Failed to write output file {args.output}: {e}")
        else:
            logger.warning("PDF processing resulted in empty content. No output file written.")

    asyncio.run(main())

