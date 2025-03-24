#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "litellm",
#     "pydantic",
#     "tenacity",
#     "pypdf",
#     "python-dotenv"
# ]
# ///

from logging import getLogger
from dotenv import load_dotenv
import os
import json
import asyncio
from typing import Optional, Type
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential
import pypdf
from litellm import acompletion
import argparse
import logging

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
async def ingest_pdf_to_markdown(pdf_path: str) -> str:
    """
    Reads a PDF file, extracts text page by page, sends it to the LLM for
    cleaning/formatting in Markdown, and concatenates the result with HTML comments.
    """
    logger.info(f"Starting PDF ingestion for file: {pdf_path}")
    
    # Read the PDF
    reader = pypdf.PdfReader(pdf_path)
    logger.info(f"PDF loaded successfully. Total pages: {len(reader.pages)}")
    
    async def process_page(page_num: int, page) -> tuple[int, str]:
        """Process a single page and return its index and formatted markdown"""
        logger.info(f"Processing page {page_num + 1}/{len(reader.pages)}")
        text_content = page.extract_text() or ""

        prompt = f"""
You are a helpful assistant. Take the following PDF page text and return a JSON with two fields:
1. 'markdown': the text formatted nicely in Markdown
2. 'doc_page_number': page number if visible at the top or bottom of the page; otherwise null

JSON schema:
{LLMPageOutput.model_json_schema()}

PDF page content:
{text_content}
"""

        result = await classify_text(prompt, LLMPageOutput)
        if result is None:
            logger.warning(f"Failed to process page {page_num + 1}. Skipping.")
            return page_num, ""

        logger.debug(f"Successfully processed page {page_num + 1}")

        doc_page_number = result.doc_page_number if result.doc_page_number else "NA"
        page_header = f"<!-- PDF page {page_num + 1} --><!-- Document page {doc_page_number} -->"
        return page_num, f"{page_header}\n{result.markdown.strip()}"

    # Process pages concurrently in batches
    batch_size = 10  # Adjust this number based on API limits and performance
    all_pages_markdown = [""] * len(reader.pages)  # Pre-allocate list
    
    for i in range(0, len(reader.pages), batch_size):
        batch_tasks = [
            process_page(i + idx, page) 
            for idx, page in enumerate(reader.pages[i:i + batch_size])
        ]
        batch_results = await asyncio.gather(*batch_tasks)
        
        # Store results in the correct order
        for idx, content in batch_results:
            if content:  # Only store non-empty results
                all_pages_markdown[idx] = content

    logger.info("PDF processing completed. Combining pages.")
    # Filter out empty strings and join
    final_markdown = "\n\n".join(page for page in all_pages_markdown if page)
    return final_markdown

############################################
# Example usage
############################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert PDF to formatted Markdown')
    parser.add_argument('pdf_path', help='Path to the PDF file to convert')
    parser.add_argument('--output', '-o', default='output.md',
                       help='Output markdown file path (default: output.md)')
    args = parser.parse_args()

    async def main():
        logger.info("Starting PDF to Markdown conversion")
        cleaned_markdown = await ingest_pdf_to_markdown(args.pdf_path)
        
        # Save or do further processing
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(cleaned_markdown)
        logger.info(f"Markdown successfully saved to {args.output}")

    asyncio.run(main())

