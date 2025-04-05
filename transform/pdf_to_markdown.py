import os
import json
import asyncio
import logging
from typing import Optional, Type, List
from dotenv import load_dotenv
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential
from litellm import acompletion


logger = logging.getLogger(__name__)
load_dotenv(override=True)

# Add logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

############################################
# Pydantic models for data structures
############################################
class TextPage(BaseModel):
    page_number: int
    text_content: str
    markdown_content: Optional[str] = None

class Document(BaseModel):
    doc_id: str
    pages: List[TextPage]

class Publication(BaseModel):
    pub_id: str
    documents: List[Document]

class LLMPageOutput(BaseModel):
    """A Pydantic model that represents the JSON structure expected from the LLM."""
    markdown: str

############################################
# Utility functions for parsing JSON
############################################
def extract_json_from_markdown(content: str) -> str:
    """Extract JSON content from markdown code fence if present."""
    if '```json' in content:
        return content.split('```json')[1].split('```')[0].strip()
    return content.strip().strip('"\'')

def parse_llm_json_response(content: str, model_class: Type[BaseModel]) -> BaseModel:
    """Parse JSON from LLM response, handling both direct JSON and markdown-fenced output."""
    try:
        return model_class.model_validate(json.loads(content))
    except json.JSONDecodeError:
        json_str = extract_json_from_markdown(content)
        return model_class.model_validate(json.loads(json_str))

############################################
# LLM call with retry
############################################
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def process_text(text_content: str) -> Optional[str]:
    """Process text content using the LLM with retry logic."""
    try:
        prompt = f"""
You are a helpful assistant. Take the following text and return a JSON with one field:
'markdown': the text formatted nicely in Markdown, preserving structure like paragraphs, lists, and headings where possible.
Try to clean up artifacts like spurious line breaks within sentences.

JSON schema:
{LLMPageOutput.model_json_schema()}

Text content:
```text
{text_content}
```
"""
        logger.debug("Sending request to LLM")
        response = await acompletion(
            model="deepseek/deepseek-chat", 
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            api_key=os.getenv('DEEPSEEK_API_KEY')
        )
        logger.debug("Received response from LLM")
        content = response['choices'][0]['message']['content']
        result = parse_llm_json_response(content, LLMPageOutput)
        return result.markdown.strip()
    except Exception as e:
        logger.error(f"Error during text processing: {str(e)}")
        return None

############################################
# Main text processing logic
############################################
async def process_page(page: TextPage) -> None:
    """Process a single page's text content."""
    logger.info(f"Processing page {page.page_number}")
    
    if len(page.text_content.strip()) < 10:
        logger.info(f"Skipping page {page.page_number} due to minimal content")
        page.markdown_content = page.text_content
        return

    markdown_content = await process_text(page.text_content)
    if markdown_content:
        page.markdown_content = markdown_content
    else:
        logger.warning(f"Failed to process page {page.page_number}. Using raw text as fallback.")
        page.markdown_content = page.text_content

async def process_document(doc: Document) -> None:
    """Process all pages in a document."""
    logger.info(f"Processing document {doc.doc_id}")
    batch_size = 5
    for i in range(0, len(doc.pages), batch_size):
        batch = doc.pages[i:i + batch_size]
        await asyncio.gather(*[process_page(page) for page in batch])

async def process_publication(pub: Publication) -> None:
    """Process all documents in a publication."""
    logger.info(f"Processing publication {pub.pub_id}")
    for doc in pub.documents:
        await process_document(doc)

async def process_text_content(input_path: str, output_path: str) -> None:
    """Process all publications from input JSON and save results."""
    try:
        # Read input JSON
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Parse into Pydantic models
        publications = [Publication.model_validate(pub) for pub in data]
        
        # Process all publications
        for pub in publications:
            await process_publication(pub)
        
        # Convert back to JSON and save
        output_data = [pub.model_dump() for pub in publications]
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Successfully processed and saved results to {output_path}")
    
    except Exception as e:
        logger.error(f"Error processing text content: {e}", exc_info=True)
        raise

############################################
# Main execution
############################################
if __name__ == "__main__":
    input_file = "transform/text/text_content.json"
    output_file = "transform/text/text_content_processed.json"
    
    async def main():
        logger.info("Starting text content processing")
        await process_text_content(input_file, output_file)

    asyncio.run(main())

