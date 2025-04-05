import os
import json
import logging
import asyncio
from typing import List, Optional, Dict
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from litellm import acompletion
from tenacity import retry, stop_after_attempt, wait_exponential


logger = logging.getLogger(__name__)
load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

############################################
# Pydantic models for input text data structures
############################################

class TextPage(BaseModel):
    """Represents a single page from a PDF document."""
    page_number: int = Field(..., description="1-based page number")
    text_content: str = Field(..., description="Extracted text content from the page")

class TextDocument(BaseModel):
    """Represents a document with multiple pages."""
    doc_id: str = Field(..., description="Unique document identifier")
    pages: List[TextPage] = Field(default_factory=list, description="List of pages in the document")

class TextPublication(BaseModel):
    """Represents a publication containing multiple documents."""
    pub_id: str = Field(..., description="Unique publication identifier")
    documents: List[TextDocument] = Field(default_factory=list, description="List of documents in the publication")

############################################
# Pydantic models for output text data structures
############################################

class Heading(BaseModel):
    """Represents a heading in the text."""
    level: int = Field(..., description="Heading level")
    text: str = Field(..., description="Heading text")
    page_number: int = Field(..., description="Page number where the heading occurs")

class Headings(BaseModel):
    """Represents the JSON structure expected from the LLM."""
    headings: List[Heading] = Field(..., description="List of headings")

############################################
# LLM call with retry
############################################
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def process_text(text_document: TextDocument) -> Optional[Dict]:
    """Process text content using the LLM with retry logic."""
    try:
        prompt = f"""
You are a helpful assistant. Take the following document text and return JSON with one field:
'headings': a list of sequential headings in the text, with the level, text, and page number where each heading occurs.
Include the document title as a level 0 heading. Include figure titles as subheadings of the section in which they appear.
Try to clean up artifacts like spurious line breaks within the heading text.

Text content:
```text
{str(text_document.pages)}
```
"""
        logger.debug("Sending request to LLM")
        response = await acompletion(
            model="gemini/gemini-2.0-flash-001", 
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object", "response_schema": Headings.model_json_schema()},
            api_key=os.getenv('GEMINI_API_KEY')
        )
        logger.debug("Received response from LLM")
        content = response['choices'][0]['message']['content']
        result = {text_document.doc_id: Headings.model_validate_json(content)}
        return result
    except Exception as e:
        logger.error(f"Error during text processing: {str(e)}")
        return None

############################################
# Main workflow function
############################################
async def process_documents_concurrently(input_file: str, output_file: str, max_concurrency: int = 5):
    """
    Process multiple documents concurrently and save the headings to a JSON file.
    
    Args:
        input_file: Path to the input JSON file containing text content
        output_file: Path to save the output headings JSON
        max_concurrency: Maximum number of concurrent LLM requests
    """
    logger.info(f"Loading text content from {input_file}")
    
    try:
        with open(input_file, 'r') as f:
            content = json.load(f)
        
        # The content is an array of publications
        publications = []
        for pub_data in content:
            try:
                publication = TextPublication.model_validate(pub_data)
                publications.append(publication)
            except Exception as e:
                logger.error(f"Error parsing publication: {str(e)}")
        
        logger.info(f"Found {len(publications)} publications")
        
        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(max_concurrency)
        
        # Collect all documents from all publications
        all_documents = []
        for pub in publications:
            all_documents.extend(pub.documents)
            
        logger.info(f"Found {len(all_documents)} documents to process across all publications")
        
        async def process_with_semaphore(document):
            async with semaphore:
                logger.info(f"Processing document: {document.doc_id}")
                return await process_text(document)
        
        # Create tasks for all documents
        tasks = [process_with_semaphore(doc) for doc in all_documents]
        
        # Execute all tasks concurrently and gather results
        results = await asyncio.gather(*tasks)
        
        # Filter out None results (failed processing)
        results = [r for r in results if r is not None]
        
        # Combine all results into a single dictionary
        headings_data = {}
        for result in results:
            # Each result is a dictionary with doc_id as key and Headings object as value
            for doc_id, headings_obj in result.items():
                # Convert Headings object to a dictionary
                headings_data[doc_id] = headings_obj.model_dump()
        
        # Save results to output file
        logger.info(f"Saving headings to {output_file}")
        with open(output_file, 'w') as f:
            json.dump(headings_data, f, indent=2)
            
        logger.info(f"Successfully processed {len(headings_data)} documents")
        
    except Exception as e:
        logger.error(f"Error processing documents: {str(e)}")
        raise

if __name__ == "__main__":
    # Define input and output file paths
    input_file = "transform/text/text_content.json"
    output_file = "transform/text/headings.json"
    
    # Run the async function in the event loop
    asyncio.run(process_documents_concurrently(input_file, output_file))
