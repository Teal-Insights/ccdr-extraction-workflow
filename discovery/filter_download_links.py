#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "litellm",
#     "pydantic",
#     "tenacity",
#     "python-dotenv"
# ]
# ///

from typing import Optional, Type, Literal, Dict, Any, List
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential
import json
import os
from litellm import acompletion
from logging import getLogger
import asyncio
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logger = getLogger(__name__)

############################################
# Pydantic model(s) for the LLM output
############################################
class LinkClassification(BaseModel):
    """Classification for a single link."""
    to_download: bool
    type: Literal["main", "supplemental", "other"]

class LLMOutput(BaseModel):
    """A Pydantic model that represents the JSON structure expected from the LLM."""
    results: Optional[List[LinkClassification]] = Field(None, description="Array of link classifications")

    def get_classifications(self) -> List[LinkClassification]:
        """Get the classifications."""
        return self.results or []

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

def needs_classification(link: Dict[str, Any]) -> bool:
    """Check if a link needs classification."""
    return (
        "to_download" not in link 
        or link["to_download"] is None 
        or "type" not in link 
        or link["type"] is None
    )

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
            messages=[{
                "role": "system",
                "content": (
                    "You are a helpful assistant that classifies document links. "
                    "Return your response as a JSON object with a 'results' array containing "
                    "classifications for each link."
                )
            }, {
                "role": "user",
                "content": prompt
            }],
            response_format={"type": "json_object"},
            api_key=os.getenv('DEEPSEEK_API_KEY')
        )
        logger.debug("Received response from LLM")
        content = response['choices'][0]['message']['content']
        return parse_llm_json_response(content, output_format)
    except Exception as e:
        logger.error(f"Error during classification: {str(e)}")
        if "rate limit" in str(e).lower():
            logger.warning("Rate limit hit - waiting before retry...")
        return None

############################################
# Main link classification logic
############################################
async def classify_link_array(link_array: list[Dict[str, Any]], pub_title: str) -> list[Dict[str, Any]]:
    """
    Takes an array of download links and returns the same array with classification
    fields added to each link.
    """
    # Filter out links that already have valid classifications
    links_to_classify = [link for link in link_array if needs_classification(link)]
    
    if not links_to_classify:
        logger.info(f"Skipping classification for {pub_title} - all {len(link_array)} links already classified")
        return link_array
    
    logger.info(f"Starting classification of {len(links_to_classify)} links for publication: {pub_title}")
    
    # Extract just the URLs and text descriptions for the LLM
    link_info = [{"url": link["url"], "text": link["text"]} for link in links_to_classify]
    
    # Log the links we're about to classify
    for i, link in enumerate(link_info, 1):
        logger.debug(f"Link {i}/{len(link_info)}: {link['text']} ({link['url']})")
    
    prompt = (
        "For each link in the following array, classify it with two fields:\n"
        "1. 'to_download': boolean indicating whether to download the link\n"
        "2. 'type': one of 'main', 'supplemental', or 'other'\n"
        "The 'main' type is for the main report, while 'supplemental' is for "
        "other documents or attachments that are associated with the main "
        "report, but are not the main report itself, such as appendices, "
        "annexes, etc. The 'other' type is provided in case any link does not "
        "fit under this rubric and should be reviewed manually.\n"
        "In general, we should download the most easily parseable version "
        "of any given document. Plain text is more easily parseable than HTML, "
        "which is in turn more easily parseable than PDF. If a document is "
        "available in multiple formats, set the to_download field to true for "
        "the most parseable version and false for less parseable duplicates.\n"
        "For all items without duplicates, set the to_download field to true.\n"
        "Return your response as a JSON object with a 'results' array "
        "containing the classifications.\n\n"
        "Links to classify:\n"
        f"{json.dumps(link_info, indent=2)}\n"
    )
    
    try:
        result = await classify_text(prompt, LLMOutput)
        if result:
            # Get classifications
            classifications = result.get_classifications()
            
            # If we got the right number of classifications, apply them
            if len(classifications) == len(links_to_classify):
                logger.info(f"Successfully classified all {len(links_to_classify)} links")
                for i, (link, classification) in enumerate(zip(links_to_classify, classifications), 1):
                    link["to_download"] = classification.to_download
                    link["type"] = classification.type
                    logger.info(f"Link {i}/{len(links_to_classify)}: type={classification.type}, to_download={classification.to_download}")
            else:
                logger.error(f"Classification mismatch: got {len(classifications)} classifications for {len(links_to_classify)} links")
                # If we got the wrong number, set null values
                for link in links_to_classify:
                    link["to_download"] = None
                    link["type"] = None
        else:
            # If classification failed, add null values
            logger.error("Classification failed - setting null values")
            for link in links_to_classify:
                link["to_download"] = None
                link["type"] = None
    except Exception as e:
        logger.error(f"Error classifying links: {str(e)}")
        # On error, add null values
        for link in links_to_classify:
            link["to_download"] = None
            link["type"] = None
    
    return link_array

async def process_publication_details(input_path: str) -> None:
    """
    Process the publication details JSON file, classifying all download links
    and writing the results back to the file.
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Read the input JSON
    logger.info(f"Reading publication details from {input_path}")
    with open(input_path, 'r', encoding='utf-8') as f:
        publications = json.load(f)
    
    total_pubs = len(publications)
    logger.info(f"Found {total_pubs} publications to process")
    
    # Track statistics
    pubs_processed = 0
    pubs_skipped = 0
    
    # Process each publication's download links
    for i, pub in enumerate(publications, 1):
        title = pub.get('title', 'Untitled')
        if "downloadLinks" not in pub or not pub["downloadLinks"]:
            logger.warning(f"Skipping publication {i}/{total_pubs}: {title} - no download links found")
            pubs_skipped += 1
            continue
            
        # Check if any links need classification
        if not any(needs_classification(link) for link in pub["downloadLinks"]):
            logger.info(f"Skipping publication {i}/{total_pubs}: {title} - all links already classified")
            pubs_skipped += 1
            continue
            
        num_links = len(pub["downloadLinks"])
        logger.info(f"Processing publication {i}/{total_pubs}: {title} ({num_links} links)")
        pub["downloadLinks"] = await classify_link_array(pub["downloadLinks"], title)
        pubs_processed += 1
    
    # Write the updated JSON back to the file
    logger.info(f"Writing updated publication details to {input_path}")
    with open(input_path, 'w', encoding='utf-8') as f:
        json.dump(publications, f, indent=2, ensure_ascii=False)
    logger.info(f"Successfully processed {pubs_processed} publications (skipped {pubs_skipped})")

async def main():
    """Main entry point for the script."""
    logger.info("Starting download link classification")
    
    # Check for API key
    if not os.getenv('DEEPSEEK_API_KEY'):
        logger.error("DEEPSEEK_API_KEY environment variable not set")
        return
    
    input_path = "discovery/data/publication_details.json"
    await process_publication_details(input_path)
    
    logger.info("Download link classification complete")

if __name__ == "__main__":
    asyncio.run(main())
