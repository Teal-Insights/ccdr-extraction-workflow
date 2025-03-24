#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "playwright",
# ]
# ///

import json
import time
import random
import os
from playwright.sync_api import sync_playwright

def verify_publication_details(output_file):
    """
    Verify that we have sufficient and valid publication details in the output file.
    Returns True if verification passes, False otherwise.
    """
    try:
        # Check if file exists
        if not os.path.exists(output_file):
            print("Output file does not exist. Need to run scraping.")
            return False
            
        # Load and parse JSON
        with open(output_file, "r", encoding="utf-8") as f:
            publications = json.load(f)
            
        # Check number of results
        if len(publications) < 61:
            print(f"Insufficient number of publications: {len(publications)} < 61. Need to run scraping.")
            return False
            
        # Check required fields in each publication
        required_fields = ["title", "abstract", "citation", "uri"]
        for idx, pub in enumerate(publications):
            # Check basic required fields
            for field in required_fields:
                if not pub.get(field):
                    print(f"Publication {idx + 1} missing or null {field}. Need to run scraping.")
                    return False
            
            # Check for non-empty downloadLinks with valid fields
            if not pub.get("downloadLinks") or len(pub["downloadLinks"]) == 0:
                print(f"Publication {idx + 1} has no download links. Need to run scraping.")
                return False
            
            # Verify each download link has non-null url and text
            for link_idx, link in enumerate(pub["downloadLinks"]):
                if not link.get("url") or not link.get("text"):
                    print(f"Publication {idx + 1}, download link {link_idx + 1} missing url or text. Need to run scraping.")
                    return False
                    
        print(f"Verification passed: Found {len(publications)} valid publications with all required fields and valid download links.")
        return True
        
    except Exception as e:
        print(f"Error during verification: {str(e)}. Need to run scraping.")
        return False

def extract_publication_details(page, url):
    """
    Extract detailed information from a publication page.
    Returns a dictionary with title, abstract, citation, URI, and download links.
    """
    print(f"Navigating to: {url}")
    response = page.goto(url, wait_until="domcontentloaded")
    
    # Check if we got rate limited
    if response.status == 429:
        print(f"Rate limited (429 status).")
        return None
    
    # Wait for the page to load
    page.wait_for_load_state("networkidle")
    print("Waiting for additional content loading...")
    page.wait_for_timeout(5000)  # Additional 5s wait for any dynamic content
    
    # Check page title for common error indicators
    title = page.title()
    print(f"Page title: {title}")
    
    if "429" in title or "too many requests" in title.lower():
        print(f"Detected rate limiting in title.")
        return None
    
    # Use JavaScript to extract all required information
    publication_data = page.evaluate("""() => {
        // Get the title
        const titleElement = document.querySelector('h2');
        const title = titleElement ? titleElement.textContent.trim().replace('Publication:', '').trim() : null;
        
        // Helper to get field value based on heading
        const getFieldValue = (label) => {
            const heading = Array.from(document.querySelectorAll('h5')).find(h => h.textContent.trim() === label);
            if (!heading) return null;
            
            let sibling = heading.nextElementSibling;
            while (sibling && !sibling.textContent.trim()) {
                sibling = sibling.nextElementSibling;
            }
            return sibling ? sibling.textContent.trim() : null;
        };
        
        // Get abstract - look for content after the Abstract heading
        const abstract = getFieldValue('Abstract');
        
        // Get citation - look for content after Citation heading
        const citation = getFieldValue('Citation');
        
        // Get URI - look for content after URI heading
        let uri = null;
        const uriHeading = Array.from(document.querySelectorAll('h5')).find(h => h.textContent.trim() === 'URI');
        if (uriHeading) {
            // Get the next sibling with URI content and extract the link
            let sibling = uriHeading.nextElementSibling;
            while (sibling && (!sibling.textContent.trim() || !sibling.querySelector('a'))) {
                sibling = sibling.nextElementSibling;
            }
            if (sibling && sibling.querySelector('a')) {
                uri = sibling.querySelector('a').href;
            }
        }
        
        // Get download links
        const downloadLinks = Array.from(document.querySelectorAll('a[href*="/bitstreams/"]'))
            .map(link => ({
                url: link.href,
                text: link.textContent.trim()
            }));
        
        // Get additional metadata
        const date = getFieldValue('Date');
        const published = getFieldValue('Published');
        const authors = getFieldValue('Author(s)');
        
        return {
            title,
            abstract,
            citation,
            uri,
            downloadLinks,
            metadata: {
                date,
                published,
                authors
            }
        };
    }""")
    
    # Add source URL to the data
    publication_data["source_url"] = url
    
    return publication_data

def scrape_all_publications(input_file, output_file, max_retries=3):
    """
    Load publication links from JSON file, scrape details from each page,
    and save to a new JSON file. Implements retry logic for handling rate limits.
    """
    # Create the output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Load publication links from JSON file
    with open(input_file, "r", encoding="utf-8") as f:
        publication_links = json.load(f)
    
    detailed_publications = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        
        # Create context with more realistic browser settings
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        page = context.new_page()
        
        for index, pub in enumerate(publication_links):
            print(f"\nProcessing publication {index + 1} of {len(publication_links)}: {pub['title']}")
            
            for attempt in range(1, max_retries + 1):
                try:
                    # Add delay between requests to avoid rate limiting
                    if index > 0 or attempt > 1:
                        wait_time = random.uniform(5, 10)  # Random wait between 5-10 seconds
                        print(f"Waiting {wait_time:.1f} seconds before next request...")
                        time.sleep(wait_time)
                    
                    # Extract publication details
                    publication_details = extract_publication_details(page, pub["url"])
                    
                    # Check if we were successful or rate limited
                    if publication_details is None:
                        print(f"Failed attempt {attempt}/{max_retries} for publication {index + 1}")
                        
                        if attempt == max_retries:
                            print(f"Maximum retries reached for publication {index + 1}. Moving to next.")
                            break
                            
                        # Wait longer before retry
                        wait_time = random.uniform(15, 30)
                        print(f"Rate limited. Waiting {wait_time:.1f} seconds before retry...")
                        time.sleep(wait_time)
                        continue
                    
                    # Add original metadata from the input file
                    publication_details["original_title"] = pub.get("title")
                    publication_details["source"] = pub.get("source")
                    publication_details["page_found"] = pub.get("page_found")
                    
                    # Add to the list of detailed publications
                    detailed_publications.append(publication_details)
                    
                    print(f"Successfully extracted details for: {publication_details.get('title', 'Unknown Title')}")
                    
                    # Save progress after each successful extraction
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(detailed_publications, f, indent=2, ensure_ascii=False)
                    
                    # Successfully processed this publication, move to next
                    break
                    
                except Exception as e:
                    print(f"Error during processing of publication {index + 1} (attempt {attempt}/{max_retries}): {str(e)}")
                    
                    if attempt == max_retries:
                        print(f"Maximum retries reached for publication {index + 1}. Moving to next.")
                        break
                        
                    # Wait before retry
                    wait_time = random.uniform(10, 20)
                    print(f"Error encountered. Waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
        
        browser.close()
    
    print(f"\nCompleted extraction of {len(detailed_publications)} publications out of {len(publication_links)}")
    
    # Final save with all results
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(detailed_publications, f, indent=2, ensure_ascii=False)
    
    return len(detailed_publications) > 0

if __name__ == "__main__":
    input_file = "discovery/data/publication_links.json"
    output_file = "discovery/data/publication_details.json"
    
    # First verify if we already have valid results
    if not verify_publication_details(output_file):
        success = scrape_all_publications(input_file, output_file)
        print(f"Publication details extraction {'successful' if success else 'failed'}")
    else:
        print("Using existing valid publication details. Skipping scraping.") 