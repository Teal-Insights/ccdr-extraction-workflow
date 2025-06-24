import json
import time
import random
import os
from playwright.sync_api import sync_playwright
from typing import Dict, Any, Optional

def scrape_publication_details(url: str) -> Optional[Dict[str, Any]]:
    """
    Scrapes the details for a single publication page.

    Args:
        url: The URL of the publication's detail page.

    Returns:
        A dictionary containing all scraped metadata (title, abstract, authors,
        citation, uri, downloadLinks, etc.). Returns None if scraping fails.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-dev-shm-usage'
            ]
        )
        
        # Create context with more realistic browser settings
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0"
            }
        )
        
        page = context.new_page()
        
        # Hide automation indicators
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Remove automation indicators
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        """)
        
        try:
            print(f"Navigating to: {url}")
            response = page.goto(url, wait_until="domcontentloaded")
            
            # Check if we got rate limited
            if response and response.status == 429:
                print(f"Rate limited (429 status).")
                return None
            
            # Wait for the page to load
            page.wait_for_load_state("networkidle")
            
            # Wait for page content to load
            print("Waiting for page content to load...")
            try:
                page.wait_for_selector('h2, h5, a[href*="/bitstreams/"]', timeout=8000)
                print("Page content loaded")
            except Exception as e:
                print(f"Content loading timeout (continuing anyway): {e}")
            
            # Check page title for common error indicators
            title = page.title()
            print(f"Page title: {title}")
            
            if "429" in title or "too many requests" in title.lower():
                print(f"Detected rate limiting in title.")
                return None
            
            # Use Python Playwright API to extract all required information
            print("Extracting publication details...")
            publication_data = {}
            
            # Get the title
            try:
                title_element = page.locator('h2').first
                if title_element.count() > 0:
                    title_text = title_element.inner_text().strip()
                    # Remove "Publication:" prefix if present
                    if title_text.startswith('Publication:'):
                        title_text = title_text.replace('Publication:', '').strip()
                    publication_data['title'] = title_text
                else:
                    publication_data['title'] = None
                    print("Warning: No title found")
            except Exception as e:
                print(f"Error extracting title: {e}")
                publication_data['title'] = None
            
            # Helper function to get field value based on h5 heading
            def get_field_value(label: str) -> Optional[str]:
                try:
                    # Find h5 heading with the label
                    headings = page.locator('h5')
                    heading_count = headings.count()
                    
                    for i in range(heading_count):
                        heading = headings.nth(i)
                        if heading.inner_text().strip() == label:
                            # Found the heading, now find the next sibling with content
                            try:
                                # Try to find the next element with content
                                parent = heading.locator('xpath=..')
                                following_elements = parent.locator(f'xpath=.//*[normalize-space(text()) != ""][position() > count(h5[normalize-space(text()) = "{label}"]/preceding-sibling::*) + 1]')
                                
                                if following_elements.count() > 0:
                                    content = following_elements.first.inner_text().strip()
                                    return content if content else None
                                
                                # Fallback: try next sibling elements
                                next_sibling = heading.locator('xpath=following-sibling::*[1]')
                                if next_sibling.count() > 0:
                                    content = next_sibling.inner_text().strip()
                                    return content if content else None
                                    
                            except Exception:
                                pass
                    return None
                except Exception as e:
                    print(f"Error getting field value for {label}: {e}")
                    return None
            
            # Extract field values
            publication_data['abstract'] = get_field_value('Abstract')
            publication_data['citation'] = get_field_value('Citation')
            
            # Get URI - special handling to extract the link
            try:
                uri = None
                headings = page.locator('h5')
                heading_count = headings.count()
                
                for i in range(heading_count):
                    heading = headings.nth(i)
                    if heading.inner_text().strip() == 'URI':
                        # Find the link in the following content
                        try:
                            parent = heading.locator('xpath=..')
                            uri_links = parent.locator('a')
                            uri_link_count = uri_links.count()
                            
                            for j in range(uri_link_count):
                                link = uri_links.nth(j)
                                href = link.get_attribute('href')
                                if href and ('hdl.handle.net' in href or 'doi.org' in href):
                                    uri = href
                                    break
                        except Exception:
                            pass
                        break
                
                publication_data['uri'] = uri
            except Exception as e:
                print(f"Error extracting URI: {e}")
                publication_data['uri'] = None
            
            # Get download links
            try:
                download_links = []
                bitstream_links = page.locator('a[href*="/bitstreams/"]')
                link_count = bitstream_links.count()
                
                for i in range(link_count):
                    link = bitstream_links.nth(i)
                    try:
                        url_attr = link.get_attribute('href')
                        if not url_attr:
                            continue  # Skip links without URLs
                        url = url_attr  # Now url is guaranteed to be str
                        
                        text = link.inner_text().strip()
                        if not text:
                            continue  # Skip links without text
                            
                        # At this point, both url and text are guaranteed to be non-None strings
                        download_links.append({
                            'url': str(url),  # Type assertion for linter
                            'text': text
                        })
                    except Exception:
                        continue
                
                publication_data['downloadLinks'] = download_links
            except Exception as e:
                print(f"Error extracting download links: {e}")
                publication_data['downloadLinks'] = []
            
            # Get additional metadata
            metadata: Dict[str, Optional[str]] = {
                'date': get_field_value('Date'),
                'published': get_field_value('Published'),
                'authors': get_field_value('Author(s)')
            }
            publication_data['metadata'] = metadata
            
            # Add source URL to the data
            publication_data["source_url"] = url
            
            print(f"Extracted details for: {publication_data.get('title', 'Unknown Title')}")
            
            return publication_data
            
        except Exception as e:
            print(f"Error during scraping of {url}: {str(e)}")
            return None
        finally:
            browser.close()

# Legacy functions for backward compatibility
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
    Legacy function for backward compatibility.
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
    
    # Wait for page content to load  
    print("Waiting for page content to load...")
    try:
        page.wait_for_selector('h2, h5, a[href*="/bitstreams/"]', timeout=8000)
        print("Page content loaded")
    except Exception as e:
        print(f"Content loading timeout (continuing anyway): {e}")
    
    # Check page title for common error indicators
    title = page.title()
    print(f"Page title: {title}")
    
    if "429" in title or "too many requests" in title.lower():
        print(f"Detected rate limiting in title.")
        return None
    
    # Use Python Playwright API to extract all required information
    print("Extracting publication details...")
    publication_data = {}
    
    # Get the title
    try:
        title_element = page.locator('h2').first
        if title_element.count() > 0:
            title_text = title_element.inner_text().strip()
            # Remove "Publication:" prefix if present
            if title_text.startswith('Publication:'):
                title_text = title_text.replace('Publication:', '').strip()
            publication_data['title'] = title_text
        else:
            publication_data['title'] = None
            print("Warning: No title found")
    except Exception as e:
        print(f"Error extracting title: {e}")
        publication_data['title'] = None
    
    # Helper function to get field value based on h5 heading
    def get_field_value(label: str) -> Optional[str]:
        try:
            # Find h5 heading with the label
            headings = page.locator('h5')
            heading_count = headings.count()
            
            for i in range(heading_count):
                heading = headings.nth(i)
                if heading.inner_text().strip() == label:
                    # Found the heading, now find the next sibling with content
                    try:
                        # Try to find the next element with content
                        parent = heading.locator('xpath=..')
                        following_elements = parent.locator(f'xpath=.//*[normalize-space(text()) != ""][position() > count(h5[normalize-space(text()) = "{label}"]/preceding-sibling::*) + 1]')
                        
                        if following_elements.count() > 0:
                            content = following_elements.first.inner_text().strip()
                            return content if content else None
                        
                        # Fallback: try next sibling elements
                        next_sibling = heading.locator('xpath=following-sibling::*[1]')
                        if next_sibling.count() > 0:
                            content = next_sibling.inner_text().strip()
                            return content if content else None
                            
                    except Exception:
                        pass
            return None
        except Exception as e:
            print(f"Error getting field value for {label}: {e}")
            return None
    
    # Extract field values
    publication_data['abstract'] = get_field_value('Abstract')
    publication_data['citation'] = get_field_value('Citation')
    
    # Get URI - special handling to extract the link
    try:
        uri = None
        headings = page.locator('h5')
        heading_count = headings.count()
        
        for i in range(heading_count):
            heading = headings.nth(i)
            if heading.inner_text().strip() == 'URI':
                # Find the link in the following content
                try:
                    parent = heading.locator('xpath=..')
                    uri_links = parent.locator('a')
                    uri_link_count = uri_links.count()
                    
                    for j in range(uri_link_count):
                        link = uri_links.nth(j)
                        href = link.get_attribute('href')
                        if href and ('hdl.handle.net' in href or 'doi.org' in href):
                            uri = href
                            break
                except Exception:
                    pass
                break
        
        publication_data['uri'] = uri
    except Exception as e:
        print(f"Error extracting URI: {e}")
        publication_data['uri'] = None
    
    # Get download links
    try:
        download_links = []
        bitstream_links = page.locator('a[href*="/bitstreams/"]')
        link_count = bitstream_links.count()
        
        for i in range(link_count):
            link = bitstream_links.nth(i)
            try:
                url_attr = link.get_attribute('href')
                if not url_attr:
                    continue  # Skip links without URLs
                link_url = url_attr  # Now link_url is guaranteed to be str
                
                text = link.inner_text().strip()
                if not text:
                    continue  # Skip links without text
                    
                # At this point, both link_url and text are guaranteed to be non-None strings
                download_links.append({
                    'url': link_url,
                    'text': text
                })
            except Exception:
                continue
        
        publication_data['downloadLinks'] = download_links
    except Exception as e:
        print(f"Error extracting download links: {e}")
        publication_data['downloadLinks'] = []
    
    # Get additional metadata
    metadata: Dict[str, Optional[str]] = {
        'date': get_field_value('Date'),
        'published': get_field_value('Published'),
        'authors': get_field_value('Author(s)')
    }
    publication_data['metadata'] = metadata
    
    # Add source URL to the data
    publication_data["source_url"] = url
    
    print(f"Extracted details for: {publication_data.get('title', 'Unknown Title')}")
    
    return publication_data

def scrape_all_publications(input_file, output_file, max_retries=3):
    """
    Legacy function for backward compatibility.
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
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0"
            }
        )
        
        page = context.new_page()
        
        # Hide automation indicators
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Remove automation indicators
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        """)
        
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
    input_file = "extract/data/publication_links.json"
    output_file = "extract/data/publication_details.json"
    
    # First verify if we already have valid results
    if not verify_publication_details(output_file):
        success = scrape_all_publications(input_file, output_file)
        print(f"Publication details extraction {'successful' if success else 'failed'}")
    else:
        print("Using existing valid publication details. Skipping scraping.") 