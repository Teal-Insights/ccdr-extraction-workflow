import json
import time
import random
from playwright.sync_api import sync_playwright
import os
from typing import List, Dict

def get_all_publication_links(base_url: str, total_pages: int = 7) -> List[Dict[str, str]]:
    """
    Scrapes all pages of the CCDR collection to get publication links.

    Args:
        base_url: The base URL of the CCDR collection.
        total_pages: The number of pages to scrape.

    Returns:
        A list of dictionaries, where each dict has "title" and "url".
        Example: [{"title": "Publication A", "url": "http://..."}, ...]
    """
    all_links = []
    all_urls = set()  # Track URLs we've already processed
    max_retries = 3
    
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
        
        for page_num in range(1, total_pages + 1):
            for attempt in range(1, max_retries + 1):
                try:
                    # Construct page URL with pagination parameter
                    page_url = f"{base_url}?spc.page={page_num}"
                    print(f"\nProcessing page {page_num} of {total_pages}")
                    
                    # Wait between page requests to avoid rate limiting
                    if page_num > 1 or attempt > 1:
                        wait_time = random.uniform(5, 10)  # Random wait between 5-10 seconds
                        print(f"Waiting {wait_time:.1f} seconds before next request...")
                        time.sleep(wait_time)
                    
                    # Extract publications from this page
                    page_publications = extract_publication_links_from_page(page, page_url)
                    
                    # Check if we were successful or rate limited
                    if page_publications is None:
                        print(f"Failed attempt {attempt}/{max_retries} for page {page_num}")
                        
                        if attempt == max_retries:
                            print(f"Maximum retries reached for page {page_num}. Moving to next page.")
                            break
                            
                        # Wait longer before retry
                        wait_time = random.uniform(15, 30)
                        print(f"Rate limited. Waiting {wait_time:.1f} seconds before retry...")
                        time.sleep(wait_time)
                        continue
                    
                    # Process publications found on this page
                    new_count = 0
                    for pub in page_publications:
                        # Only add if we haven't seen this URL before
                        if pub["url"] not in all_urls:
                            all_urls.add(pub["url"])
                            all_links.append({
                                "title": pub["title"],
                                "url": pub["url"],
                                "source": "World Bank Open Knowledge Repository",
                                "page_found": page_num
                            })
                            new_count += 1
                    
                    print(f"Found {len(page_publications)} publications on page {page_num}, {new_count} are new")
                    print(f"Total unique publications collected so far: {len(all_links)}")
                    
                    # Successfully processed this page, move to next
                    break
                    
                except Exception as e:
                    print(f"Error during processing of page {page_num} (attempt {attempt}/{max_retries}): {str(e)}")
                    
                    if attempt == max_retries:
                        print(f"Maximum retries reached for page {page_num}. Moving to next page.")
                        break
                        
                    # Wait before retry
                    wait_time = random.uniform(10, 20)
                    print(f"Error encountered. Waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
        
        browser.close()
    
    print(f"\nCompleted extraction from {total_pages} pages")
    print(f"Total unique publications collected: {len(all_links)}")
    
    return all_links

def extract_publication_links_from_page(page, url):
    """
    Extract publication links from a single page using Playwright's Python API.
    Returns a list of publication data.
    """
    print(f"Navigating to: {url}")
    response = page.goto(url, wait_until="domcontentloaded")
    
    # Check if we got rate limited
    if response.status == 429:
        print(f"Rate limited (429 status).")
        return None
    
    # Wait for the page to load
    page.wait_for_load_state("networkidle")
    
    # Wait for content to appear - this replaces the hardcoded delays
    print("Waiting for page content to load...")
    try:
        # Wait for publication links or other content indicators
        page.wait_for_selector('a[href*="/publication/"], .pagination, .search-results, .item', timeout=8000)
        print("Page content loaded")
    except Exception as e:
        print(f"Content loading timeout (continuing anyway): {e}")
    
    # Handle cookie consent banner if present
    print("Checking for cookie consent banner...")
    try:
        # Look for common cookie consent elements
        consent_selectors = [
            'button:has-text("Accept")',
            'button:has-text("That\'s ok")', 
            'button:has-text("OK")',
            'button:has-text("Agree")',
            'button:has-text("Continue")',
            '[data-testid="accept-cookies"]',
            '.cookie-accept',
            '#accept-cookies'
        ]
        
        for selector in consent_selectors:
            try:
                consent_button = page.locator(selector).first
                if consent_button.count() > 0:
                    print(f"Found consent button with selector: {selector}")
                    consent_button.click()
                    print("Clicked consent button")
                    
                    # Wait for page to stabilize after consent
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                        print("Page stabilized after consent")
                    except Exception as e:
                        print(f"Timeout waiting for page to stabilize: {e}")
                    
                    break
            except Exception as e:
                print(f"Error with consent selector {selector}: {e}")
                continue
        else:
            print("No consent button found")
            
    except Exception as e:
        print(f"Error handling consent banner: {e}")
    
    # Check page title for common error indicators
    title = page.title()
    print(f"Page title: {title}")
    
    if "429" in title or "too many requests" in title.lower():
        print(f"Detected rate limiting in title.")
        return None
    
    # Use Playwright's Python API to extract publication links
    print("Extracting publication links...")
    
    # Find all links containing '/publication/' in their href
    publication_links = page.locator('a[href*="/publication/"]')
    link_count = publication_links.count()
    print(f"Found {link_count} publication links on page")
    
    if link_count == 0:
        print("No publication links found.")
        return []
    
    # Track unique URLs to avoid duplicates
    link_map = {}
    skip_texts = {'download', 'view', 'pdf', 'read'}
    
    # Process each link
    for i in range(link_count):
        try:
            link = publication_links.nth(i)
            href = link.get_attribute('href')
            
            if not href:
                continue
                
            # Convert relative URLs to absolute
            if href.startswith('/'):
                href = f"https://openknowledge.worldbank.org{href}"
            
            # Initialize entry if URL hasn't been seen
            if href not in link_map:
                link_map[href] = {
                    'url': href,
                    'titles': [],
                }
            
            # Get link text
            try:
                link_text = link.inner_text().strip()
                if link_text and link_text.lower() not in skip_texts:
                    link_map[href]['titles'].append(link_text)
            except Exception:
                pass  # Skip if can't get text
            
            # Try to find title from parent containers
            try:
                # Look for parent elements that might contain the title
                parent_selectors = ['li', '.item', '.publication-item', '.result-item']
                
                for selector in parent_selectors:
                    try:
                        parent = link.locator(f'xpath=ancestor::{selector.replace(".", "")}[1]')
                        if parent.count() > 0:
                            # Try common title selectors within the parent
                            title_selectors = ['h1', 'h2', 'h3', 'h4', 'h5', '.title', '.item-title']
                            for title_selector in title_selectors:
                                try:
                                    title_elem = parent.locator(title_selector).first
                                    if title_elem.count() > 0:
                                        title_text = title_elem.inner_text().strip()
                                        if title_text and len(title_text) > 3:
                                            link_map[href]['titles'].append(title_text)
                                            break
                                except Exception:
                                    continue
                            break
                    except Exception:
                        continue
                        
            except Exception:
                pass  # Skip if can't find parent title
                
        except Exception as e:
            print(f"Error processing link {i}: {e}")
            continue
    
    # Process the results to select the best title for each URL
    results = []
    for url, data in link_map.items():
        # Sort titles by length (longer titles often have more information)
        sorted_titles = sorted(data['titles'], key=len, reverse=True)
        
        # Select the longest title that's substantial
        best_title = next((title for title in sorted_titles if len(title) > 3), "Unknown Title")
        
        results.append({
            'url': url,
            'title': best_title,
            'allTitles': data['titles']
        })
    
    print(f"Extracted {len(results)} unique publications from page")
    return results

# Legacy functions for backward compatibility
def verify_existing_results():
    """
    Verify that we have at least 61 results and all required fields are present.
    Returns True if verification passes, False otherwise.
    """
    try:
        # Check if the file exists
        json_path = "extract/data/publication_links.json"
        if not os.path.exists(json_path):
            print("No existing results file found.")
            return False

        # Read and parse the JSON file
        with open(json_path, "r", encoding="utf-8") as f:
            results = json.load(f)

        # Check number of results
        if len(results) < 61:
            print(f"Insufficient results: found {len(results)}, need at least 61.")
            return False

        # Check required fields in each result
        required_fields = ["title", "url", "source", "page_found"]
        for i, result in enumerate(results):
            for field in required_fields:
                if field not in result or result[field] is None:
                    print(f"Missing or null {field} in result {i+1}")
                    return False

        print(f"Verification passed: {len(results)} valid results found.")
        return True

    except Exception as e:
        print(f"Error during verification: {str(e)}")
        return False

def extract_all_publication_links(base_url, total_pages=7, max_retries=3):
    """
    Legacy function that saves results to file.
    Load multiple pages, extract links with '/publication/' in href, and save to JSON.
    Implements retry logic for handling rate limits.
    """
    all_links = get_all_publication_links(base_url, total_pages)
    
    # Save to file for backward compatibility
    os.makedirs("extract/data", exist_ok=True)
    with open("extract/data/publication_links.json", "w", encoding="utf-8") as f:
        json.dump(all_links, f, indent=2, ensure_ascii=False)
    
    return len(all_links) > 0

if __name__ == "__main__":
    base_url = "https://openknowledge.worldbank.org/communities/67a73be9-a253-4ce5-8092-36d19572f721"
    
    # Check existing results first
    if verify_existing_results():
        print("Using existing verified results. Skipping scraping.")
    else:
        print("Running scraping to collect results...")
        success = extract_all_publication_links(base_url)
        print(f"Link extraction {'successful' if success else 'failed'}")