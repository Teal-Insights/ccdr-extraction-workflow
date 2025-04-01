import json
import time
import random
from playwright.sync_api import sync_playwright
import os

def verify_existing_results():
    """
    Verify that we have at least 61 results and all required fields are present.
    Returns True if verification passes, False otherwise.
    """
    try:
        # Check if the file exists
        json_path = "discovery/data/publication_links.json"
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

def extract_publication_links_from_page(page, url):
    """
    Extract publication links from a single page.
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
    print("Waiting for additional content loading...")
    page.wait_for_timeout(5000)  # Additional 5s wait for any dynamic content
    
    # Check page title for common error indicators
    title = page.title()
    print(f"Page title: {title}")
    
    if "429" in title or "too many requests" in title.lower():
        print(f"Detected rate limiting in title.")
        return None
    
    # Use JavaScript to extract all publication links with better title handling
    # and deduplication based on URL
    publication_data = page.evaluate("""() => {
        // Create a Map to store unique links by URL
        const linkMap = new Map();
        
        // Find all links containing '/publication/'
        const links = Array.from(document.querySelectorAll('a[href*="/publication/"]'));
        
        // Process each link
        links.forEach(link => {
            const href = link.href;
            const linkText = link.textContent.trim();
            
            // Initialize entry if this URL hasn't been seen yet
            if (!linkMap.has(href)) {
                linkMap.set(href, {
                    url: href,
                    titles: [],
                    bestTitle: ""
                });
            }
            
            // Add this title if not empty
            if (linkText && !['download', 'view', 'pdf', 'read'].includes(linkText.toLowerCase())) {
                linkMap.get(href).titles.push(linkText);
            }
            
            // Try to find title from parent containers
            const parent = link.closest('.item') || link.closest('li') || link.parentElement;
            if (parent) {
                // Try common title selectors
                const titleSelectors = ['h2', 'h3', 'h4', '.title', '.item-title', 'h1', 'h5'];
                for (const selector of titleSelectors) {
                    const titleElem = parent.querySelector(selector);
                    if (titleElem) {
                        const titleText = titleElem.textContent.trim();
                        if (titleText) {
                            linkMap.get(href).titles.push(titleText);
                            break;
                        }
                    }
                }
            }
        });
        
        // Process the map to select the best title for each URL
        const results = [];
        linkMap.forEach((item, url) => {
            // Sort titles by length (longer titles often have more information)
            const sortedTitles = item.titles.sort((a, b) => b.length - a.length);
            
            // Select the longest title that's not just a few characters
            const bestTitle = sortedTitles.find(title => title.length > 3) || "Unknown Title";
            
            results.push({
                url: url,
                title: bestTitle,
                allTitles: item.titles
            });
        });
        
        return results;
    }""")
    
    return publication_data

def extract_all_publication_links(base_url, total_pages=7, max_retries=3):
    """
    Load multiple pages, extract links with '/publication/' in href, and save to JSON.
    Implements retry logic for handling rate limits.
    """
    all_links = []
    all_urls = set()  # Track URLs we've already processed
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        
        # Create context with more realistic browser settings
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        page = context.new_page()
        
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
                    
                    # Save progress after each page
                    with open("artifacts/publication_links.json", "w", encoding="utf-8") as f:
                        json.dump(all_links, f, indent=2, ensure_ascii=False)
                    
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
    
    # Final save with all results
    with open("discovery/data/publication_links.json", "w", encoding="utf-8") as f:
        json.dump(all_links, f, indent=2, ensure_ascii=False)
    
    return len(all_links) > 0

if __name__ == "__main__":
    base_url = "https://openknowledge.worldbank.org/collections/5cd4b6f6-94bb-5996-b00c-58be279093de"
    
    # Check existing results first
    if verify_existing_results():
        print("Using existing verified results. Skipping scraping.")
    else:
        print("Running scraping to collect results...")
        success = extract_all_publication_links(base_url)
        print(f"Link extraction {'successful' if success else 'failed'}")