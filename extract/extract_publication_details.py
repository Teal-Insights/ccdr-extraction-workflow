import json
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
                            
                        # Convert relative URLs to absolute URLs
                        if url.startswith('/'):
                            url = f"https://openknowledge.worldbank.org{url}"
                        
                        # At this point, both url and text are guaranteed to be non-None strings
                        download_links.append({
                            'url': url,
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

if __name__ == "__main__":
    # Example usage of the main function
    test_url = "https://openknowledge.worldbank.org/publication/example"
    result = scrape_publication_details(test_url)
    if result:
        print("Successfully extracted publication details:")
        print(json.dumps(result, indent=2)) 