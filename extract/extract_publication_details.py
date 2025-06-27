import json
import time
import random
from playwright.sync_api import sync_playwright
from typing import Dict, Any, Optional


def scrape_publication_details_with_retry(url: str, max_retries: int = 3, base_delay: float = 2.0) -> Optional[Dict[str, Any]]:
    """
    Scrapes publication details with retry logic and exponential backoff.
    
    Args:
        url: The URL of the publication's detail page.
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds between retries (default: 2.0)
    
    Returns:
        A dictionary containing all scraped metadata, or None if all attempts fail.
    """
    for attempt in range(max_retries + 1):  # +1 because we include the initial attempt
        if attempt > 0:
            # Calculate exponential backoff with jitter
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
            print(f"Attempt {attempt + 1}/{max_retries + 1} for {url} (waiting {delay:.1f}s)")
            time.sleep(delay)
        else:
            print(f"Attempt {attempt + 1}/{max_retries + 1} for {url}")
        
        result = scrape_publication_details(url)
        
        if result is not None:
            # Success - we got valid data
            if attempt > 0:
                print(f"Success on attempt {attempt + 1}")
            return result
        
        # Check if we should retry
        if attempt < max_retries:
            print(f"Attempt {attempt + 1} failed, will retry...")
        else:
            print(f"All {max_retries + 1} attempts failed for {url}")
    
    return None


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
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
                "--disable-dev-shm-usage",
            ],
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
                "Cache-Control": "max-age=0",
            },
        )

        page = context.new_page()

        # Hide automation indicators
        page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Remove automation indicators
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        """
        )

        try:
            print(f"Navigating to: {url}")
            response = page.goto(url, wait_until="domcontentloaded", timeout=30000)  # Increased timeout to 30s

            # Check if we got rate limited
            if response and response.status == 429:
                print(f"Rate limited (429 status).")
                return None

            # Wait for the page to load with longer timeout
            page.wait_for_load_state("networkidle", timeout=20000)  # Increased to 20s

            # Wait for page content to load with much longer timeout and better error detection
            print("Waiting for page content to load...")
            try:
                page.wait_for_selector('h2, h5, a[href*="/bitstreams/"]', timeout=20000)  # Increased to 20s
                print("Page content loaded")
            except Exception as e:
                print(f"Content loading timeout: {e}")
                # Check if this looks like a rate limiting or error page
                title = page.title()
                body_text = ""
                try:
                    body_text = page.locator("body").inner_text().lower()
                except:
                    pass
                
                # Enhanced rate limiting detection
                rate_limit_indicators = [
                    "429", "too many requests", "rate limit", "slow down",
                    "try again later", "temporarily unavailable"
                ]
                
                if any(indicator in title.lower() for indicator in rate_limit_indicators) or \
                   any(indicator in body_text for indicator in rate_limit_indicators):
                    print("Detected rate limiting or server issues.")
                    return None
                
                # If it's just a timeout but page seems to have loaded partially, continue
                print("Continuing with partial page load...")

            # Check page title for common error indicators
            title = page.title()
            print(f"Page title: {title}")

            # Enhanced error detection
            error_indicators = ["429", "too many requests", "error", "not found", "unavailable"]
            if any(indicator in title.lower() for indicator in error_indicators):
                print(f"Detected error page based on title: {title}")
                return None

            # Use Python Playwright API to extract all required information
            print("Extracting publication details...")
            publication_data = {}

            # Get the title
            try:
                title_element = page.locator("h2").first
                if title_element.count() > 0:
                    title_text = title_element.inner_text().strip()
                    # Remove "Publication:" prefix if present
                    if title_text.startswith("Publication:"):
                        title_text = title_text.replace("Publication:", "").strip()
                    publication_data["title"] = title_text
                else:
                    publication_data["title"] = None
                    print("Warning: No title found")
            except Exception as e:
                print(f"Error extracting title: {e}")
                publication_data["title"] = None

            # If we couldn't extract a title, this is likely a failed page load
            if not publication_data.get("title"):
                print("No title extracted - likely failed page load")
                return None

            # Helper function to get field value based on h5 heading
            def get_field_value(label: str) -> Optional[str]:
                try:
                    # Find h5 heading with the label
                    headings = page.locator("h5")
                    heading_count = headings.count()

                    for i in range(heading_count):
                        heading = headings.nth(i)
                        if heading.inner_text().strip() == label:
                            # Found the heading, now find the next sibling with content
                            try:
                                # Try to find the next element with content
                                parent = heading.locator("xpath=..")
                                following_elements = parent.locator(
                                    f'xpath=.//*[normalize-space(text()) != ""][position() > count(h5[normalize-space(text()) = "{label}"]/preceding-sibling::*) + 1]'
                                )

                                if following_elements.count() > 0:
                                    content = (
                                        following_elements.first.inner_text().strip()
                                    )
                                    return content if content else None

                                # Fallback: try next sibling elements
                                next_sibling = heading.locator(
                                    "xpath=following-sibling::*[1]"
                                )
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
            publication_data["abstract"] = get_field_value("Abstract")
            publication_data["citation"] = get_field_value("Citation")

            # Get URI - special handling to extract the link
            try:
                uri = None
                headings = page.locator("h5")
                heading_count = headings.count()

                for i in range(heading_count):
                    heading = headings.nth(i)
                    if heading.inner_text().strip() == "URI":
                        # Find the link in the following content
                        try:
                            parent = heading.locator("xpath=..")
                            uri_links = parent.locator("a")
                            uri_link_count = uri_links.count()

                            for j in range(uri_link_count):
                                link = uri_links.nth(j)
                                href = link.get_attribute("href")
                                if href and (
                                    "hdl.handle.net" in href or "doi.org" in href
                                ):
                                    uri = href
                                    break
                        except Exception:
                            pass
                        break

                publication_data["uri"] = uri
            except Exception as e:
                print(f"Error extracting URI: {e}")
                publication_data["uri"] = None

            # Get download links with enhanced functionality
            try:
                download_links = []

                # First, try to click SHOW MORE button if present
                try:
                    show_more_button = page.locator('a:has-text("SHOW MORE")')
                    if show_more_button.count() > 0 and show_more_button.is_visible():
                        print("Found SHOW MORE button, clicking...")
                        show_more_button.click()

                        # Wait for content to load
                        import time

                        time.sleep(2)
                        try:
                            page.wait_for_load_state("networkidle", timeout=10000)
                        except:
                            pass  # Continue even if timeout

                        print("Successfully clicked SHOW MORE button")
                except Exception as e:
                    print(f"Error trying to click SHOW MORE button: {e}")

                # Extract all download links
                bitstream_links = page.locator('a[href*="/bitstreams/"]')
                link_count = bitstream_links.count()
                print(f"Found {link_count} download links")

                for i in range(link_count):
                    link = bitstream_links.nth(i)
                    try:
                        url_attr = link.get_attribute("href")
                        if not url_attr:
                            continue  # Skip links without URLs
                        download_url = url_attr  # Use a different variable name

                        text = link.inner_text().strip()
                        if not text:
                            continue  # Skip links without text

                        # Convert relative URLs to absolute URLs
                        if download_url.startswith("/"):
                            download_url = f"https://openknowledge.worldbank.org{download_url}"

                        # At this point, both download_url and text are guaranteed to be non-None strings
                        download_links.append({"url": download_url, "text": text})  # Use download_url here
                    except Exception:
                        continue

                publication_data["downloadLinks"] = download_links
            except Exception as e:
                print(f"Error extracting download links: {e}")
                publication_data["downloadLinks"] = []

            # Get additional metadata
            metadata: Dict[str, Optional[str]] = {
                "date": get_field_value("Date"),
                "published": get_field_value("Published"),
                "authors": get_field_value("Author(s)"),
            }
            publication_data["metadata"] = metadata

            # Add source URL to the data
            publication_data["source_url"] = url

            print(
                f"Extracted details for: {publication_data.get('title', 'Unknown Title')}"
            )

            return publication_data

        except Exception as e:
            print(f"Error during scraping of {url}: {str(e)}")
            return None
        finally:
            browser.close()


if __name__ == "__main__":
    # Example usage of the main function
    test_url = "https://openknowledge.worldbank.org/publication/example"
    result = scrape_publication_details_with_retry(test_url)
    if result:
        print("Successfully extracted publication details:")
        print(json.dumps(result, indent=2))
