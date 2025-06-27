import time
import random
from playwright.sync_api import sync_playwright
from typing import List
from pydantic import BaseModel, HttpUrl


class PublicationLink(BaseModel):
    """Data model for a scraped publication link."""

    title: str
    url: HttpUrl
    source: str
    page_found: int


def get_all_publication_links(base_url: str) -> List[PublicationLink]:
    """
    Scrapes all pages of the CCDR collection to get publication links.
    Continues until "Your search returned no results" is found.

    Args:
        base_url: The base URL of the CCDR collection.

    Returns:
        A list of PublicationLink objects.
    """
    all_links: List[PublicationLink] = []
    all_urls = set()  # Track URLs we've already processed
    max_retries = 3

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

        page_num = 1
        while True:
            for attempt in range(1, max_retries + 1):
                try:
                    # Construct page URL with pagination parameter
                    page_url = f"{base_url}?spc.page={page_num}"
                    print(f"\nProcessing page {page_num}")

                    # Wait between page requests to avoid rate limiting
                    if page_num > 1 or attempt > 1:
                        wait_time = random.uniform(
                            5, 10
                        )  # Random wait between 5-10 seconds
                        print(f"Waiting {wait_time:.1f} seconds before next request...")
                        time.sleep(wait_time)

                    # Extract publications from this page
                    result = extract_publication_links_from_page(page, page_url)

                    # Check if we were successful or rate limited
                    if result is None:
                        print(
                            f"Failed attempt {attempt}/{max_retries} for page {page_num}"
                        )

                        if attempt == max_retries:
                            print(
                                f"Maximum retries reached for page {page_num}. Moving to next page."
                            )
                            break

                        # Wait longer before retry
                        wait_time = random.uniform(15, 30)
                        print(
                            f"Rate limited. Waiting {wait_time:.1f} seconds before retry..."
                        )
                        time.sleep(wait_time)
                        continue

                    # Check if we reached the end (no results found)
                    if result == "no_results":
                        print("Reached end of results - 'Your search returned no results' found")
                        break  # Exit the retry loop

                    page_publications = result

                    # Process publications found on this page
                    new_count = 0
                    for pub in page_publications:
                        # Only add if we haven't seen this URL before
                        if pub["url"] not in all_urls:
                            all_urls.add(pub["url"])
                            all_links.append(
                                PublicationLink(
                                    title=pub["title"],
                                    url=pub["url"],
                                    source="World Bank Open Knowledge Repository",
                                    page_found=page_num,
                                )
                            )
                            new_count += 1

                    print(
                        f"Found {len(page_publications)} publications on page {page_num}"
                    )
                    print(
                        f"Total unique publications collected so far: {len(all_links)}"
                    )

                    # Successfully processed this page, move to next
                    break

                except Exception as e:
                    print(
                        f"Error during processing of page {page_num} (attempt {attempt}/{max_retries}): {str(e)}"
                    )

                    if attempt == max_retries:
                        print(
                            f"Maximum retries reached for page {page_num}. Moving to next page."
                        )
                        break

                    # Wait before retry
                    wait_time = random.uniform(10, 20)
                    print(
                        f"Error encountered. Waiting {wait_time:.1f} seconds before retry..."
                    )
                    time.sleep(wait_time)

            # Check if we found "no results" and should exit the main loop
            if result == "no_results":
                break

            # Increment page number for next iteration
            page_num += 1

        browser.close()

    print(f"\nCompleted extraction from {page_num} pages")
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
        page.wait_for_selector(
            'a[href*="/publication/"], .pagination, .search-results, .item',
            timeout=8000,
        )
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
            ".cookie-accept",
            "#accept-cookies",
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

    # Check if we've reached the end of results
    try:
        no_results_text = page.locator('text="Your search returned no results"')
        if no_results_text.count() > 0:
            print("Found 'Your search returned no results' - reached end of pagination")
            return "no_results"
    except Exception as e:
        print(f"Error checking for no results text: {e}")

    # Use Playwright's Python API to extract publication links
    print("Extracting publication links...")

    # Find all links containing '/publication/' in their href
    publication_links = page.locator('a[href*="/publication/"]')
    link_count = publication_links.count()
    print(f"Found {link_count} publication links on page")

    if link_count == 0:
        print("No publication links found - likely reached end of results.")
        return "no_results"

    # Track unique URLs to avoid duplicates
    link_map = {}
    skip_texts = {"download", "view", "pdf", "read"}

    # Process each link
    for i in range(link_count):
        try:
            link = publication_links.nth(i)
            href = link.get_attribute("href")

            if not href:
                continue

            # Convert relative URLs to absolute
            if href.startswith("/"):
                href = f"https://openknowledge.worldbank.org{href}"

            # Initialize entry if URL hasn't been seen
            if href not in link_map:
                link_map[href] = {
                    "url": href,
                    "titles": [],
                }

            # Get link text
            try:
                link_text = link.inner_text().strip()
                if link_text and link_text.lower() not in skip_texts:
                    link_map[href]["titles"].append(link_text)
            except Exception:
                pass  # Skip if can't get text

            # Try to find title from parent containers
            try:
                # Look for parent elements that might contain the title
                parent_selectors = ["li", ".item", ".publication-item", ".result-item"]

                for selector in parent_selectors:
                    try:
                        parent = link.locator(
                            f'xpath=ancestor::{selector.replace(".", "")}[1]'
                        )
                        if parent.count() > 0:
                            # Try common title selectors within the parent
                            title_selectors = [
                                "h1",
                                "h2",
                                "h3",
                                "h4",
                                "h5",
                                ".title",
                                ".item-title",
                            ]
                            for title_selector in title_selectors:
                                try:
                                    title_elem = parent.locator(title_selector).first
                                    if title_elem.count() > 0:
                                        title_text = title_elem.inner_text().strip()
                                        if title_text and len(title_text) > 3:
                                            link_map[href]["titles"].append(title_text)
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
        sorted_titles = sorted(data["titles"], key=len, reverse=True)

        # Select the longest title that's substantial
        best_title = next(
            (title for title in sorted_titles if len(title) > 3), "Unknown Title"
        )

        results.append({"url": url, "title": best_title, "allTitles": data["titles"]})

    print(f"Extracted {len(results)} unique publications from page")
    return results


if __name__ == "__main__":
    # Example usage of the main function
    base_url = "https://openknowledge.worldbank.org/collections/5cd4b6f6-94bb-5996-b00c-58be279093de"
    all_links = get_all_publication_links(base_url)

    print(f"Extracted {len(all_links)} publication links:")
    for i, link in enumerate(all_links[:5]):  # Show first 5 as example
        print(f"{i+1}. {link.title}")
        print(f"   URL: {link.url}")
        print(f"   Found on page: {link.page_found}")
        print()
