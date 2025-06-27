import requests
import re
import time
import random
from typing import Union, List
from pydantic import BaseModel, HttpUrl
from extract.extract_publication_details import DownloadLink, PublicationDetailsBase

# Browser-like headers to avoid rate limiting
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# Set to True to store "/content" (post-redirect URL);
# False to store "/download" (pre-redirect URL)
STORE_FINAL_URL = True


# Pydantic models for structured data
class FileTypeInfo(BaseModel):
    """Represents file type information extracted from a URL."""

    mime_type: str
    charset: str
    content_length: Union[int, str]  # Can be int or "unknown"/"error"


class DownloadLinkWithFileInfo(DownloadLink):
    file_info: FileTypeInfo


class PublicationDetailsWithFileInfo(PublicationDetailsBase):
    download_links: List[DownloadLinkWithFileInfo]


def parse_content_type(content_type_str):
    """Parse content type string into MIME type and charset"""
    if (
        not content_type_str
        or content_type_str == "unknown"
        or content_type_str == "error"
    ):
        return {"mime_type": "unknown", "charset": None}

    # Split on first semicolon
    parts = content_type_str.split(";", 1)
    mime_type = parts[0].strip().lower()

    # Extract charset if present
    charset = None
    if len(parts) > 1:
        charset_match = re.search(r"charset\s*=\s*([^\s;]+)", parts[1], re.I)
        if charset_match:
            charset = charset_match.group(1).strip("\"'").lower()

    return {"mime_type": mime_type, "charset": charset}


def guess_file_type_from_text(text):
    """Guess file type from the link text"""
    text = text.lower()
    if "pdf" in text:
        return "application/pdf"
    elif "text" in text:
        return "text/plain"
    return None


def is_valid_file_info(file_info):
    """Check if the file info represents a valid, successful response"""
    if not file_info:
        return False

    # Check for error indicators
    if file_info.get("mime_type") in ["error", "unknown"]:
        return False

    # Check for non-utf-8 HTML responses (likely rate limiting or redirect pages)
    mime_type = file_info.get("mime_type", "")
    charset = file_info.get("charset", "")
    if (
        mime_type.startswith("text/html")
        and charset
        and charset.startswith("iso-8859-1")
    ):
        return False

    return True


def transform_worldbank_url(url):
    """Transform World Bank download URLs to content URLs for direct file access"""
    import re

    # Pattern for World Bank bitstream download URLs
    download_pattern = (
        r"https://openknowledge\.worldbank\.org/bitstreams/([a-f0-9-]+)/download"
    )
    match = re.match(download_pattern, url)

    if match:
        uuid = match.group(1)
        content_url = f"https://openknowledge.worldbank.org/server/api/core/bitstreams/{uuid}/content"
        return content_url

    return url  # Return original URL if no transformation needed


def get_file_type_from_url(download_link: DownloadLink, max_retries=3) -> DownloadLinkWithFileInfo:
    """
    Get file type with retry logic for rate limiting.
    
    Raises:
        Exception: If unable to determine a valid MIME type after all retries.
    """
    guessed_type = guess_file_type_from_text(download_link.text)

    # Transform World Bank download URLs to content URLs for direct access
    actual_url = transform_worldbank_url(download_link.url)
    if actual_url != download_link.url:
        print(f"Transformed URL: {download_link.url} -> {actual_url}")

    for attempt in range(1, max_retries + 1):
        try:
            # Add random delay between requests
            if attempt > 1:
                wait_time = random.uniform(5, 10)  # Random wait between 5-10 seconds
                print(f"Waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)

            # Make a GET request with stream=True to get headers and peek at content
            with requests.get(
                str(actual_url), stream=True, allow_redirects=True, headers=DEFAULT_HEADERS
            ) as response:
                # Check for rate limiting
                if response.status_code == 429:
                    if attempt == max_retries:
                        raise Exception(f"Rate limited (429) after {max_retries} attempts for {download_link.url}")
                    wait_time = random.uniform(15, 30)  # Longer wait for rate limiting
                    print(
                        f"Rate limited. Waiting {wait_time:.1f} seconds before retry..."
                    )
                    time.sleep(wait_time)
                    continue

                # Get content type and charset from the FINAL response headers (post-redirect)
                content_type = response.headers.get("Content-Type", "unknown")
                parsed_header = parse_content_type(content_type)

                # Get content length if available
                content_length = response.headers.get("Content-Length", "unknown")
                # Convert to int if it's a valid number
                if content_length != "unknown":
                    try:
                        content_length = int(content_length)
                    except ValueError:
                        content_length = "unknown"

                # If we're still getting JSON content type or HTML, try to peek at actual content
                if (
                    "json" in parsed_header["mime_type"]
                    or "html" in parsed_header["mime_type"]
                ):
                    # Read first few bytes to detect actual file type
                    # Use response.content instead of raw to get decompressed content
                    response_content = response.content
                    content_start = response_content[:2048]  # Get first 2KB

                    # Use python-magic to detect file type from content
                    import magic

                    detected_type = magic.from_buffer(content_start, mime=True)
                    if not detected_type:
                        detected_type = "application/octet-stream"  # Fallback
                    parsed_content = parse_content_type(detected_type)

                    # Use detected MIME type, but prefer charset from final response headers
                    mime_type = parsed_content["mime_type"]
                    charset = parsed_header["charset"]
                else:
                    mime_type = parsed_header["mime_type"]
                    charset = parsed_header["charset"]

                # Apply UTF-8 fallback if no charset was determined
                if not charset:
                    charset = "utf-8"
                    print(f"Warning: No charset detected for {download_link.url}, defaulting to UTF-8")

                # Log warning if guessed type doesn't match actual MIME type
                if guessed_type and guessed_type != mime_type:
                    print(f"Warning: Guessed type '{guessed_type}' doesn't match detected type '{mime_type}' for {download_link.url}")

                result = FileTypeInfo(
                    mime_type=mime_type,
                    charset=charset,
                    content_length=content_length,
                )

                # If we got HTML when expecting PDF/text, consider it a failure
                if not is_valid_file_info(result.model_dump()):
                    if attempt == max_retries:
                        raise Exception(f"Failed to get valid file type for {download_link.url} after {max_retries} attempts - got {mime_type}")
                    print("Got unexpected file type, retrying...")
                    continue

                return DownloadLinkWithFileInfo(
                    url=HttpUrl(actual_url) if STORE_FINAL_URL else download_link.url,
                    text=download_link.text,
                    file_info=result
                )

        except Exception as e:
            print(f"Attempt {attempt}/{max_retries} failed: {str(e)}")
            if attempt == max_retries:
                raise Exception(f"Failed to determine MIME type for {download_link.url} after {max_retries} attempts: {str(e)}")
    
    # This should never be reached, but satisfies the type checker
    raise Exception(f"Unexpected exit from retry loop for {download_link.url}")


def main():
    # Create a sample DownloadLink
    download_link = DownloadLink(
        url=HttpUrl("https://openknowledge.worldbank.org/bitstreams/cf2a2b54-559b-5909-ada8-af36b21bd4da/download"),
        text="English PDF (18.05 MB)"
    )

    dl_with_info = get_file_type_from_url(download_link)
    print(dl_with_info.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
