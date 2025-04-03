import json
import requests
from pathlib import Path
import sys
import re
import time
import random

# Browser-like headers to avoid rate limiting
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
}

def parse_content_type(content_type_str):
    """Parse content type string into MIME type and charset"""
    if not content_type_str or content_type_str == 'unknown' or content_type_str == 'error':
        return {'mime_type': 'unknown', 'charset': None}
    
    # Split on first semicolon
    parts = content_type_str.split(';', 1)
    mime_type = parts[0].strip().lower()
    
    # Extract charset if present
    charset = None
    if len(parts) > 1:
        charset_match = re.search(r'charset\s*=\s*([^\s;]+)', parts[1], re.I)
        if charset_match:
            charset = charset_match.group(1).strip('"\'').lower()
    
    return {'mime_type': mime_type, 'charset': charset}

def guess_file_type_from_text(text):
    """Guess file type from the link text"""
    text = text.lower()
    if 'pdf' in text:
        return 'application/pdf'
    elif 'text' in text:
        return 'text/plain'
    return None

def is_valid_file_info(file_info):
    """Check if the file info represents a valid, successful response"""
    if not file_info:
        return False
        
    # Check for error indicators
    if file_info.get('mime_type') in ['error', 'unknown']:
        return False
        
    # Check for non-utf-8 HTML responses (likely rate limiting or redirect pages)
    if file_info.get('mime_type', '').startswith('text/html') and file_info.get('charset', '').startswith('iso-8859-1'):
        return False
            
    return True

def get_file_type_from_url(url, link_text, max_retries=3):
    """Get file type with retry logic for rate limiting"""
    guessed_type = guess_file_type_from_text(link_text)
    
    for attempt in range(1, max_retries + 1):
        try:
            # Add random delay between requests
            if attempt > 1:
                wait_time = random.uniform(5, 10)  # Random wait between 5-10 seconds
                print(f"Waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)
            
            # Make a GET request with stream=True to get headers and peek at content
            with requests.get(url, stream=True, allow_redirects=True, headers=DEFAULT_HEADERS) as response:
                # Check for rate limiting
                if response.status_code == 429:
                    if attempt == max_retries:
                        raise Exception("Rate limited (429) - max retries reached")
                    wait_time = random.uniform(15, 30)  # Longer wait for rate limiting
                    print(f"Rate limited. Waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                
                # Get the final URL after redirects
                final_url = response.url
                
                # Get content type and charset from headers
                content_type = response.headers.get('Content-Type', 'unknown')
                parsed_header = parse_content_type(content_type)
                
                # Get content length if available
                content_length = response.headers.get('Content-Length', 'unknown')
                
                # If we're still getting JSON content type or HTML, try to peek at actual content
                if 'json' in parsed_header['mime_type'] or 'html' in parsed_header['mime_type']:
                    # Read first few bytes to detect actual file type
                    content_start = response.raw.read(2048)  # Read first 2KB
                    response.close()  # Important: close the connection
                    
                    # Use python-magic to detect file type from content
                    import magic
                    detected_type = magic.from_buffer(content_start, mime=True)
                    parsed_content = parse_content_type(detected_type)
                    
                    # Use detected MIME type but keep charset from headers if it was specified
                    mime_type = parsed_content['mime_type']
                    charset = parsed_header['charset'] if parsed_header['charset'] != 'utf-8' else parsed_content['charset']
                else:
                    mime_type = parsed_header['mime_type']
                    charset = parsed_header['charset']
                
                result = {
                    'guessed_type': guessed_type,
                    'mime_type': mime_type,
                    'charset': charset,
                    'content_length': content_length,
                    'final_url': final_url,
                    'status_code': response.status_code
                }
                
                # If we got HTML when expecting PDF/text, consider it a failure
                if not is_valid_file_info(result):
                    if attempt == max_retries:
                        raise Exception("Failed to get expected file type")
                    print("Got unexpected file type, retrying...")
                    continue
                
                return result
                
        except Exception as e:
            print(f"Attempt {attempt}/{max_retries} failed: {str(e)}")
            if attempt == max_retries:
                return {
                    'error': str(e),
                    'guessed_type': guessed_type,
                    'mime_type': 'error',
                    'charset': None,
                    'content_length': 'error',
                    'final_url': url,
                    'status_code': 'error'
                }

def main():
    # Read the publication details
    data_file = Path(__file__).parent / 'data' / 'publication_details.json'
    
    try:
        with open(data_file, 'r') as f:
            publications = json.load(f)
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

    # Check if all entries already have valid file info
    total_links = sum(len(pub.get('downloadLinks', [])) for pub in publications)
    needs_processing = False
    
    print(f"Checking {total_links} total download links for valid file info...")
    for pub in publications:
        for link in pub.get('downloadLinks', []):
            if 'file_info' not in link or not is_valid_file_info(link['file_info']):
                needs_processing = True
                break
        if needs_processing:
            break
    
    if not needs_processing:
        print("All entries already have valid file info. No processing needed.")
        return

    # Process each publication and update the data
    modified = False
    processed = 0
    
    for pub in publications:
        print(f"\nPublication: {pub['title']}")
        
        for link in pub.get('downloadLinks', []):
            processed += 1
            url = link['url']
            print(f"\nProcessing link {processed}/{total_links}")
            print(f"URL: {url}")
            print(f"Link text: {link['text']}")
            
            # Skip if we already have valid file info
            if 'file_info' in link and is_valid_file_info(link['file_info']):
                print("Already have valid file info, skipping...")
                continue
            
            info = get_file_type_from_url(url, link['text'])
            
            if 'error' in info:
                print(f"Error: {info['error']}")
                # Store error information in the link
                link['file_info'] = {
                    'error': info['error'],
                    'mime_type': 'error',
                    'charset': None
                }
            else:
                print(f"Guessed type from text: {info['guessed_type']}")
                print(f"MIME Type: {info['mime_type']}")
                print(f"Charset: {info['charset']}")
                print(f"Content-Length: {info['content_length']}")
                print(f"Final URL: {info['final_url']}")
                print(f"Status Code: {info['status_code']}")
                
                # Store the file information in the link
                link['file_info'] = {
                    'mime_type': info['mime_type'],
                    'charset': info['charset'],
                    'content_length': info['content_length'],
                    'final_url': info['final_url']
                }
            
            modified = True
            
            # Save progress periodically (every 5 links)
            if processed % 5 == 0:
                try:
                    with open(data_file, 'w') as f:
                        json.dump(publications, f, indent=2)
                    print(f"\nSaved progress ({processed}/{total_links} links processed)")
                except Exception as e:
                    print(f"\nError saving progress: {e}")
    
    # Final save if modifications were made
    if modified:
        try:
            with open(data_file, 'w') as f:
                json.dump(publications, f, indent=2)
            print("\nSuccessfully updated publication_details.json with file type information.")
        except Exception as e:
            print(f"\nError saving updates: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()
