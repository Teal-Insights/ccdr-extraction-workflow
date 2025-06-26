#!/usr/bin/env python3
"""
Classification script for World Bank download links.

This script includes:
1. READ MORE button detection and clicking
2. Language detection to filter non-English documents
3. Priority-based classification of English documents
4. Logging of all classifications for review
"""

import json
import re
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime

def install_langcodes():
    """Install langcodes if not available"""
    try:
        import langcodes
        return langcodes
    except ImportError:
        print("langcodes not found. Installing...")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "langcodes[data]"])
        import langcodes
        return langcodes

def detect_language_in_text(text: str) -> Optional[str]:
    """
    Detect if text contains a non-English language name.
    
    Returns:
        Language code if found, None if not found or if English
    """
    langcodes = install_langcodes()
    
    # Clean and split the text into words
    words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
    
    # Filter out common non-language words
    non_language_words = {
        'pdf', 'download', 'file', 'document', 'report', 'mb', 'kb', 'gb',
        'summary', 'full', 'main', 'background', 'note', 'overview', 'the',
        'and', 'or', 'for', 'in', 'with', 'of', 'a', 'an', 'is', 'are',
        'executive', 'technical', 'appendix', 'annex', 'chapter', 'section'
    }
    
    filtered_words = [word for word in words if word not in non_language_words and len(word) > 2]
    
    for word in filtered_words:
        try:
            lang = langcodes.find(word)
            if lang and lang.language != 'en':  # Not English
                return lang.language
        except:
            continue
    
    return None

def classify_download_link(text: str, position: int = 0) -> Dict[str, Any]:
    """
    Classify a single download link.
    
    Args:
        text: The link text to classify
        position: Position in the list (0-indexed, for priority determination)
    
    Returns:
        Classification dictionary with keys:
        - should_download: bool
        - classification: str
        - priority: int (1=highest, 999=skip)
        - language_detected: str or None
        - reasoning: str
    """
    text_lower = text.lower()
    
    # Check for non-English language first
    detected_lang = detect_language_in_text(text)
    if detected_lang:
        langcodes = install_langcodes()
        lang_obj = langcodes.get(detected_lang)
        lang_name = lang_obj.display_name() if lang_obj else detected_lang
        
        return {
            'should_download': False,
            'classification': f'non-english-{detected_lang}',
            'priority': 999,
            'language_detected': detected_lang,
            'reasoning': f'Detected non-English language: {lang_name}'
        }
    
    # Check for main report indicators
    main_indicators = ['main report', 'full report', 'complete report']
    if any(indicator in text_lower for indicator in main_indicators):
        return {
            'should_download': True,
            'classification': 'main-report',
            'priority': 1,
            'language_detected': None,
            'reasoning': 'Explicitly labeled as main/full/complete report'
        }
    
    # Check for supplementary document indicators
    supplementary_indicators = [
        'summary', 'executive summary', 'overview',
        'background', 'technical note', 'appendix', 
        'annex', 'chapter', 'brief'
    ]
    
    if any(indicator in text_lower for indicator in supplementary_indicators):
        return {
            'should_download': True,
            'classification': 'supplementary-english',
            'priority': 2,
            'language_detected': None,
            'reasoning': 'Identified as supplementary document'
        }
    
    # For "English PDF" without specific indicators, use position heuristic
    if 'english' in text_lower:
        if position == 0:
            return {
                'should_download': True,
                'classification': 'main-english-pdf',
                'priority': 1,
                'language_detected': None,
                'reasoning': 'First English PDF (assumed main report)'
            }
        else:
            return {
                'should_download': True,
                'classification': 'supplementary-english-pdf',
                'priority': 2,
                'language_detected': None,
                'reasoning': f'English PDF in position {position + 1} (assumed supplementary)'
            }
    
    # Default case - no explicit language specified
    if position == 0:
        return {
            'should_download': True,
            'classification': 'assumed-main-english',
            'priority': 1,
            'language_detected': None,
            'reasoning': 'First document with no language specified (assumed main English report)'
        }
    else:
        return {
            'should_download': True,
            'classification': 'assumed-supplementary-english',
            'priority': 2,
            'language_detected': None,
            'reasoning': f'Document in position {position + 1} with no language specified (assumed supplementary)'
        }

def click_show_more_if_present(page) -> bool:
    """
    Look for and click a SHOW MORE button if present.
    
    Returns:
        True if button was found and clicked, False otherwise
    """
    try:
        # Try the selector we discovered works
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
            
            return True
    except Exception as e:
        print(f"Error trying to click SHOW MORE button: {e}")
    
    return False

def extract_download_links(page) -> List[Dict[str, Any]]:
    """
    Extract download links:
    - Clicks SHOW MORE button if present
    - Extracts all download links
    - Classifies each link
    
    Returns:
        List of dictionaries with url, text, and classification info
    """
    # First, try to click SHOW MORE button
    show_more_clicked = click_show_more_if_present(page)
    if show_more_clicked:
        print("Successfully clicked SHOW MORE button")
    
    # Extract all download links
    download_links = []
    try:
        bitstream_links = page.locator('a[href*="/bitstreams/"]')
        link_count = bitstream_links.count()
        print(f"Found {link_count} download links")
        
        for i in range(link_count):
            link = bitstream_links.nth(i)
            try:
                url_attr = link.get_attribute('href')
                if not url_attr:
                    continue
                
                text = link.inner_text().strip()
                if not text:
                    continue
                
                # Convert relative URLs to absolute
                if url_attr.startswith('/'):
                    url = f"https://openknowledge.worldbank.org{url_attr}"
                else:
                    url = url_attr
                
                # Classify the link
                classification = classify_download_link(text, i)
                
                download_links.append({
                    'url': url,
                    'text': text,
                    'position': i,
                    'classification': classification
                })
                
            except Exception as e:
                print(f"Error processing link {i}: {e}")
                continue
                
    except Exception as e:
        print(f"Error extracting download links: {e}")
    
    return download_links

def filter_and_prioritize_links(download_links: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter download links to only include those that should be downloaded,
    and sort by priority.
    
    Returns:
        Filtered and sorted list of download links
    """
    # Filter to only downloadable links
    downloadable = [link for link in download_links if link['classification']['should_download']]
    
    # Sort by priority (lower number = higher priority)
    downloadable.sort(key=lambda x: x['classification']['priority'])
    
    return downloadable

def log_classification_results(download_links: List[Dict[str, Any]], publication_title: str, 
                              source_url: str, output_file: Optional[str] = None) -> None:
    """
    Log classification results to a file for review.
    """
    if output_file is None:
        # Create timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"artifacts/classification_log_{timestamp}.json"
    
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'publication_title': publication_title,
        'source_url': source_url,
        'total_links': len(download_links),
        'downloadable_links': len([l for l in download_links if l['classification']['should_download']]),
        'skipped_links': len([l for l in download_links if not l['classification']['should_download']]),
        'links': download_links
    }
    
    # Ensure directory exists
    Path(output_file).parent.mkdir(exist_ok=True)
    
    # Append to or create log file
    if Path(output_file).exists():
        with open(output_file, 'r') as f:
            logs = json.load(f)
        logs.append(log_entry)
    else:
        logs = [log_entry]
    
    with open(output_file, 'w') as f:
        json.dump(logs, f, indent=2)
    
    print(f"Classification results logged to: {output_file}")

def classify_download_links(download_links: List[Dict[str, str]], 
                                   publication_title: str = "Unknown", 
                                   source_url: str = "Unknown") -> List[Dict[str, Any]]:
    """
    This function takes the basic download links and enhances them with classification
    information, then logs the results for review.
    
    Args:
        download_links: List of dicts with 'url' and 'text' keys
        publication_title: Title of the publication for logging
        source_url: Source URL for logging
    
    Returns:
        download links with classification information
    """
    links = []
    
    for i, link in enumerate(download_links):
        classification = classify_download_link(link['text'], i)
        
        link = {
            'url': link['url'],
            'text': link['text'],
            'position': i,
            'classification': classification,
            # Keep any existing keys (like file_info)
            **{k: v for k, v in link.items() if k not in ['url', 'text']}
        }
        links.append(link)
    
    # Log the results
    log_classification_results(links, publication_title, source_url)
    
    # Filter to only include downloadable links
    downloadable_links = filter_and_prioritize_links(links)
    
    print(f"Classification summary:")
    print(f"  Total links: {len(links)}")
    print(f"  Downloadable: {len(downloadable_links)}")
    print(f"  Skipped: {len(links) - len(downloadable_links)}")
    
    # Mark which ones to download
    for link in links:
        link['to_download'] = link['classification']['should_download']
    
    return links

if __name__ == "__main__":
    # Test the classification with some sample data
    test_links = [
        {'url': 'test1.pdf', 'text': 'English PDF (3.71 MB)'},
        {'url': 'test2.pdf', 'text': 'English PDF (3.78 MB)'},
        {'url': 'test3.pdf', 'text': 'English PDF (2.86 MB)'},
        {'url': 'test4.pdf', 'text': 'Vietnamese PDF (3.59 MB)'},
        {'url': 'test5.pdf', 'text': 'Vietnamese PDF (3.56 MB)'},
    ]
    
    result = classify_download_links(test_links, "Test Publication", "test_url")
    print("\nClassification results:")
    for link in result:
        print(f"  {link['text']} -> {'DOWNLOAD' if link['to_download'] else 'SKIP'}")
        print(f"    {link['classification']['reasoning']}") 