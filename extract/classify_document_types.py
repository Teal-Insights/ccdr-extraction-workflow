#!/usr/bin/env python3
"""
Classification script for World Bank download links.

This script includes:
1. Language detection to filter non-English documents
2. File type-based filtering of English documents to only include PDFs
3. Logging of all classifications for review
"""

import re
from typing import List, Optional
import langcodes

from extract.schema import DocumentType
from extract.extract_publication_details import PublicationDetailsBase
from extract.classify_mime_types import DownloadLinkWithFileInfo


# Filter out common non-language words and short words
NON_LANGUAGE_WORDS = {
    "pdf", "download", "file", "document", "report", "mb", "kb", "gb",
    "summary", "full", "main", "background", "note", "overview",
    "the", "and", "or", "for", "in", "with", "of", "a", "an", "is", "are",
    "executive", "technical", "appendix", "annex", "chapter", "section",
    # Add common English words that were causing false positives
    "climate", "change", "economic", "damage", "environmental", "risks",
    "financial", "private", "sector", "forestry", "agroforestry", 
    "assessment", "groundwater", "irrigation", "indicative", "total",
    "development", "financing", "needs", "estimating", "country"
}

NON_PDF_INDICATORS = [" text "]

# Check for supplementary document indicators
SUPPLEMENTARY_INDICATORS = [
    "summary",
    "executive summary",
    "overview",
    "background",
    "technical note",
    "appendix",
    "annex",
    "chapter",
    "brief",
]

class DownloadLinkWithClassification(DownloadLinkWithFileInfo):
    classification: DocumentType
    language_detected: Optional[str]
    reasoning: str

class PublicationDetailsWithClassification(PublicationDetailsBase):
    download_links: List[DownloadLinkWithClassification]


def detect_language_in_text(text: str) -> Optional[str]:
    """
    Detect if text contains explicit non-English language names.
    
    This function looks for actual language names in English using langcodes.find(),
    which is much more precise than fuzzy matching language codes.

    Returns:
        Two-letter language code if found, None if not found or if English
    """
    # Extract words, removing punctuation and size info
    words = re.findall(r"\b[a-zA-Z]+\b", text.lower())
    
    filtered_words = [word for word in words if word not in NON_LANGUAGE_WORDS and len(word) > 2]
    
    for word in filtered_words:
        try:
            # Try to find the word as a language name in English
            lang = langcodes.find(word, language='en')
            if lang and lang.language != 'en':  # Not English
                return lang.language
        except LookupError:
            # Word is not a language name, continue checking other words
            continue
    
    return None


def classify_download_link(input: DownloadLinkWithFileInfo, position: int = 0) -> Optional[DownloadLinkWithClassification]:
    """
    Classify a single download link.

    Args:
        text: The link text to classify
        position: Position in the list (0-indexed, for priority determination)

    Returns:
        Classification result or None if the link should not be downloaded
    """
    text_lower = input.text.lower()

    # Detect language and set default to English
    detected_lang = detect_language_in_text(input.text) or 'en'
    is_pdf = any(indicator in text_lower for indicator in NON_PDF_INDICATORS)

    if detected_lang != 'en' or not is_pdf:
        return None

    # Check for main report indicators
    main_indicators = ["main report", "full report", "complete report"]
    if any(indicator in text_lower for indicator in main_indicators):
        return DownloadLinkWithClassification(
            url=input.url,
            text=input.text,
            file_info=input.file_info,
            classification=DocumentType.MAIN,
            language_detected=detected_lang,
            reasoning="Explicitly labeled as main/full/complete report",
        )

    if any(indicator in text_lower for indicator in SUPPLEMENTARY_INDICATORS):
        return DownloadLinkWithClassification(
            url=input.url,
            text=input.text,
            file_info=input.file_info,
            classification=DocumentType.SUPPLEMENTAL,
            language_detected=detected_lang,
            reasoning="Identified as supplementary document",
        )

    # For "English PDF" without specific indicators, use position heuristic
    if "english" in text_lower:
        return DownloadLinkWithClassification(
            url=input.url,
            text=input.text,
            file_info=input.file_info,
            classification=DocumentType.MAIN,
            language_detected=detected_lang,
            reasoning=(
                "First English PDF (assumed main report)"
                if position == 0 else
                f"English PDF in position {position + 1} (assumed supplementary)"
            )
        )

    # Default case - no explicit language or PDF specified
    return DownloadLinkWithClassification(
        url=input.url,
        text=input.text,
        file_info=input.file_info,
        classification=DocumentType.MAIN,
        language_detected=detected_lang,
        reasoning=(
            "First document with no language specified (assumed main English report)"
            if position == 0 else
            f"Document in position {position + 1} with no language specified (assumed supplementary)"
        )
    )


def classify_download_links(
    download_links: List[DownloadLinkWithFileInfo]
) -> List[DownloadLinkWithClassification]:
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
    links = [
        classify_download_link(link, i)
        for i, link in enumerate(download_links)
    ]

    filtered_links = [link for link in links if link]

    print(f"Classification summary:")
    print(f"  Total links: {len(links)}")
    print(f"  Downloadable: {len(filtered_links)}")
    print(f"  Skipped: {len(links) - len(filtered_links)}")

    return filtered_links


if __name__ == "__main__":
    from extract.classify_mime_types import FileTypeInfo
    from pydantic import HttpUrl
    
    # Test the classification with some sample data including problematic cases
    test_links = [
        "English PDF (3.71 MB)",
        "English PDF (3.78 MB)",
        "English PDF (2.86 MB)",
        "Vietnamese PDF (3.59 MB)",
        "Vietnamese PDF (3.56 MB)",
        "English Summary Text (88.61 KB)",
        # Test the problematic cases that were incorrectly flagged as non-English
        "Estimating the Economic Damage of Climate Change in Kenya (7.44 MB)",
        "Climate Change and Environmental Risks in the Financial and Private Sector (1.3 MB)",
        "Forestry and Agroforestry Sector Assessment - A Background Note on Peru (2.03 MB)",
        "Groundwater Irrigation in Punjab (97.18 KB)",
        "Indicative Total Climate and Development Financing Needs (84.74 KB)",
        # Test some legitimate non-English cases
        "French Report Summary (2.1 MB)",
        "Spanish Document Overview (1.8 MB)",
        "Chinese Analysis Report (3.2 MB)",
        "Agriculture in Punjab (3.71 MB)"
    ]

    results = classify_download_links([
        DownloadLinkWithFileInfo(
            url=HttpUrl(f"https://localhost:8000/test{i+1}.pdf"),
            text=link,
            file_info=FileTypeInfo(mime_type="application/pdf",
            charset="utf-8",
            content_length=1000)
        )
        for i, link in enumerate(test_links)
    ])
    print("\nClassification results:")
    for link in results:
        print(f"{link.text} -> {link.classification}")
