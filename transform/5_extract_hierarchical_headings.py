import json
import re
import logging
from typing import List, Dict, Optional
from pydantic import BaseModel, Field

# --- Pydantic Models (for text_content.json structure) ---
class TextPage(BaseModel):
    """Represents a single page from a PDF document."""
    page_number: int = Field(..., description="1-based page number")
    text_content: str = Field(..., description="Extracted text content from the page")

class TextDocument(BaseModel):
    """Represents a document with multiple pages."""
    doc_id: str = Field(..., description="Unique document identifier")
    pages: List[TextPage] = Field(default_factory=list, description="List of pages in the document")

class TextPublication(BaseModel):
    """Represents a publication containing multiple documents."""
    pub_id: str = Field(..., description="Unique publication identifier")
    documents: List[TextDocument] = Field(default_factory=list, description="List of documents in the publication")

# --- Pydantic Models (for sections.json structure) ---
class SectionPageRange(BaseModel):
    start_page: int
    end_page: int

class DocSections(BaseModel): # From sections.json values
    front_matter: Optional[SectionPageRange] = None
    contents: Optional[SectionPageRange] = None
    list_of_figures: Optional[SectionPageRange] = None
    list_of_tables: Optional[SectionPageRange] = None
    list_of_boxes: Optional[SectionPageRange] = None # Will be None if not in source JSON
    body: Optional[SectionPageRange] = None
    references: Optional[SectionPageRange] = None
    end_notes: Optional[SectionPageRange] = None
    annexes: Optional[SectionPageRange] = None
    
    class Config:
        extra = "ignore" # Ignore any other fields from sections.json

# --- Pydantic Models for Output (hierarchical_headings.json) ---
class TocHeading(BaseModel):
    level: int
    title: str
    page_number: int

class FigureInfo(BaseModel):
    id: Optional[str] = None
    caption: str
    page_number: int

class TableInfo(BaseModel):
    id: Optional[str] = None
    title: str
    page_number: int

class BoxInfo(BaseModel):
    id: Optional[str] = None
    title: str
    page_number: int

class HierarchicalDocHeadings(BaseModel):
    document_title: Optional[str] = None
    toc_headings: List[TocHeading] = Field(default_factory=list)
    figures: List[FigureInfo] = Field(default_factory=list)
    tables: List[TableInfo] = Field(default_factory=list)
    boxes: List[BoxInfo] = Field(default_factory=list)

# --- Global Paths ---
TEXT_CONTENT_FILE = "transform/text/text_content.json"
SECTIONS_FILE = "transform/text/sections.json"
OUTPUT_FILE = "transform/text/hierarchical_headings.json"

# --- Logger ---
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --- Helper function to load text_content.json and provide quick page access ---
def load_text_data(file_path: str) -> Dict[str, TextDocument]:
    docs_map: Dict[str, TextDocument] = {}
    try:
        with open(file_path, 'r') as f:
            publications_data = json.load(f)
        
        # Assuming publications_data is List[Dict]
        for pub_data_dict in publications_data:
            try:
                # The root of text_content.json is a list of publications, not an object with a "publications" key
                publication = TextPublication.model_validate(pub_data_dict)
                for doc in publication.documents:
                    docs_map[doc.doc_id] = doc
            except Exception as e:
                logger.error(f"Error validating publication data: {pub_data_dict.get('pub_id', 'Unknown Pub')}: {e}")
                continue # Skip this publication
    except FileNotFoundError:
        logger.error(f"Text content file not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from text content file: {file_path}")
        raise
    return docs_map

def get_text_for_page_range(doc: TextDocument, start_page: int, end_page: int) -> List[str]:
    page_texts: List[str] = []
    page_map = {page.page_number: page.text_content for page in doc.pages}
    for i in range(start_page, end_page + 1):
        if i in page_map:
            page_texts.append(page_map[i])
        else:
            logger.warning(f"Page {i} not found in document {doc.doc_id} (requested range {start_page}-{end_page})")
    return page_texts

# --- Parsing Functions ---
TOC_RE = re.compile(r"^\s*(\d+(?:\.\d+)*)\.?\s+(.+?)\s*(\d+)\s*$")
FIGURE_RE = re.compile(r"^\s*Figure\s+([\w\.\-]+)\s*[:\.]?\s*(.+?)\s*(\d+)\s*$", re.IGNORECASE)
TABLE_RE = re.compile(r"^\s*Table\s+([\w\.\-]+)\s*[:\.]?\s*(.+?)\s*(\d+)\s*$", re.IGNORECASE)
BOX_RE = re.compile(r"^\s*Box\s+([\w\.\-]+)\s*[:\.]?\s*(.+?)\s*(\d+)\s*$", re.IGNORECASE)

def parse_toc(page_texts: List[str]) -> List[TocHeading]:
    headings: List[TocHeading] = []
    full_text = "\\n".join(page_texts)
    for line in full_text.splitlines():
        line = line.strip() # Process individual lines
        match = TOC_RE.match(line)
        if match:
            num_str, title, page_num_str = match.groups()
            try:
                level = num_str.count('.') + 1
                page_number = int(page_num_str)
                headings.append(TocHeading(level=level, title=title.strip(), page_number=page_number))
            except ValueError:
                logger.warning(f"Could not parse page number in ToC line: '{line}'")
        # else: logger.debug(f"No ToC match for line: '{line}'") # For debugging non-matches
    return headings

def _parse_generic_list_items(page_texts: List[str], item_re: re.Pattern, item_class: type) -> list:
    items: list = []
    full_text = "\\n".join(page_texts)
    for line in full_text.splitlines():
        line = line.strip()
        match = item_re.match(line)
        if match:
            id_str, text_str, page_num_str = match.groups()
            try:
                page_number = int(page_num_str)
                # Determine if it's FigureInfo (caption) or TableInfo/BoxInfo (title)
                if item_class == FigureInfo:
                    items.append(item_class(id=id_str.strip(), caption=text_str.strip(), page_number=page_number))
                else: # TableInfo, BoxInfo
                    items.append(item_class(id=id_str.strip(), title=text_str.strip(), page_number=page_number))
            except ValueError:
                logger.warning(f"Could not parse page number in {item_class.__name__} line: '{line}'")
    return items

def parse_figures(page_texts: List[str]) -> List[FigureInfo]:
    return _parse_generic_list_items(page_texts, FIGURE_RE, FigureInfo)

def parse_tables(page_texts: List[str]) -> List[TableInfo]:
    return _parse_generic_list_items(page_texts, TABLE_RE, TableInfo)

def parse_boxes(page_texts: List[str]) -> List[BoxInfo]:
    return _parse_generic_list_items(page_texts, BOX_RE, BoxInfo)

def extract_document_title(doc: TextDocument, doc_sections: Optional[DocSections]) -> Optional[str]:
    if not doc_sections:
        logger.warning(f"No section data to extract title for doc {doc.doc_id}")
        return None

    page_to_scan_num = -1
    if doc_sections.front_matter and doc_sections.front_matter.start_page is not None:
        page_to_scan_num = doc_sections.front_matter.start_page
    elif doc_sections.body and doc_sections.body.start_page is not None:
        page_to_scan_num = doc_sections.body.start_page
    
    if page_to_scan_num == -1:
        if doc.pages: # Fallback to the first page of the document if no specific section start page
            page_to_scan_num = doc.pages[0].page_number
            logger.info(f"No front_matter/body start_page for title, using first page {page_to_scan_num} of doc {doc.doc_id}")
        else:
            logger.warning(f"Doc {doc.doc_id} has no pages to extract title from.")
            return None
                
    page_map = {page.page_number: page.text_content for page in doc.pages}
    if page_to_scan_num in page_map:
        text_content = page_map[page_to_scan_num]
        for line in text_content.splitlines():
            stripped_line = line.strip()
            if stripped_line: # First non-empty line
                if 2 < len(stripped_line) < 150 and not stripped_line.isdigit(): # Basic heuristic for a title
                    return stripped_line
        logger.warning(f"Could not find a suitable title line on page {page_to_scan_num} for doc {doc.doc_id}.")
        return None
    else:
        logger.warning(f"Page {page_to_scan_num} (for title extraction) not found in doc {doc.doc_id}.")
        return None

# --- Main Processing Logic ---
def main():
    try:
        logger.info(f"Loading text content from {TEXT_CONTENT_FILE}")
        all_docs_data = load_text_data(TEXT_CONTENT_FILE) # Dict[doc_id, TextDocument]
    except Exception as e:
        logger.error(f"Failed to load text content: {e}. Exiting.")
        return

    if not all_docs_data:
        logger.error(f"No documents loaded from {TEXT_CONTENT_FILE}. Exiting.")
        return
        
    logger.info(f"Loading section page ranges from {SECTIONS_FILE}")
    try:
        with open(SECTIONS_FILE, 'r') as f:
            sections_json_data = json.load(f) # This is Dict[str, Dict]
        
        parsed_sections_data: Dict[str, DocSections] = {}
        for doc_id, sec_data_dict in sections_json_data.items():
            try:
                parsed_sections_data[doc_id] = DocSections.model_validate(sec_data_dict)
            except Exception as e:
                logger.error(f"Failed to validate sections for doc_id {doc_id}: {e}. Section data: {sec_data_dict}. Skipping this doc.")
    except FileNotFoundError:
        logger.error(f"{SECTIONS_FILE} not found. Exiting.")
        return
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {SECTIONS_FILE}. Exiting.")
        return

    all_hierarchical_headings: Dict[str, HierarchicalDocHeadings] = {}
    doc_ids_to_process = parsed_sections_data.keys()

    for doc_id in doc_ids_to_process:
        if doc_id not in all_docs_data:
            logger.warning(f"Document ID {doc_id} from {SECTIONS_FILE} not found in {TEXT_CONTENT_FILE}. Skipping.")
            continue

        current_doc_text_data = all_docs_data[doc_id]
        current_doc_section_ranges = parsed_sections_data.get(doc_id) 

        if not current_doc_section_ranges: # Should be caught by iteration logic, but good for safety
             logger.warning(f"No parsed section data for {doc_id} though it was in keys. Skipping.")
             continue

        logger.info(f"Processing document: {doc_id}")
        doc_output = HierarchicalDocHeadings()
        doc_output.document_title = extract_document_title(current_doc_text_data, current_doc_section_ranges)

        # Table of Contents
        if current_doc_section_ranges.contents:
            toc_pages_text = get_text_for_page_range(
                current_doc_text_data,
                current_doc_section_ranges.contents.start_page,
                current_doc_section_ranges.contents.end_page
            )
            if toc_pages_text:
                doc_output.toc_headings = parse_toc(toc_pages_text)
        
        # List of Figures
        if current_doc_section_ranges.list_of_figures:
            lof_pages_text = get_text_for_page_range(
                current_doc_text_data,
                current_doc_section_ranges.list_of_figures.start_page,
                current_doc_section_ranges.list_of_figures.end_page
            )
            if lof_pages_text:
                doc_output.figures = parse_figures(lof_pages_text)

        # List of Tables
        if current_doc_section_ranges.list_of_tables:
            lot_pages_text = get_text_for_page_range(
                current_doc_text_data,
                current_doc_section_ranges.list_of_tables.start_page,
                current_doc_section_ranges.list_of_tables.end_page
            )
            if lot_pages_text:
                doc_output.tables = parse_tables(lot_pages_text)
        
        # List of Boxes
        if current_doc_section_ranges.list_of_boxes:
            lob_pages_text = get_text_for_page_range(
                current_doc_text_data,
                current_doc_section_ranges.list_of_boxes.start_page,
                current_doc_section_ranges.list_of_boxes.end_page
            )
            if lob_pages_text:
                doc_output.boxes = parse_boxes(lob_pages_text)
        
        all_hierarchical_headings[doc_id] = doc_output

    logger.info(f"Saving hierarchical headings to {OUTPUT_FILE}")
    output_dict_for_json = {doc_id: data.model_dump() for doc_id, data in all_hierarchical_headings.items()}
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_dict_for_json, f, indent=2)
    
    logger.info(f"Successfully processed {len(all_hierarchical_headings)} documents. Output saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main() 