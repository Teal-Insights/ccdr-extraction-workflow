import logging
from typing import List
from pydantic import BaseModel, Field
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

############################################
# Pydantic models for input image data structures
############################################
class ImageRegion(BaseModel):
    image_path: str
    label: str
    bbox_normalized: List[int]
    bbox_pixels: List[int]
    description: str

class ImagePage(BaseModel):
    page_number: int
    regions: List[ImageRegion]

class ImageDocument(BaseModel):
    dl_id: str
    pages: List[ImagePage]

class ExtractedImageContent(BaseModel):
    """Root model containing all documents."""
    documents: List[ImageDocument] = Field(default_factory=list, description="List of all documents")

############################################
# Pydantic models for input text data structures
############################################
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

class ExtractedTextContent(BaseModel):
    """Root model containing all publications."""
    publications: List[TextPublication] = Field(default_factory=list, description="List of all publications")

############################################
# Output content node data structures
############################################
class ContentNode(BaseModel):
    """Represents a content node from a document."""
    id: int = Field(..., description="Unique node identifier")
    document_id: str = Field(..., description="Foreign key to document (dl_XXX)")
    node_type: str = Field(..., description="Type (HEADING, PARAGRAPH, TABLE, IMAGE)")
    raw_content: str | None = Field(None, description="Optional original text content of the node")
    content: str | None = Field(None, description="Optional cleaned text content of the node")
    storage_url: str | None = Field(None, description="Optional URL to the node storage bucket (s3://...)")
    caption: str | None = Field(None, description="Optional original caption for the node (image, table, etc.)")
    description: str | None = Field(None, description="Optional VLM description of the node (image, table, etc.)")
    sequence_in_document: int = Field(..., description="Sequence number in the document")
    start_page_pdf: int = Field(..., description="Start page of the node in the PDF")
    end_page_pdf: int = Field(..., description="End page of the node in the PDF")
    start_page_logical: str = Field(..., description="Numbered start page of the node")
    end_page_logical: str = Field(..., description="Numbered end page of the node")
    bounding_box: dict = Field(..., description="Bounding box of the node in the document")
