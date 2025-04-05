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
# Output content node data structures
############################################
class ContentNode(BaseModel):
    """Represents a content node from a document."""
    id: int = Field(..., description="Unique node identifier")
    document_id: str = Field(..., description="Foreign key to document (dl_XXX)")
    parent_node_id: int | None = Field(None, description="Foreign key to parent node")
    node_type: str = Field(..., description="Type (HEADING, PARAGRAPH, TABLE, IMAGE)")
    raw_content: str | None = Field(None, description="Optional original text content of the node")
    content: str | None = Field(None, description="Optional cleaned text content of the node")
    storage_url: str | None = Field(None, description="Optional URL to the node storage bucket (s3://...)")
    caption: str | None = Field(None, description="Optional original caption for the node (image, table, etc.)")
    description: str | None = Field(None, description="Optional VLM description of the node (image, table, etc.)")
    sequence_in_parent: int = Field(..., description="Sequence number in the parent node")
    sequence_in_document: int = Field(..., description="Sequence number in the document")
    start_page_pdf: int = Field(..., description="Start page of the node in the PDF")
    end_page_pdf: int = Field(..., description="End page of the node in the PDF")
    start_page_logical: str = Field(..., description="Numbered start page of the node")
    end_page_logical: str = Field(..., description="Numbered end page of the node")
    bounding_box: dict = Field(..., description="Bounding box of the node in the document")
