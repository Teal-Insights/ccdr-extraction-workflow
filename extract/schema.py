from datetime import date, datetime, UTC
from typing import List, Optional, Dict, Any
from enum import Enum
from dotenv import load_dotenv
from sqlmodel import Field, Relationship, SQLModel, Column
from pydantic import HttpUrl, field_validator
from sqlalchemy.dialects.postgresql import ARRAY, FLOAT, JSONB

# Load environment variables
load_dotenv(override=True)

# Enums for document and node types
class DocumentType(str, Enum):
    MAIN = "MAIN"
    SUPPLEMENTAL = "SUPPLEMENTAL"
    OTHER = "OTHER"

class NodeType(str, Enum):
    HEADING = "HEADING"
    PARAGRAPH = "PARAGRAPH"
    TABLE = "TABLE"
    IMAGE = "IMAGE"

# Pydantic model for bounding box
class BoundingBox(SQLModel):
    x1: float
    y1: float
    x2: float
    y2: float

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        return {
            "x1": self.x1,
            "y1": self.y1,
            "x2": self.x2,
            "y2": self.y2
        }

# Define the models
class Publication(SQLModel, table=True):
    __table_args__ = {'comment': 'Contains publication metadata and relationships to documents'}
    
    id: str = Field(primary_key=True, max_length=50, index=True)
    title: str = Field(max_length=500)
    abstract: Optional[str] = None
    citation: str
    authors: str
    publication_date: date = Field(index=True)
    source: str = Field(max_length=100)
    source_url: str = Field(max_length=500)
    uri: str = Field(max_length=500)
    
    # Validators for URLs
    @field_validator('source_url', 'uri')
    @classmethod
    def validate_url(cls, v: str) -> str:
        # Validate the URL format but return as string
        HttpUrl(v)
        return v
    
    # Relationships
    documents: List["Document"] = Relationship(back_populates="publication")


class Document(SQLModel, table=True):
    __table_args__ = {'comment': 'Contains document metadata and relationships to content nodes'}
    
    id: str = Field(primary_key=True, max_length=50, index=True)
    publication_id: str = Field(foreign_key="publication.id", index=True)
    type: DocumentType
    download_url: str = Field(max_length=500)
    description: str
    mime_type: str = Field(max_length=100)
    charset: str = Field(max_length=50)
    storage_url: Optional[str] = Field(default=None, max_length=500)
    file_size: Optional[int] = None
    
    # Validator for URL
    @field_validator('download_url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        # Validate the URL format but return as string
        HttpUrl(v)
        return v

    @field_validator('storage_url')
    @classmethod
    def validate_optional_url(cls, v: Optional[str]) -> Optional[str]:
        # Validate the URL format but return as string
        if v is not None:
            HttpUrl(v)
        return v

    # Relationships
    publication: Publication = Relationship(back_populates="documents")
    content_nodes: List["ContentNode"] = Relationship(back_populates="document")


class ContentNode(SQLModel, table=True):
    __tablename__ = "content_node" # type: ignore
    __table_args__ = {'comment': 'Contains hierarchical document content with relationships to embeddings and footnotes'}
    
    id: str = Field(primary_key=True, max_length=50, index=True)
    document_id: str = Field(foreign_key="document.id", index=True)
    parent_node_id: Optional[str] = Field(default=None, foreign_key="content_node.id", index=True)
    node_type: NodeType
    raw_content: Optional[str] = None
    content: Optional[str] = None
    storage_url: Optional[str] = Field(default=None, max_length=500)
    caption: Optional[str] = None
    description: Optional[str] = None
    sequence_in_parent: int
    sequence_in_document: int = Field(index=True)
    start_page_pdf: int
    end_page_pdf: int
    start_page_logical: str
    end_page_logical: str
    bounding_box: Dict[str, Any] = Field(sa_column=Column(JSONB))

    # Validator for optional URL
    @field_validator('storage_url')
    @classmethod
    def validate_optional_url(cls, v: Optional[str]) -> Optional[str]:
        # Validate the URL format but return as string
        if v is not None:
            HttpUrl(v)
        return v

    # Relationships
    document: Document = Relationship(back_populates="content_nodes")
    parent: Optional["ContentNode"] = Relationship(
        back_populates="children",
        sa_relationship_kwargs={"remote_side": "ContentNode.id"}
    )
    children: List["ContentNode"] = Relationship(back_populates="parent")
    embeddings: List["Embedding"] = Relationship(back_populates="content_node")
    referencing_footnotes: List["FootnoteReference"] = Relationship(
        back_populates="referencing_node",
        sa_relationship_kwargs={"foreign_keys": "FootnoteReference.referencing_node_id"}
    )
    definition_footnotes: List["FootnoteReference"] = Relationship(
        back_populates="definition_node",
        sa_relationship_kwargs={"foreign_keys": "FootnoteReference.definition_node_id"}
    )


class Embedding(SQLModel, table=True):
    __table_args__ = {'comment': 'Contains vector embeddings for content nodes'}
    
    id: str = Field(primary_key=True, max_length=50, index=True)
    node_id: str = Field(foreign_key="content_node.id", index=True)
    embedding_vector: List[float] = Field(
        sa_column=Column(ARRAY(FLOAT))
    )
    model_name: str = Field(max_length=100)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    
    # Relationships
    content_node: ContentNode = Relationship(back_populates="embeddings")


class FootnoteReference(SQLModel, table=True):
    __tablename__ = "footnote_reference" # type: ignore
    __table_args__ = {'comment': 'Contains bidirectional footnote references between content nodes'}

    id: str = Field(primary_key=True, max_length=50, index=True)
    referencing_node_id: str = Field(foreign_key="content_node.id", index=True)
    definition_node_id: str = Field(foreign_key="content_node.id", index=True)
    marker_text: str = Field(max_length=50)
    sequence_in_node: int

    # Relationships
    referencing_node: ContentNode = Relationship(
        back_populates="referencing_footnotes",
        sa_relationship_kwargs={"foreign_keys": "FootnoteReference.referencing_node_id"}
    )
    definition_node: ContentNode = Relationship(
        back_populates="definition_footnotes",
        sa_relationship_kwargs={"foreign_keys": "FootnoteReference.definition_node_id"}
    )
