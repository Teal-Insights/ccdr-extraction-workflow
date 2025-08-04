from datetime import date, datetime, UTC
from typing import List, Optional, Dict, Any
from enum import Enum
from sqlmodel import Field, Relationship, SQLModel, Column
from pydantic import HttpUrl, field_validator
from sqlalchemy.dialects.postgresql import ARRAY, FLOAT, JSONB
from sqlalchemy.orm import Mapped


# Enums for document and node types
class DocumentType(str, Enum):
    MAIN = "MAIN"
    SUPPLEMENTAL = "SUPPLEMENTAL"
    OTHER = "OTHER"


class NodeType(str, Enum):
    TEXT_NODE = "TEXT_NODE"
    ELEMENT_NODE = "ELEMENT_NODE"


# TODO: Create helper methods to return tags of a given type, e.g., top-level, structural, headings, etc.
class TagName(str, Enum):
    # Only structural elements
    HEADER = "header"
    MAIN = "main"
    FOOTER = "footer"
    FIGURE = "figure"
    FIGCAPTION = "figcaption"
    TABLE = "table"
    THEAD = "thead"
    TBODY = "tbody"
    TFOOT = "tfoot"
    TH = "th"
    TR = "tr"
    TD = "td"
    CAPTION = "caption"
    SECTION = "section"
    NAV = "nav"
    ASIDE = "aside"
    P = "p"
    UL = "ul"
    OL = "ol"
    LI = "li"
    H1 = "h1"
    H2 = "h2"
    H3 = "h3"
    H4 = "h4"
    H5 = "h5"
    H6 = "h6"
    IMG = "img"
    MATH = "math"
    CODE = "code"
    CITE = "cite"
    BLOCKQUOTE = "blockquote"


class SectionType(str, Enum):
    ABSTRACT = "ABSTRACT"
    ACKNOWLEDGEMENTS = "ACKNOWLEDGEMENTS"
    APPENDIX = "APPENDIX"
    BIBLIOGRAPHY = "BIBLIOGRAPHY"
    CHAPTER = "CHAPTER"
    CONCLUSION = "CONCLUSION"
    COPYRIGHT_PAGE = "COPYRIGHT_PAGE"
    DEDICATION = "DEDICATION"
    EPILOGUE = "EPILOGUE"
    EXECUTIVE_SUMMARY = "EXECUTIVE_SUMMARY"
    FOOTER = "FOOTER"
    FOREWORD = "FOREWORD"
    HEADER = "HEADER"
    INDEX = "INDEX"
    INTRODUCTION = "INTRODUCTION"
    LIST_OF_BOXES = "LIST_OF_BOXES"
    LIST_OF_FIGURES = "LIST_OF_FIGURES"
    LIST_OF_TABLES = "LIST_OF_TABLES"
    NOTES_SECTION = "NOTES_SECTION"
    PART = "PART"
    PREFACE = "PREFACE"
    PROLOGUE = "PROLOGUE"
    SECTION = "SECTION"
    STANZA = "STANZA"
    SUBSECTION = "SUBSECTION"
    TABLE_OF_CONTENTS = "TABLE_OF_CONTENTS"
    TEXT_BOX = "TEXT_BOX"
    TITLE_PAGE = "TITLE_PAGE"


class EmbeddingSource(str, Enum):
    TEXT_CONTENT = "TEXT_CONTENT"
    DESCRIPTION = "DESCRIPTION"
    CAPTION = "CAPTION"


class RelationType(str, Enum):
    REFERENCES_NOTE = "REFERENCES_NOTE"
    REFERENCES_CITATION = "REFERENCES_CITATION"
    IS_CAPTIONED_BY = "IS_CAPTIONED_BY"
    IS_SUPPLEMENTED_BY = "IS_SUPPLEMENTED_BY"
    CONTINUES = "CONTINUES"
    CROSS_REFERENCES = "CROSS_REFERENCES"


class BoundingBox(SQLModel):
    """Model for bounding box coordinates with serialization support."""
    x1: float
    y1: float
    x2: float
    y2: float


# Pydantic model for positional data
class PositionalData(SQLModel, table=False):
    """Represents the position of *one* of the bounding boxes
    that make up a node. Intended for storage in a JSONB array
    containing complete positional data for the node.

    Most nodes will have only one bounding box, but some will
    have multiple (e.g., paragraphs split across pages).
    """

    page_pdf: int
    page_logical: Optional[str] = None  # str to support roman, alpha, etc.
    bbox: BoundingBox

    @field_validator('page_logical', mode='before')
    @classmethod
    def convert_page_logical_to_string(cls, v):
        """Convert integer page_logical values to strings for backward compatibility."""
        if v is not None and not isinstance(v, str):
            return str(v)
        return v

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        return {
            "page_pdf": self.page_pdf,
            "page_logical": self.page_logical,
            "bbox": self.bbox.model_dump(),
        }


# Define the models
class Publication(SQLModel, table=True):
    __table_args__ = {
        "comment": "Contains publication metadata and relationships to documents"
    }

    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    title: str = Field(max_length=500)
    abstract: Optional[str] = None
    citation: str
    authors: str
    publication_date: date = Field(index=True)
    source: str = Field(max_length=100)
    source_url: str = Field(max_length=500)
    uri: str = Field(max_length=500)

    # Validators for URLs
    @field_validator("source_url", "uri")
    @classmethod
    def validate_url(cls, v: str) -> str:
        # Validate the URL format but return as string
        HttpUrl(v)
        return v

    # Relationships
    documents: Mapped[List["Document"]] = Relationship(
        back_populates="publication", cascade_delete=True
    )


class Document(SQLModel, table=True):
    __table_args__ = {
        "comment": "Contains document metadata and relationships to nodes"
    }

    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    publication_id: Optional[int] = Field(
        default=None, foreign_key="publication.id", index=True, ondelete="CASCADE"
    )
    type: DocumentType
    download_url: str = Field(max_length=500)
    description: str
    mime_type: str = Field(max_length=100)
    charset: str = Field(max_length=50)
    storage_url: Optional[str] = Field(default=None, max_length=500)
    file_size: Optional[int] = None
    language: Optional[str] = Field(default=None, max_length=10)
    version: Optional[str] = Field(default=None, max_length=50)

    # Validator for URL
    @field_validator("download_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        # Validate the URL format but return as string
        HttpUrl(v)
        return v

    @field_validator("storage_url")
    @classmethod
    def validate_optional_url(cls, v: Optional[str]) -> Optional[str]:
        # Validate the URL format but return as string
        if v is not None:
            HttpUrl(v)
        return v

    # Relationships
    publication: Mapped[Optional[Publication]] = Relationship(
        back_populates="documents"
    )
    nodes: Mapped[List["Node"]] = Relationship(
        back_populates="document", cascade_delete=True
    )


class Node(SQLModel, table=True):
    __table_args__ = {
        "comment": "Unified DOM node structure for both element and text nodes"
    }

    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    document_id: Optional[int] = Field(
        default=None, foreign_key="document.id", index=True, ondelete="CASCADE"
    )
    node_type: NodeType
    tag_name: Optional[TagName] = Field(default=None, index=True)
    section_type: Optional[SectionType] = Field(default=None, index=True)
    parent_id: Optional[int] = Field(
        default=None, foreign_key="node.id", index=True, ondelete="CASCADE"
    )
    sequence_in_parent: int
    positional_data: List[Dict[str, Any]] = Field(
        default_factory=list, sa_column=Column(JSONB)
    )

    # Relationships
    document: Mapped[Optional[Document]] = Relationship(back_populates="nodes")
    parent: Mapped[Optional["Node"]] = Relationship(
        back_populates="children", sa_relationship_kwargs={"remote_side": "Node.id"}
    )
    children: Mapped[List["Node"]] = Relationship(
        back_populates="parent", cascade_delete=True
    )
    content_data: Mapped[Optional["ContentData"]] = Relationship(
        back_populates="node", cascade_delete=True
    )
    source_relations: Mapped[List["Relation"]] = Relationship(
        back_populates="source_node",
        sa_relationship_kwargs={"foreign_keys": "Relation.source_node_id"},
        cascade_delete=True,
    )
    target_relations: Mapped[List["Relation"]] = Relationship(
        back_populates="target_node",
        sa_relationship_kwargs={"foreign_keys": "Relation.target_node_id"},
        cascade_delete=True,
    )


class ContentData(SQLModel, table=True):
    __table_args__ = {"comment": "Contains actual content for content-bearing nodes"}

    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    node_id: int = Field(
        foreign_key="node.id", index=True, unique=True, ondelete="CASCADE"
    )
    text_content: Optional[str] = None
    storage_url: Optional[str] = Field(default=None, max_length=500)
    description: Optional[str] = None
    caption: Optional[str] = None
    embedding_source: EmbeddingSource

    # Validator for optional URL
    @field_validator("storage_url")
    @classmethod
    def validate_optional_url(cls, v: Optional[str]) -> Optional[str]:
        # Validate the URL format but return as string
        if v is not None:
            HttpUrl(v)
        return v

    # Relationships
    node: Mapped[Node] = Relationship(back_populates="content_data")
    embeddings: Mapped[List["Embedding"]] = Relationship(
        back_populates="content_data", cascade_delete=True
    )


class Relation(SQLModel, table=True):
    __table_args__ = {
        "comment": "Contains non-hierarchical relationships between nodes"
    }

    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    source_node_id: int = Field(foreign_key="node.id", index=True, ondelete="CASCADE")
    target_node_id: int = Field(foreign_key="node.id", index=True, ondelete="CASCADE")
    relation_type: RelationType

    # Relationships
    source_node: Mapped[Node] = Relationship(
        back_populates="source_relations",
        sa_relationship_kwargs={"foreign_keys": "Relation.source_node_id"},
    )
    target_node: Mapped[Node] = Relationship(
        back_populates="target_relations",
        sa_relationship_kwargs={"foreign_keys": "Relation.target_node_id"},
    )


class Embedding(SQLModel, table=True):
    __table_args__ = {"comment": "Contains vector embeddings for content data"}

    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    content_data_id: Optional[int] = Field(
        default=None, foreign_key="contentdata.id", index=True, ondelete="CASCADE"
    )
    embedding_vector: List[float] = Field(sa_column=Column(ARRAY(FLOAT)))
    model_name: str = Field(max_length=100)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # Relationships
    content_data: Mapped[Optional[ContentData]] = Relationship(
        back_populates="embeddings"
    )
