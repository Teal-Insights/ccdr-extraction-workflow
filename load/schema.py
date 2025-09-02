from datetime import date, datetime, UTC
from typing import List, Optional, Dict, Any
from html import escape
from bs4 import BeautifulSoup
from enum import Enum
from sqlmodel import Field, Relationship, SQLModel, Column
from sqlalchemy import event
from sqlalchemy.orm import Session as SASession
from pydantic import HttpUrl, field_validator
from sqlalchemy.dialects.postgresql import ARRAY, FLOAT, JSONB
from sqlalchemy.orm import Mapped


def list_to_ranges(nums):
    """
    Helper to convert a list of numbers into a string of ranges.
    Used for the `data-pages` attribute on HTML elements.
    """
    if not nums:
        return ""
    
    # Sort the list to handle unsorted input
    nums = sorted(set(nums))  # Remove duplicates and sort
    
    ranges = []
    start = nums[0]
    end = nums[0]
    
    for i in range(1, len(nums)):
        if nums[i] == end + 1:
            # Continue the current range
            end = nums[i]
        else:
            # End the current range and start a new one
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = nums[i]
            end = nums[i]
    
    # Handle the last range
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")
    
    return ",".join(ranges)


# Enums for document and node types
class DocumentType(str, Enum):
    MAIN = "MAIN"
    SUPPLEMENTAL = "SUPPLEMENTAL"
    OTHER = "OTHER"


# TODO: Create helper methods to return tags of a given type, e.g., top-level, structural, headings, and inline styles
# TODO: Consider storing table children as plain text content
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


# TODO: Given that code can be its own node or an in-line style, we might want a None value for this to use when code is a child of p
# TODO: Similarly, we might skip embedding for children of table elements and add an ALL_CHILDREN value for the parent
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

    @property
    def citation(self) -> str:
        def cleaned(s: Optional[str]) -> Optional[str]:
            if s is None:
                return None
            s = s.strip()
            return s or None

        parts: List[str] = []

        authors = cleaned(self.authors)
        year = f"({self.publication_date.year})" if getattr(self, "publication_date", None) else None
        author_year = " ".join(p for p in (authors, year) if p)
        if author_year:
            parts.append(author_year + ".")

        title = cleaned(self.title)
        if title:
            parts.append(title + ".")

        source = cleaned(self.source)
        if source:
            parts.append(source + ".")

        # Prefer version from a MAIN document; fall back to any document with a version.
        version: Optional[str] = None
        docs = list(self.documents or [])
        if docs:
            main_with_version = next((d for d in docs if d.type == DocumentType.MAIN and cleaned(d.version)), None)
            any_with_version = next((d for d in docs if cleaned(d.version)), None)
            version = cleaned(main_with_version.version if main_with_version else (any_with_version.version if any_with_version else None))
        if version:
            parts.append(f"Version {version}.")

        url = cleaned(self.source_url) or cleaned(self.uri)
        if url:
            parts.append(url)

        return " ".join(parts).strip()


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

    def to_html(
        self,
        *,
        include_citation_data: bool = False,
        separator: str = "\n",
        include_html_wrapper: bool = False,
        pretty: bool = True,
    ) -> str:
        """Render the document as HTML, traversing nodes in DOM order.

        This preserves the hierarchical structure using each node's `tag_name` when present.
        Descriptions may be used as alt text for images, but are never emitted as plain text.
        """
        root_nodes: List["Node"] = sorted(
            (n for n in self.nodes if n.parent_id is None),
            key=lambda n: n.sequence_in_parent,
        )

        parts: List[str] = []
        for root in root_nodes:
            html_fragment = root.to_html(
                include_citation_data=include_citation_data,
                is_top_level=True,
                separator=separator,
                pretty=False,
            )
            if html_fragment:
                parts.append(html_fragment)

        body = separator.join(p for p in parts if p)
        result = f"<html>\n<body>\n{body}\n</body>\n</html>" if include_html_wrapper else body

        if pretty:
            soup = BeautifulSoup(result, "html.parser")
            return soup.prettify(formatter="html")
        return result


class Node(SQLModel, table=True):
    __table_args__ = {
        "comment": "Unified DOM node structure for both element and text nodes"
    }

    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    document_id: Optional[int] = Field(
        default=None, foreign_key="document.id", index=True, ondelete="CASCADE"
    )
    tag_name: TagName = Field(index=True)
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

    def to_html(
        self,
        *,
        include_citation_data: bool = False,
        is_top_level: bool = False,
        separator: str = "\n",
        pretty: bool = True,
    ) -> str:
        """Render this node and its subtree to HTML.

        - For element nodes with children, wrap children in the element tag when available.
        - For leaf nodes with `ContentData`, render within the element tag when available;
          otherwise return escaped text or element markup.
        - Captions are intentionally not rendered.
        """
        result: str
        # If the node has children, render the children in order
        def cleaned_string(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            v = value.strip()
            return v or None

        # Build attributes for this node render
        attr_parts: List[str] = []
        # Citation attributes only on top-level elements and only when a tag is present
        if include_citation_data and is_top_level and self.tag_name is not None:
            doc = self.document
            if doc is not None:
                pub = doc.publication
                if pub is not None:
                    authors = cleaned_string(getattr(pub, "authors", None))
                    if authors:
                        attr_parts.append(f'data-publication-authors="{escape(authors)}"')
                    title = cleaned_string(getattr(pub, "title", None))
                    if title:
                        attr_parts.append(f'data-publication-title="{escape(title)}"')
                    pub_date = getattr(pub, "publication_date", None)
                    if pub_date is not None:
                        attr_parts.append(f'data-publication-date="{pub_date.isoformat()}"')
                    source = cleaned_string(getattr(pub, "source", None))
                    if source:
                        attr_parts.append(f'data-publication-source="{escape(source)}"')
                    pub_url = cleaned_string(getattr(pub, "source_url", None)) or cleaned_string(getattr(pub, "uri", None))
                    if pub_url:
                        attr_parts.append(f'data-publication-url="{escape(pub_url)}"')
                doc_desc = cleaned_string(getattr(doc, "description", None))
                if doc_desc:
                    attr_parts.append(f'data-document-description="{escape(doc_desc)}"')

        # Pages attribute on any emitted element that has positional data
        pages: List[int] = []
        for pos in (self.positional_data or []):
            page_value = None
            if isinstance(pos, dict):
                page_value = pos.get("page_pdf")
            else:
                page_value = getattr(pos, "page_pdf", None)
            if page_value is not None:
                try:
                    pages.append(int(page_value))
                except (TypeError, ValueError):
                    continue
        if include_citation_data and pages:
            pages_str = list_to_ranges(pages)
            if pages_str:
                attr_parts.append(f'data-pages="{pages_str}"')

        attrs: str = (" " + " ".join(attr_parts)) if attr_parts else ""

        if self.children:
            ordered_children: List["Node"] = sorted(
                list(self.children), key=lambda n: n.sequence_in_parent
            )
            child_html: List[str] = [
                child.to_html(
                    include_citation_data=include_citation_data,
                    is_top_level=False,
                    separator=separator,
                    pretty=False,
                )
                for child in ordered_children
            ]
            children_joined = separator.join(s for s in child_html if s)

            if self.tag_name is not None:
                tag = self.tag_name.value
                result = f"<{tag}{attrs}>{children_joined}</{tag}>"
            else:
                # No tag name; return children concatenated
                result = children_joined
        else:
            # Leaf node: render from ContentData
            content = self.content_data
            if content is None:
                result = ""
            elif self.tag_name == TagName.IMG:
                # Special case for images
                src = content.storage_url or ""
                alt = content.description or ""
                # Self-contained img element; no caption rendering
                result = f"<img src=\"{escape(src)}\" alt=\"{escape(alt)}\"{attrs}/>"
            else:
                text_parts: List[str] = []
                if content.text_content:
                    text_parts.append(escape(content.text_content))

                text_html = separator.join(p for p in text_parts if p)

                if self.tag_name is not None:
                    tag = self.tag_name.value
                    result = f"<{tag}{attrs}>{text_html}</{tag}>"
                else:
                    # No tag name; default to span for inline/leaf content
                    result = f"<span{attrs}>{text_html}</span>"

        if pretty:
            soup = BeautifulSoup(f"<div>{result}</div>", "html.parser")
            div = soup.div
            parts: List[str] = []
            for child in div.contents:
                if hasattr(child, "prettify"):
                    parts.append(child.prettify(formatter="html"))
                else:
                    parts.append(str(child))
            return "".join(parts)
        return result


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

    @property
    def document_id(self) -> Optional[int]:
        if self.node is None:
            return None
        return self.node.document_id


def _has_nonempty_text(value: Optional[str]) -> bool:
    """Return True if the provided string has non-whitespace content."""
    if value is None:
        return False
    return value.strip() != ""


def ensure_description_caption_allowed(
    node_tag: Optional[TagName],
    description: Optional[str],
    caption: Optional[str],
) -> None:
    """Enforce that description/caption only exist for IMG or TABLE nodes.

    This is an application-level rule that we cannot express as a single
    database CHECK constraint because it involves a cross-table relationship
    (ContentData -> Node.tag_name).
    """
    if not (_has_nonempty_text(description) or _has_nonempty_text(caption)):
        return
    if node_tag not in (TagName.IMG, TagName.TABLE):
        raise ValueError(
            "ContentData.description and caption are only allowed when the linked Node.tag_name is IMG or TABLE."
        )


@event.listens_for(SASession, "before_flush")
def _validate_contentdata_fields(
    session: SASession, flush_context, instances
) -> None:
    """Session hook to enforce ContentData description/caption constraints.

    This runs for both new and updated rows, regardless of how they are created.
    """
    # Collect candidates from new and dirty instances
    candidates = list(getattr(session, "new", ())) + list(getattr(session, "dirty", ()))
    for obj in candidates:
        if isinstance(obj, ContentData):
            node = obj.node
            if node is None and getattr(obj, "node_id", None) is not None:
                # Fallback to load node if relationship not populated
                node = session.get(Node, obj.node_id)
            node_tag: Optional[TagName] = getattr(node, "tag_name", None) if node is not None else None
            ensure_description_caption_allowed(node_tag, obj.description, obj.caption)


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

    @property
    def document_id(self) -> Optional[int]:
        if self.content_data is None:
            return None
        node = self.content_data.node
        if node is None:
            return None
        return node.document_id
