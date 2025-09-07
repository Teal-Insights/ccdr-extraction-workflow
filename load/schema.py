from datetime import date, datetime, UTC
from typing import List, Optional, Dict, Any, Iterable
from html import escape
from bs4 import BeautifulSoup
from enum import Enum
from sqlmodel import Field, Relationship, SQLModel, Column
from sqlmodel import Session
from sqlalchemy import event
from sqlalchemy.orm import Session as SASession
from pydantic import HttpUrl, field_validator
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from pgvector.sqlalchemy import Vector


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


class ISO3Country(Enum):
    # Generated from pycountry.countries
    ABW = "ABW"
    AFG = "AFG"
    AGO = "AGO"
    AIA = "AIA"
    ALA = "ALA"
    ALB = "ALB"
    AND = "AND"
    ARE = "ARE"
    ARG = "ARG"
    ARM = "ARM"
    ASM = "ASM"
    ATA = "ATA"
    ATF = "ATF"
    ATG = "ATG"
    AUS = "AUS"
    AUT = "AUT"
    AZE = "AZE"
    BDI = "BDI"
    BEL = "BEL"
    BEN = "BEN"
    BES = "BES"
    BFA = "BFA"
    BGD = "BGD"
    BGR = "BGR"
    BHR = "BHR"
    BHS = "BHS"
    BIH = "BIH"
    BLM = "BLM"
    BLR = "BLR"
    BLZ = "BLZ"
    BMU = "BMU"
    BOL = "BOL"
    BRA = "BRA"
    BRB = "BRB"
    BRN = "BRN"
    BTN = "BTN"
    BVT = "BVT"
    BWA = "BWA"
    CAF = "CAF"
    CAN = "CAN"
    CCK = "CCK"
    CHE = "CHE"
    CHL = "CHL"
    CHN = "CHN"
    CIV = "CIV"
    CMR = "CMR"
    COD = "COD"
    COG = "COG"
    COK = "COK"
    COL = "COL"
    COM = "COM"
    CPV = "CPV"
    CRI = "CRI"
    CUB = "CUB"
    CUW = "CUW"
    CXR = "CXR"
    CYM = "CYM"
    CYP = "CYP"
    CZE = "CZE"
    DEU = "DEU"
    DJI = "DJI"
    DMA = "DMA"
    DNK = "DNK"
    DOM = "DOM"
    DZA = "DZA"
    ECU = "ECU"
    EGY = "EGY"
    ERI = "ERI"
    ESH = "ESH"
    ESP = "ESP"
    EST = "EST"
    ETH = "ETH"
    FIN = "FIN"
    FJI = "FJI"
    FLK = "FLK"
    FRA = "FRA"
    FRO = "FRO"
    FSM = "FSM"
    GAB = "GAB"
    GBR = "GBR"
    GEO = "GEO"
    GGY = "GGY"
    GHA = "GHA"
    GIB = "GIB"
    GIN = "GIN"
    GLP = "GLP"
    GMB = "GMB"
    GNB = "GNB"
    GNQ = "GNQ"
    GRC = "GRC"
    GRD = "GRD"
    GRL = "GRL"
    GTM = "GTM"
    GUF = "GUF"
    GUM = "GUM"
    GUY = "GUY"
    HKG = "HKG"
    HMD = "HMD"
    HND = "HND"
    HRV = "HRV"
    HTI = "HTI"
    HUN = "HUN"
    IDN = "IDN"
    IMN = "IMN"
    IND = "IND"
    IOT = "IOT"
    IRL = "IRL"
    IRN = "IRN"
    IRQ = "IRQ"
    ISL = "ISL"
    ISR = "ISR"
    ITA = "ITA"
    JAM = "JAM"
    JEY = "JEY"
    JOR = "JOR"
    JPN = "JPN"
    KAZ = "KAZ"
    KEN = "KEN"
    KGZ = "KGZ"
    KHM = "KHM"
    KIR = "KIR"
    KNA = "KNA"
    KOR = "KOR"
    KWT = "KWT"
    LAO = "LAO"
    LBN = "LBN"
    LBR = "LBR"
    LBY = "LBY"
    LCA = "LCA"
    LIE = "LIE"
    LKA = "LKA"
    LSO = "LSO"
    LTU = "LTU"
    LUX = "LUX"
    LVA = "LVA"
    MAC = "MAC"
    MAF = "MAF"
    MAR = "MAR"
    MCO = "MCO"
    MDA = "MDA"
    MDG = "MDG"
    MDV = "MDV"
    MEX = "MEX"
    MHL = "MHL"
    MKD = "MKD"
    MLI = "MLI"
    MLT = "MLT"
    MMR = "MMR"
    MNE = "MNE"
    MNG = "MNG"
    MNP = "MNP"
    MOZ = "MOZ"
    MRT = "MRT"
    MSR = "MSR"
    MTQ = "MTQ"
    MUS = "MUS"
    MWI = "MWI"
    MYS = "MYS"
    MYT = "MYT"
    NAM = "NAM"
    NCL = "NCL"
    NER = "NER"
    NFK = "NFK"
    NGA = "NGA"
    NIC = "NIC"
    NIU = "NIU"
    NLD = "NLD"
    NOR = "NOR"
    NPL = "NPL"
    NRU = "NRU"
    NZL = "NZL"
    OMN = "OMN"
    PAK = "PAK"
    PAN = "PAN"
    PCN = "PCN"
    PER = "PER"
    PHL = "PHL"
    PLW = "PLW"
    PNG = "PNG"
    POL = "POL"
    PRI = "PRI"
    PRK = "PRK"
    PRT = "PRT"
    PRY = "PRY"
    PSE = "PSE"
    PYF = "PYF"
    QAT = "QAT"
    REU = "REU"
    ROU = "ROU"
    RUS = "RUS"
    RWA = "RWA"
    SAU = "SAU"
    SDN = "SDN"
    SEN = "SEN"
    SGP = "SGP"
    SGS = "SGS"
    SHN = "SHN"
    SJM = "SJM"
    SLB = "SLB"
    SLE = "SLE"
    SLV = "SLV"
    SMR = "SMR"
    SOM = "SOM"
    SPM = "SPM"
    SRB = "SRB"
    SSD = "SSD"
    STP = "STP"
    SUR = "SUR"
    SVK = "SVK"
    SVN = "SVN"
    SWE = "SWE"
    SWZ = "SWZ"
    SXM = "SXM"
    SYC = "SYC"
    SYR = "SYR"
    TCA = "TCA"
    TCD = "TCD"
    TGO = "TGO"
    THA = "THA"
    TJK = "TJK"
    TKL = "TKL"
    TKM = "TKM"
    TLS = "TLS"
    TON = "TON"
    TTO = "TTO"
    TUN = "TUN"
    TUR = "TUR"
    TUV = "TUV"
    TWN = "TWN"
    TZA = "TZA"
    UGA = "UGA"
    UKR = "UKR"
    UMI = "UMI"
    URY = "URY"
    USA = "USA"
    UZB = "UZB"
    VAT = "VAT"
    VCT = "VCT"
    VEN = "VEN"
    VGB = "VGB"
    VIR = "VIR"
    VNM = "VNM"
    VUT = "VUT"
    WLF = "WLF"
    WSM = "WSM"
    YEM = "YEM"
    ZAF = "ZAF"
    ZMB = "ZMB"
    ZWE = "ZWE"


class GeoAggregate(str, Enum):
    CONTINENT_AF = "continent:AF"
    CONTINENT_AN = "continent:AN"
    CONTINENT_AS = "continent:AS"
    CONTINENT_EU = "continent:EU"
    CONTINENT_NA = "continent:NA"
    CONTINENT_OC = "continent:OC"
    CONTINENT_SA = "continent:SA"


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


class GeographicalData(SQLModel, table=False):
    """Stores the geographies that a publication relates to.

    - iso3_country_codes: zero or more ISO3 codes (validated/coerced from Enum/str)
    - aggregates: zero or more namespaced aggregates (e.g., continent:EU)
    """
    iso3_country_codes: List[str] = []
    aggregates: List[str] = []

    @field_validator("iso3_country_codes", mode="before")
    @classmethod
    def _normalize_iso3_list(cls, v):
        if v is None:
            return []
        items = v if isinstance(v, list) else [v]
        normalized: List[str] = []
        seen = set()
        for item in items:
            code = getattr(item, "value", item)
            if not isinstance(code, str):
                continue
            code = code.strip().upper()
            if not code:
                continue
            # If code comes as full Enum repr like ISO3Country.USA, take value above
            if code not in seen:
                seen.add(code)
                normalized.append(code)
        return normalized

    @field_validator("aggregates", mode="before")
    @classmethod
    def _normalize_aggregates(cls, v):
        if v is None:
            return []
        items = v if isinstance(v, list) else [v]
        normalized: List[str] = []
        seen = set()
        for item in items:
            value = getattr(item, "value", item)
            if not isinstance(value, str):
                continue
            s = value.strip()
            if not s:
                continue
            if s not in seen:
                seen.add(s)
                normalized.append(s)
        return normalized


class PublicationMetadata(SQLModel, table=False):
    geographical: Optional[GeographicalData] = None
    # other metadata fields


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
    publication_metadata: Dict[str, Any] = Field(sa_column=Column("publication_metadata", JSONB))

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

    @field_validator("publication_metadata", mode="before")
    @classmethod
    def _coerce_metadata(cls, v):
        if v is None:
            return {}
        if isinstance(v, PublicationMetadata):
            return v.model_dump()
        if isinstance(v, dict):
            geo = v.get("geographical")
            if isinstance(geo, GeographicalData):
                v = dict(v)
                v["geographical"] = geo.model_dump()
            elif isinstance(geo, dict):
                # normalize nested fields possibly containing Enums
                gd = GeographicalData(**geo)
                v = dict(v)
                v["geographical"] = gd.model_dump()
            return v
        return v

    @property
    def geographical_data(self) -> Optional[GeographicalData]:
        raw = (self.publication_metadata or {}).get("geographical")
        if raw is None:
            return None
        if isinstance(raw, GeographicalData):
            return raw
        if isinstance(raw, dict):
            return GeographicalData(**raw)
        return None

    @geographical_data.setter
    def geographical_data(self, value: Optional[GeographicalData]) -> None:
        meta = dict(self.publication_metadata or {})
        if value is None:
            meta.pop("geographical", None)
        else:
            meta["geographical"] = value.model_dump()
        self.publication_metadata = meta

    @property
    def metadata_models(self) -> PublicationMetadata:
        if isinstance(self.publication_metadata, PublicationMetadata):
            return self.publication_metadata
        if isinstance(self.publication_metadata, dict):
            return PublicationMetadata(**self.publication_metadata)
        return PublicationMetadata()

    @metadata_models.setter
    def metadata_models(self, value: PublicationMetadata) -> None:
        self.publication_metadata = value.model_dump()


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

    @property
    def positional_data_models(self) -> List[PositionalData]:
        result: List[PositionalData] = []
        for item in (self.positional_data or []):
            if isinstance(item, PositionalData):
                result.append(item)
            elif isinstance(item, dict):
                result.append(PositionalData(**item))
        return result

    @positional_data_models.setter
    def positional_data_models(self, value: List[PositionalData]) -> None:
        self.positional_data = [v.model_dump() for v in (value or [])]

    @field_validator("positional_data", mode="before")
    @classmethod
    def _coerce_positional_data(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            out: List[Dict[str, Any]] = []
            for item in v:
                if isinstance(item, PositionalData):
                    out.append(item.model_dump())
                elif isinstance(item, dict):
                    out.append(item)
            return out
        return v

    def to_html(
        self,
        *,
        include_citation_data: bool = False,
        is_top_level: bool = True,
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

    def nearest_ancestor_with_tag(
        self,
        *,
        tag_names: Iterable[TagName] = (TagName.SECTION,),
    ) -> Optional["Node"]:
        """Return the closest ancestor whose tag is in tag_names.

        If no matching ancestor exists, returns None.
        """
        current: Optional["Node"] = self
        wanted = set(tag_names)
        # Start from the current node's parent
        current = current.parent if current is not None else None
        while current is not None:
            if current.tag_name in wanted:
                return current
            current = current.parent
        return None

    @classmethod
    def render_containing_parent_html(
        cls,
        session: Session,
        node_id: int,
        *,
        container_tags: Iterable[TagName] = (TagName.SECTION,),
        include_citation_data: bool = True,
        pretty: bool = True,
        separator: str = "\n",
    ) -> Optional[str]:
        """Render the HTML for the nearest containing parent and its subtree.

        Typical usage is to render the entire <section> a paragraph belongs to.

        - If a container ancestor is found (matching container_tags), that node's
          subtree is rendered.
        - If none is found, the original node's subtree is rendered as a fallback.
        - Returns None when node_id does not exist.
        """
        node: Optional["Node"] = session.get(cls, node_id)
        if node is None:
            return None

        # Attempt to find the nearest ancestor with the desired tag(s).
        # Because relationships are lazy-loaded, walking via attributes is fine
        # as long as we stay within this session.
        current: Optional["Node"] = node.parent
        wanted = set(container_tags)
        container: Optional["Node"] = None
        while current is not None:
            if current.tag_name in wanted:
                container = current
                break
            current = current.parent

        target: "Node" = container or node
        return target.to_html(
            include_citation_data=include_citation_data,
            is_top_level=True,
            separator=separator,
            pretty=pretty,
        )

    @classmethod
    def render_context_html(
        cls,
        session: Session,
        node_id: int,
        *,
        include_citation_data: bool = True,
        pretty: bool = True,
        separator: str = "\n",
    ) -> Optional[str]:
        """Render a human-meaningful context container for a node.

        Heuristics:
        - TD/TH/TR/THEAD/TBODY/TFOOT/CAPTION -> TABLE
        - FIGCAPTION/IMG -> FIGURE (fallback SECTION)
        - LI -> UL or OL
        - Default text/heading/code/cite/blockquote -> nearest of SECTION/ASIDE/NAV
        - In all cases, if those are absent, fallback to MAIN/HEADER/FOOTER
        - If the node itself is a container (SECTION, ASIDE, NAV, FIGURE, TABLE, UL, OL),
          render that node directly.
        """
        node: Optional["Node"] = session.get(cls, node_id)
        if node is None:
            return None

        tag = node.tag_name
        # If this node already represents a suitable container, render it directly
        direct_container_tags = {
            TagName.SECTION,
            TagName.ASIDE,
            TagName.NAV,
            TagName.FIGURE,
            TagName.TABLE,
            TagName.UL,
            TagName.OL,
        }
        if tag in direct_container_tags:
            return node.to_html(
                include_citation_data=include_citation_data,
                is_top_level=True,
                separator=separator,
                pretty=pretty,
            )

        # Heuristic container mapping
        if tag in {TagName.TD, TagName.TH, TagName.TR, TagName.THEAD, TagName.TBODY, TagName.TFOOT, TagName.CAPTION}:
            preferred_containers = (
                TagName.TABLE,
                TagName.SECTION,
                TagName.MAIN,
                TagName.HEADER,
                TagName.FOOTER,
            )
        elif tag in {TagName.FIGCAPTION, TagName.IMG}:
            preferred_containers = (
                TagName.FIGURE,
                TagName.SECTION,
                TagName.MAIN,
                TagName.HEADER,
                TagName.FOOTER,
            )
        elif tag == TagName.LI:
            preferred_containers = (
                TagName.UL,
                TagName.OL,
                TagName.SECTION,
                TagName.MAIN,
                TagName.HEADER,
                TagName.FOOTER,
            )
        else:
            # Paragraphs, headings, code, cite, blockquote, etc.
            preferred_containers = (
                TagName.FIGURE,
                TagName.TABLE,
                TagName.SECTION,
                TagName.ASIDE,
                TagName.NAV,
                TagName.MAIN,
                TagName.HEADER,
                TagName.FOOTER,
            )

        return cls.render_containing_parent_html(
            session,
            node_id,
            container_tags=preferred_containers,
            include_citation_data=include_citation_data,
            pretty=pretty,
            separator=separator,
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
    embedding_vector: List[float] = Field(sa_column=Column(Vector(1536)))
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
