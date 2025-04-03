from datetime import date, datetime, UTC
from typing import List, Optional, Annotated, Dict, Any
from enum import Enum
import os
from dotenv import load_dotenv
from sqlmodel import Field, Relationship, SQLModel, create_engine, Column, select, Session
from pydantic import HttpUrl
from sqlalchemy.dialects.postgresql import ARRAY, FLOAT, JSONB, VARCHAR
import numpy as np

# Load environment variables
load_dotenv()

# Enums for document and node types
class DocumentType(str, Enum):
    MAIN = "MAIN"
    SUPPLEMENTAL = "SUPPLEMENTAL"
    OTHER = "OTHER"

class NodeType(str, Enum):
    SECTION_HEADING = "SECTION_HEADING"
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
    __tablename__ = "publication"
    __table_args__ = {'comment': 'Contains publication metadata and relationships to documents'}
    
    publication_id: str = Field(primary_key=True, max_length=50, index=True)
    title: str = Field(max_length=500)
    abstract: Optional[str] = None
    citation: str
    authors: str
    publication_date: date = Field(index=True)
    source: str = Field(max_length=100)
    source_url: Annotated[HttpUrl, Field(sa_column=Column(VARCHAR(500)))]
    uri: Annotated[HttpUrl, Field(sa_column=Column(VARCHAR(500)))]
    
    # Relationships
    documents: List["Document"] = Relationship(back_populates="publication")


class Document(SQLModel, table=True):
    __tablename__ = "document"
    __table_args__ = {'comment': 'Contains document metadata and relationships to content nodes'}
    
    document_id: str = Field(primary_key=True, max_length=50, index=True)
    publication_id: str = Field(foreign_key="publication.publication_id", index=True)
    type: DocumentType
    download_url: Annotated[HttpUrl, Field(sa_column=Column(VARCHAR(500)))]
    description: str
    mime_type: str = Field(max_length=100)
    charset: str = Field(max_length=50)
    storage_url: Optional[str] = Field(default=None, max_length=500)
    file_size: Optional[int] = None
    
    # Relationships
    publication: Publication = Relationship(back_populates="documents")
    content_nodes: List["ContentNode"] = Relationship(back_populates="document")


class ContentNode(SQLModel, table=True):
    __tablename__ = "content_node"
    __table_args__ = {'comment': 'Contains hierarchical document content with relationships to embeddings and footnotes'}
    
    node_id: str = Field(primary_key=True, max_length=50, index=True)
    document_id: str = Field(foreign_key="document.document_id", index=True)
    parent_node_id: Optional[str] = Field(default=None, foreign_key="content_node.node_id", index=True)
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
    
    # Relationships
    document: Document = Relationship(back_populates="content_nodes")
    parent: Optional["ContentNode"] = Relationship(
        back_populates="children",
        sa_relationship_kwargs={"remote_side": "ContentNode.node_id"}
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
    __tablename__ = "embedding"
    __table_args__ = {'comment': 'Contains vector embeddings for content nodes'}
    
    embedding_id: str = Field(primary_key=True, max_length=50, index=True)
    node_id: str = Field(foreign_key="content_node.node_id", index=True)
    embedding_vector: List[float] = Field(
        sa_column=Column(ARRAY(FLOAT))
    )
    model_name: str = Field(max_length=100)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    
    # Relationships
    content_node: ContentNode = Relationship(back_populates="embeddings")


class FootnoteReference(SQLModel, table=True):
    __tablename__ = "footnote_reference"
    __table_args__ = {'comment': 'Contains bidirectional footnote references between content nodes'}
    
    footnote_ref_id: str = Field(primary_key=True, max_length=50, index=True)
    referencing_node_id: str = Field(foreign_key="content_node.node_id", index=True)
    definition_node_id: str = Field(foreign_key="content_node.node_id", index=True)
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


# Database connection setup
def get_database_url():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "nature-finance-rag-db")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


# Create database engine
engine = create_engine(get_database_url())

# Create all tables
def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def test_publication():
    """Test creating a publication with valid URLs"""
    publication = Publication(
        publication_id="pub_001",
        title="Test Publication",
        abstract="A test abstract",
        citation="Author et al. (2024)",
        authors="Test Author, Another Author",
        publication_date=date(2024, 1, 1),
        source="Nature",
        source_url="https://www.nature.com/articles/test",
        uri="https://doi.org/10.1038/test"
    )
    return publication

def test_document(publication: Publication):
    """Test creating a document with valid type enum"""
    document = Document(
        document_id="dl_001",
        publication_id=publication.publication_id,
        type=DocumentType.MAIN,
        download_url="https://www.nature.com/articles/test.pdf",
        description="Main article PDF",
        mime_type="application/pdf",
        charset="utf-8",
        storage_url="s3://bucket/test.pdf",
        file_size=1024
    )
    return document

def test_content_node(document: Document):
    """Test creating content nodes with parent-child relationship"""
    # Create parent node
    bbox = BoundingBox(
        page=1,
        x1=0.1,
        y1=0.1,
        x2=0.9,
        y2=0.2,
        width=0.8,
        height=0.1
    )
    
    parent_node = ContentNode(
        node_id="cn_001",
        document_id=document.document_id,
        node_type=NodeType.SECTION_HEADING,
        content="Introduction",
        sequence_in_parent=1,
        sequence_in_document=1,
        start_page_pdf=1,
        end_page_pdf=1,
        start_page_logical="1",
        end_page_logical="1",
        bounding_box=bbox.dict()
    )
    
    # Create child node
    child_bbox = BoundingBox(
        page=1,
        x1=0.1,
        y1=0.3,
        x2=0.9,
        y2=0.4,
        width=0.8,
        height=0.1
    )
    
    child_node = ContentNode(
        node_id="cn_002",
        document_id=document.document_id,
        parent_node_id=parent_node.node_id,
        node_type=NodeType.PARAGRAPH,
        content="This is a test paragraph.",
        sequence_in_parent=1,
        sequence_in_document=2,
        start_page_pdf=1,
        end_page_pdf=1,
        start_page_logical="1",
        end_page_logical="1",
        bounding_box=child_bbox.dict()
    )
    return parent_node, child_node

def test_embedding(node: ContentNode):
    """Test creating an embedding with vector array"""
    embedding = Embedding(
        embedding_id="em_001",
        node_id=node.node_id,
        embedding_vector=np.random.rand(384).tolist(),  # Test with 384-dim vector
        model_name="test-embedding-model",
        created_at=datetime.now(UTC)
    )
    return embedding

def test_footnote(referencing_node: ContentNode, definition_node: ContentNode):
    """Test creating a footnote reference"""
    footnote = FootnoteReference(
        footnote_ref_id="fr_001",
        referencing_node_id=referencing_node.node_id,
        definition_node_id=definition_node.node_id,
        marker_text="1",
        sequence_in_node=1
    )
    return footnote

def validate_setup():
    """Run all validation tests"""
    with Session(engine) as session:
        try:
            # Test Publication
            publication = test_publication()
            session.add(publication)
            session.commit()
            print("✓ Publication created successfully")
            
            # Test Document
            document = test_document(publication)
            session.add(document)
            session.commit()
            print("✓ Document created successfully")
            
            # Test ContentNodes
            parent_node, child_node = test_content_node(document)
            session.add(parent_node)
            session.add(child_node)
            session.commit()
            print("✓ ContentNodes created successfully")
            
            # Test parent-child relationship
            loaded_child = session.exec(
                select(ContentNode).where(ContentNode.node_id == child_node.node_id)
            ).one()
            assert loaded_child.parent.node_id == parent_node.node_id
            print("✓ Parent-child relationship verified")
            
            # Test Embedding
            embedding = test_embedding(parent_node)
            session.add(embedding)
            session.commit()
            print("✓ Embedding created successfully")
            
            # Test vector array retrieval
            loaded_embedding = session.exec(
                select(Embedding).where(Embedding.embedding_id == embedding.embedding_id)
            ).one()
            assert len(loaded_embedding.embedding_vector) == 384
            print("✓ Embedding vector verified")
            
            # Test Footnote
            footnote = test_footnote(parent_node, child_node)
            session.add(footnote)
            session.commit()
            print("✓ Footnote created successfully")
            
            # Test bidirectional footnote relationships
            loaded_footnote = session.exec(
                select(FootnoteReference).where(FootnoteReference.footnote_ref_id == footnote.footnote_ref_id)
            ).one()
            assert loaded_footnote.referencing_node.node_id == parent_node.node_id
            assert loaded_footnote.definition_node.node_id == child_node.node_id
            print("✓ Footnote relationships verified")
            
            print("\n✓ All validation tests passed successfully!")
            
        except Exception as e:
            print(f"\n❌ Validation failed: {str(e)}")
            raise
        finally:
            # Cleanup test data
            session.rollback()
            print("\nTest data rolled back")


if __name__ == "__main__":
    # Drop all tables
    SQLModel.metadata.drop_all(engine)
    # Create all tables
    create_db_and_tables()
    # Validate the setup
    validate_setup()