from datetime import date, datetime, UTC
from sqlmodel import SQLModel, Session, select
from pydantic import HttpUrl
import numpy as np
from load.schema import Publication, Document, DocumentType, ContentNode, NodeType, BoundingBox, Embedding, FootnoteReference
from load.db import engine, create_db_and_tables

def test_publication():
    """Test creating a publication with valid URLs"""
    publication = Publication(
        id="pub_001",
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
        id="dl_001",
        publication_id=publication.id,
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
        x1=0.1,
        y1=0.1,
        x2=0.9,
        y2=0.2
    )
    
    parent_node = ContentNode(
        id="cn_001",
        document_id=document.id,
        node_type=NodeType.HEADING,
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
        x1=0.1,
        y1=0.3,
        x2=0.9,
        y2=0.4
    )
    
    child_node = ContentNode(
        id="cn_002",
        document_id=document.id,
        parent_node_id=parent_node.id,
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
    # Generate random vector and convert to regular Python float list
    vector = np.random.rand(384)
    vector_list = [float(x) for x in vector]  # Convert np.float64 to Python float
    
    embedding = Embedding(
        id="em_001",
        node_id=node.id,
        embedding_vector=vector_list,  # Use Python float list instead of NumPy array
        model_name="test-embedding-model",
        created_at=datetime.now(UTC)
    )
    return embedding

def test_footnote(referencing_node: ContentNode, definition_node: ContentNode):
    """Test creating a footnote reference"""
    footnote = FootnoteReference(
        id="fr_001",
        referencing_node_id=referencing_node.id,
        definition_node_id=definition_node.id,
        marker_text="1",
        sequence_in_node=1
    )
    return footnote

def validate_setup():
    """Run all validation tests"""
    with Session(engine) as session:
        created_objects = []
        try:
            # Test Publication
            publication = test_publication()
            session.add(publication)
            session.commit()
            created_objects.append(publication)
            print("✓ Publication created successfully")
            
            # Test Document
            document = test_document(publication)
            session.add(document)
            session.commit()
            created_objects.append(document)
            print("✓ Document created successfully")
            
            # Test ContentNodes
            parent_node, child_node = test_content_node(document)
            session.add(parent_node)
            session.add(child_node)
            session.commit()
            created_objects.append(parent_node)
            created_objects.append(child_node)
            print("✓ ContentNodes created successfully")
            
            # Test parent-child relationship
            loaded_child = session.exec(
                select(ContentNode).where(ContentNode.id == child_node.id)
            ).one()
            assert loaded_child.parent is not None
            assert loaded_child.parent.id == parent_node.id
            print("✓ Parent-child relationship verified")
            
            # Test Embedding
            embedding = test_embedding(parent_node)
            session.add(embedding)
            session.commit()
            created_objects.append(embedding)
            print("✓ Embedding created successfully")
            
            # Test vector array retrieval
            loaded_embedding = session.exec(
                select(Embedding).where(Embedding.id == embedding.id)
            ).one()
            assert len(loaded_embedding.embedding_vector) == 384
            print("✓ Embedding vector verified")
            
            # Test Footnote
            footnote = test_footnote(parent_node, child_node)
            session.add(footnote)
            session.commit()
            created_objects.append(footnote)
            print("✓ Footnote created successfully")
            
            # Test bidirectional footnote relationships
            loaded_footnote = session.exec(
                select(FootnoteReference).where(FootnoteReference.id == footnote.id)
            ).one()
            assert loaded_footnote.referencing_node.id == parent_node.id
            assert loaded_footnote.definition_node.id == child_node.id
            print("✓ Footnote relationships verified")
            
            print("\n✓ All validation tests passed successfully!")
            
        except Exception as e:
            print(f"\n❌ Validation failed: {str(e)}")
            raise
        finally:
            # Cleanup test data by properly deleting the objects in reverse order
            for obj in reversed(created_objects):
                session.delete(obj)
            session.commit()
            print("\nTest data properly cleaned up")


if __name__ == "__main__":
    # Set to True to drop all tables and start fresh with a new database
    DROP_ALL = True
    
    if DROP_ALL:
        # Drop all tables
        SQLModel.metadata.drop_all(engine)
    
    # Create all tables
    create_db_and_tables()

    # Validate the setup
    validate_setup()