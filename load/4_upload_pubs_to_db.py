import json
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Tuple

from sqlmodel import Session, select

from schema import (
    Publication,
    Document,
    engine,
    DocumentType
)

def load_publication_data(json_path: str) -> List[Dict[str, Any]]:
    """Load publication data from JSON file."""
    with open(json_path, 'r') as f:
        return json.load(f)

def parse_date(date_str: str) -> date:
    """Parse date string in various formats to datetime.date object."""
    formats = [
        "%Y-%m-%d",  # 2025-01-15
        "%Y-%m",     # 2022-11
        "%Y"         # 2022
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # If no format matches, use the first day of the month/year
    if len(date_str) == 7:  # YYYY-MM
        return datetime.strptime(f"{date_str}-01", "%Y-%m-%d").date()
    elif len(date_str) == 4:  # YYYY
        return datetime.strptime(f"{date_str}-01-01", "%Y-%m-%d").date()
    
    raise ValueError(f"Unable to parse date: {date_str}")

def create_publication(pub_data: Dict[str, Any]) -> Publication:
    """Create a Publication object from JSON data."""
    return Publication(
        publication_id=pub_data["id"],
        title=pub_data["title"],
        abstract=pub_data.get("abstract"),  # Using get() for optional fields
        citation=pub_data["citation"],
        authors=pub_data["metadata"]["authors"],
        publication_date=parse_date(pub_data["metadata"]["date"]),
        source=pub_data["source"],
        source_url=pub_data["source_url"],
        uri=pub_data["uri"]
    )

def create_documents(pub_data: Dict[str, Any], publication_id: str) -> List[Document]:
    """Create Document objects from JSON data."""
    documents = []
    for dl in pub_data["downloadLinks"]:
        doc = Document(
            document_id=dl["id"],
            publication_id=publication_id,
            type=DocumentType(dl["type"].upper()),
            download_url=dl["url"],
            description=dl["text"].strip(),
            mime_type=dl["file_info"]["mime_type"],
            charset=dl["file_info"]["charset"],
            # storage_url will be populated later during processing
            # file_size will be populated later during processing
        )
        documents.append(doc)
    return documents

def publication_exists(session: Session, publication_id: str) -> bool:
    """Check if a publication already exists in the database."""
    statement = select(Publication).where(Publication.publication_id == publication_id)
    return session.exec(statement).first() is not None

def verify_publication_upload(session: Session, pub_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Verify that a publication was uploaded correctly."""
    pub_id = pub_data["id"]
    statement = select(Publication).where(Publication.publication_id == pub_id)
    db_pub = session.exec(statement).first()
    
    if not db_pub:
        return False, f"Publication {pub_id} not found in database"
    
    # Verify core fields match
    if db_pub.title != pub_data["title"]:
        return False, f"Title mismatch for publication {pub_id}"
    if db_pub.citation != pub_data["citation"]:
        return False, f"Citation mismatch for publication {pub_id}"
    if db_pub.uri != pub_data["uri"]:
        return False, f"URI mismatch for publication {pub_id}"
    
    return True, "Publication verified successfully"

def verify_documents_upload(session: Session, pub_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Verify that documents were uploaded correctly."""
    pub_id = pub_data["id"]
    statement = select(Document).where(Document.publication_id == pub_id)
    db_docs = session.exec(statement).all()
    
    source_doc_ids = {dl["id"] for dl in pub_data["downloadLinks"]}
    db_doc_ids = {doc.document_id for doc in db_docs}
    
    if source_doc_ids != db_doc_ids:
        missing = source_doc_ids - db_doc_ids
        extra = db_doc_ids - source_doc_ids
        return False, f"Document mismatch for publication {pub_id}. Missing: {missing}, Extra: {extra}"
    
    return True, "Documents verified successfully"

def upload_data(json_path: str, skip_existing: bool = True) -> Dict[str, int]:
    """Upload publication and document data to database."""
    publications_data = load_publication_data(json_path)
    stats = {
        "total": len(publications_data),
        "skipped": 0,
        "success": 0,
        "failed": 0,
        "validation_failed": 0
    }
    
    with Session(engine) as session:
        for pub_data in publications_data:
            try:
                pub_id = pub_data.get('id', 'unknown')
                
                # Skip if publication exists and skip_existing is True
                if skip_existing and publication_exists(session, pub_id):
                    print(f"⏭ Skipping existing publication {pub_id}")
                    stats["skipped"] += 1
                    continue
                
                # Create and add publication
                publication = create_publication(pub_data)
                session.add(publication)
                
                # Create and add documents
                documents = create_documents(pub_data, publication.publication_id)
                for doc in documents:
                    session.add(doc)
                
                # Commit each publication and its documents
                session.commit()
                
                # Verify the upload
                pub_verified, pub_msg = verify_publication_upload(session, pub_data)
                docs_verified, docs_msg = verify_documents_upload(session, pub_data)
                
                if pub_verified and docs_verified:
                    print(f"✓ Successfully uploaded and verified publication {publication.publication_id} with {len(documents)} documents")
                    stats["success"] += 1
                else:
                    print(f"⚠ Upload validation failed for {pub_id}:")
                    if not pub_verified:
                        print(f"  - Publication: {pub_msg}")
                    if not docs_verified:
                        print(f"  - Documents: {docs_msg}")
                    stats["validation_failed"] += 1
                    
            except Exception as e:
                session.rollback()
                print(f"❌ Error uploading publication {pub_data.get('id', 'unknown')}: {str(e)}")
                stats["failed"] += 1
                continue
    
    return stats

if __name__ == "__main__":
    json_path = Path("extract/data/publication_details.json")
    if not json_path.exists():
        print(f"❌ Error: {json_path} does not exist")
        exit(1)
    
    print("Starting data upload...")
    stats = upload_data(str(json_path))
    print("\nUpload Summary:")
    print(f"Total publications processed: {stats['total']}")
    print(f"Successfully uploaded and verified: {stats['success']}")
    print(f"Skipped (already exists): {stats['skipped']}")
    print(f"Failed to upload: {stats['failed']}")
    print(f"Failed validation: {stats['validation_failed']}")
    print("\nData upload complete!")
