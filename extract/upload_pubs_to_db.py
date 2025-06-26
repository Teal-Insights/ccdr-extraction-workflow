import json
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Tuple

from sqlmodel import Session, select

from extract.schema import Publication, Document, DocumentType
from extract.db import engine


def persist_publication(pub_data: Dict[str, Any], session: Session) -> Publication:
    """
    Creates SQLModel objects for a publication and its documents and adds them
    to the session. Does NOT commit the session.

    Args:
        pub_data: A dictionary with the complete publication metadata.
        session: The active SQLModel session.

    Returns:
        The created Publication object, ready to be committed.
    """
    # Create the publication (without ID - will be auto-generated)
    publication = Publication(
        title=pub_data["title"],
        abstract=pub_data.get("abstract"),  # Using get() for optional fields
        citation=pub_data["citation"],
        authors=pub_data["metadata"]["authors"],
        publication_date=parse_date(pub_data["metadata"]["date"]),
        source=pub_data["source"],
        source_url=pub_data["source_url"],
        uri=pub_data["uri"],
    )

    # Create documents and add them to the publication's documents list
    documents = []
    for dl in pub_data["downloadLinks"]:
        # Only create Document objects for links marked as to_download=True
        if dl.get("to_download", False):
            # Validate required fields
            file_info = dl.get("file_info", {})
            mime_type = file_info.get("mime_type")
            charset = file_info.get("charset")

            if not mime_type or mime_type == "error":
                print(f"Warning: Skipping document with invalid mime_type: {dl['url']}")
                continue

            if not charset:
                print(f"Warning: Setting default charset for document: {dl['url']}")
                charset = "utf-8"

            doc = Document(
                # No id or publication_id - these will be auto-generated and set by the relationship
                type=DocumentType(dl["type"].upper()),
                download_url=dl["url"],
                description=dl["text"].strip(),
                mime_type=mime_type,
                charset=charset,
                # storage_url and file_size are explicitly NULL - they will be populated later during Stage 2
                storage_url=None,
                file_size=None,
            )
            documents.append(doc)

    # Check that we have at least one valid document
    if not documents:
        raise ValueError(
            f"No valid documents found for publication: {pub_data.get('title', 'Unknown')}"
        )

    # Assign documents to the publication
    publication.documents = documents

    # Add the main publication object to the session
    session.add(publication)

    # Return the publication object (caller is responsible for committing)
    return publication


# Legacy and utility functions for backward compatibility
def load_publication_data(json_path: str) -> List[Dict[str, Any]]:
    """Load publication data from JSON file."""
    with open(json_path, "r") as f:
        return json.load(f)


def parse_date(date_str: str) -> date:
    """Parse date string in various formats to datetime.date object."""
    formats = ["%Y-%m-%d", "%Y-%m", "%Y"]  # 2025-01-15  # 2022-11  # 2022

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


def create_publication_with_documents(pub_data: Dict[str, Any]) -> Publication:
    """Create a Publication object with its associated documents from JSON data."""
    # Create the publication (without ID - will be auto-generated)
    publication = Publication(
        title=pub_data["title"],
        abstract=pub_data.get("abstract"),  # Using get() for optional fields
        citation=pub_data["citation"],
        authors=pub_data["metadata"]["authors"],
        publication_date=parse_date(pub_data["metadata"]["date"]),
        source=pub_data["source"],
        source_url=pub_data["source_url"],
        uri=pub_data["uri"],
    )

    # Create documents and add them to the publication's documents list
    documents = []
    for dl in pub_data["downloadLinks"]:
        # Only create Document objects for links marked as to_download=True
        if dl.get("to_download", False):
            doc = Document(
                # No id or publication_id - these will be auto-generated and set by the relationship
                type=DocumentType(dl["type"].upper()),
                download_url=dl["url"],
                description=dl["text"].strip(),
                mime_type=dl["file_info"]["mime_type"],
                charset=dl["file_info"]["charset"],
                # storage_url will be populated later during processing
                # file_size will be populated later during processing
            )
            documents.append(doc)

    # Assign documents to the publication
    publication.documents = documents

    return publication


def publication_exists_by_uri(session: Session, uri: str) -> bool:
    """Check if a publication already exists in the database by URI."""
    statement = select(Publication).where(Publication.uri == uri)
    return session.exec(statement).first() is not None


def verify_publication_upload(
    session: Session, pub_data: Dict[str, Any], db_publication: Publication
) -> Tuple[bool, str]:
    """Verify that a publication was uploaded correctly."""
    # Verify core fields match
    if db_publication.title != pub_data["title"]:
        return False, f"Title mismatch for publication {db_publication.id}"
    if db_publication.citation != pub_data["citation"]:
        return False, f"Citation mismatch for publication {db_publication.id}"
    if db_publication.uri != pub_data["uri"]:
        return False, f"URI mismatch for publication {db_publication.id}"

    return True, "Publication verified successfully"


def verify_documents_upload(
    session: Session, pub_data: Dict[str, Any], db_publication: Publication
) -> Tuple[bool, str]:
    """Verify that documents were uploaded correctly."""
    statement = select(Document).where(Document.publication_id == db_publication.id)
    db_docs = session.exec(statement).all()

    source_doc_count = len(pub_data["downloadLinks"])
    db_doc_count = len(db_docs)

    if source_doc_count != db_doc_count:
        return (
            False,
            f"Document count mismatch for publication {db_publication.id}. Expected: {source_doc_count}, Found: {db_doc_count}",
        )

    # Verify document details match
    source_download_urls = {dl["url"] for dl in pub_data["downloadLinks"]}
    db_download_urls = {doc.download_url for doc in db_docs}

    if source_download_urls != db_download_urls:
        return False, f"Document URLs don't match for publication {db_publication.id}"

    return True, "Documents verified successfully"


def upload_data(json_path: str, skip_existing: bool = True) -> Dict[str, int]:
    """Upload publication and document data to database."""
    publications_data = load_publication_data(json_path)
    stats = {
        "total": len(publications_data),
        "skipped": 0,
        "success": 0,
        "failed": 0,
        "validation_failed": 0,
    }

    with Session(engine) as session:
        for pub_data in publications_data:
            try:
                source_id = pub_data.get("id", "unknown")
                uri = pub_data.get("uri", "unknown")

                # Skip if publication exists and skip_existing is True
                if skip_existing and publication_exists_by_uri(session, uri):
                    print(f"⏭ Skipping existing publication {source_id} (URI: {uri})")
                    stats["skipped"] += 1
                    continue

                # Create publication with its documents
                publication = create_publication_with_documents(pub_data)

                # Add to session and commit
                session.add(publication)
                session.commit()

                # Refresh to get the auto-generated IDs
                session.refresh(publication)

                # Verify the upload
                pub_verified, pub_msg = verify_publication_upload(
                    session, pub_data, publication
                )
                docs_verified, docs_msg = verify_documents_upload(
                    session, pub_data, publication
                )

                if pub_verified and docs_verified:
                    print(
                        f"✓ Successfully uploaded and verified publication {publication.id} (source: {source_id}) with {len(publication.documents)} documents"
                    )
                    stats["success"] += 1
                else:
                    print(
                        f"⚠ Upload validation failed for {source_id} (DB ID: {publication.id}):"
                    )
                    if not pub_verified:
                        print(f"  - Publication: {pub_msg}")
                    if not docs_verified:
                        print(f"  - Documents: {docs_msg}")
                    stats["validation_failed"] += 1

            except Exception as e:
                session.rollback()
                print(
                    f"❌ Error uploading publication {pub_data.get('id', 'unknown')}: {str(e)}"
                )
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

    print("Sample record:")
    # Create a session
    with Session(engine) as session:
        # Query for a Document (take the first one available)
        statement = select(Document).limit(1)
        result = session.exec(statement).first()

        if result:
            print(result.model_dump_json())

            # Get and print the related Publication
            pub = result.publication
            if pub:
                print(pub.model_dump_json())
        else:
            print("Something went wrong; no documents found in the database.")
