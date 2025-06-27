from datetime import datetime, date

from sqlmodel import Session

from extract.classify_document_types import PublicationDetailsWithClassification
from load.schema import Publication, Document, DocumentType


def persist_publication(pub_data: PublicationDetailsWithClassification, session: Session) -> Publication:
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
        title=pub_data.title,
        abstract=pub_data.abstract,  # Using get() for optional fields
        citation=pub_data.citation,
        authors=pub_data.metadata.authors,
        publication_date=parse_date(pub_data.metadata.date),
        source="World Bank Open Knowledge Repository",
        source_url=str(pub_data.source_url),
        uri=str(pub_data.uri),
    )

    # Create documents and add them to the publication's documents list
    documents = []
    for dl in pub_data.download_links:
        # Only create Document objects for links marked as to_download=True
        if dl.classification == DocumentType.MAIN or dl.classification == DocumentType.SUPPLEMENTAL:
            # Validate required fields
            doc = Document(
                # No id or publication_id - these will be auto-generated and set by the relationship
                type=DocumentType(dl.classification.upper()),
                download_url=str(dl.url),
                description=dl.text.strip(),
                mime_type=dl.file_info.mime_type,
                charset=dl.file_info.charset,
                # storage_url and file_size are explicitly NULL - they will be populated later during Stage 2
                storage_url=None,
                file_size=None,
            )
            documents.append(doc)

    # Check that we have at least one valid document
    if not documents:
        raise ValueError(
            f"No valid documents found for publication: {pub_data.title}"
        )

    # Assign documents to the publication
    publication.documents = documents

    # Add the main publication object to the session
    session.add(publication)

    # Return the publication object (caller is responsible for committing)
    return publication


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
