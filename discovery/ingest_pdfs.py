#!/usr/bin/env python3

"""
PDF Ingestion Script (Python Version)

This script provides utilities to process PDF files, generate embeddings, 
and store them in a PostgreSQL database using SQLModel and pgvector.

Usage:
1. To process a single file:
   python discovery/ingest_pdfs.py --file=./data/example.pdf

2. To process all PDFs in a directory:
   python discovery/ingest_pdfs.py --dir=./data

Prerequisites:
- PostgreSQL database with the pgvector extension enabled:
  CREATE EXTENSION IF NOT EXISTS vector;
- Environment variables set in a .env file:
  DATABASE_URL="postgresql://user:password@host:port/dbname"
  OPENAI_API_KEY="your_openai_api_key"
"""

import argparse
import asyncio
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
from dotenv import load_dotenv
from nanoid import generate as generate_nanoid
from openai import AsyncOpenAI
from pypdf import PdfReader
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlmodel import Field, SQLModel, Column
from pgvector.sqlalchemy import Vector


# --- Environment Variables & Configuration ---
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMBEDDING_MODEL = "text-embedding-ada-002"
# Match the embedding dimension of text-embedding-ada-002
EMBEDDING_DIM = 1536

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set.")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable not set.")


# --- Database Setup (SQLModel & SQLAlchemy with pgvector) ---

# Define the SQLModel classes
class Resource(SQLModel, table=True):
    id: str = Field(default_factory=generate_nanoid, primary_key=True, index=True)
    content: str = Field(sa_column=Column(TEXT)) # Use TEXT for potentially large content
    source: Optional[str] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=datetime.utcnow, nullable=False, sa_column_kwargs={"onupdate": datetime.utcnow})

class Embedding(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    resource_id: str = Field(foreign_key="resource.id", index=True)
    content: str = Field(sa_column=Column(TEXT))
    embedding: List[float] = Field(sa_column=Column(Vector(EMBEDDING_DIM))) # Use pgvector type

# Create the async engine
engine = create_async_engine(DATABASE_URL, echo=False, future=True)

# Async session maker
AsyncSessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

async def init_db():
    """Initialize the database and create tables."""
    async with engine.begin() as conn:
        # Enable pgvector extension if not already enabled
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
        # Create tables
        await conn.run_sync(SQLModel.metadata.create_all)

# --- OpenAI Client ---
aclient = AsyncOpenAI(api_key=OPENAI_API_KEY)

# --- Text Processing and Embedding ---

def chunk_pdf_text(text_content: str, max_chunk_size: int = 2500) -> List[str]:
    """
    Splits PDF text into manageable chunks, respecting paragraphs and sentences.
    """
    # Normalize whitespace and split into paragraphs (double newline)
    paragraphs = re.split(r'\n{2,}', text_content.strip())
    paragraphs = [re.sub(r'\s+', ' ', p).strip() for p in paragraphs if p.strip()]

    chunks: List[str] = []
    current_chunk = ""

    for paragraph in paragraphs:
        if len(current_chunk) + len(paragraph) + 2 <= max_chunk_size: # +2 for potential '\n\n'
            current_chunk += ('\n\n' if current_chunk else '') + paragraph
        else:
            # Chunk is too large, add the current one if it exists
            if current_chunk:
                chunks.append(current_chunk)
            
            # If the paragraph itself is too large, split it
            if len(paragraph) > max_chunk_size:
                # Split by sentences
                sentences = re.split(r'(?<=[.!?])\s+', paragraph)
                sentences = [s.strip() for s in sentences if s.strip()]
                
                temp_chunk = ""
                for sentence in sentences:
                    if len(temp_chunk) + len(sentence) + 1 <= max_chunk_size: # +1 for space
                        temp_chunk += (" " if temp_chunk else "") + sentence
                    else:
                        if temp_chunk: # Add the filled temp_chunk
                           chunks.append(temp_chunk)
                        
                        # If sentence is still too long, truncate (less ideal)
                        if len(sentence) > max_chunk_size:
                           chunks.append(sentence[:max_chunk_size])
                           # Handle remainder? For now, just truncate.
                           temp_chunk = "" # Reset after forced split
                        else:
                           temp_chunk = sentence # Start new temp_chunk with the sentence
                
                if temp_chunk: # Add any remaining part
                    chunks.append(temp_chunk)
                current_chunk = "" # Reset current_chunk after handling large paragraph
            
            else: # Paragraph fits in a new chunk
                current_chunk = paragraph

    # Add the last remaining chunk
    if current_chunk:
        chunks.append(current_chunk)

    # Filter out any potential empty chunks just in case
    return [c for c in chunks if c]


async def generate_embeddings(
    texts: List[str],
) -> List[Tuple[str, List[float]]]:
    """Generates embeddings for a list of text chunks."""
    if not texts:
        return []
        
    response = await aclient.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts
    )
    
    embeddings = [item.embedding for item in response.data]
    
    if len(texts) != len(embeddings):
        # This shouldn't happen with the current API, but good to check
        raise ValueError("Mismatch between number of texts and embeddings returned.")
        
    return list(zip(texts, embeddings))

async def generate_single_embedding(text: str) -> List[float]:
    """Generates embedding for a single piece of text."""
    response = await aclient.embeddings.create(
        model=EMBEDDING_MODEL,
        input=[text.replace("\n", " ")] # OpenAI recommends replacing newlines
    )
    return response.data[0].embedding

# --- PDF Processing ---

async def process_pdf(file_path: Path) -> str:
    """Extracts text content from a PDF file."""
    try:
        reader = PdfReader(file_path)
        text_content = ""
        for page in reader.pages:
            text_content += page.extract_text() or "" # Add null check
        return text_content
    except Exception as e:
        print(f"Error reading PDF {file_path}: {e}")
        raise

async def process_and_store_pdf(file_path: Path, session: AsyncSession) -> Tuple[str, int]:
    """Processes a single PDF, extracts text, chunks, embeds, and stores."""
    print(f"Processing PDF: {file_path.name}...")
    try:
        # 1. Extract text
        pdf_text = await process_pdf(file_path)
        if not pdf_text.strip():
            print(f"Warning: No text extracted from {file_path.name}. Skipping.")
            return "", 0

        # 2. Store the full resource
        resource = Resource(content=pdf_text, source=file_path.name)
        session.add(resource)
        await session.flush() # Flush to get the resource.id

        # 3. Chunk the text
        text_chunks = chunk_pdf_text(pdf_text)
        if not text_chunks:
            print(f"Warning: No chunks generated for {file_path.name}. Skipping embedding.")
            await session.commit() # Commit the resource even if no chunks
            return resource.id, 0

        # 4. Generate embeddings for chunks
        print(f"  Generating {len(text_chunks)} embeddings for {file_path.name}...")
        embedding_data = await generate_embeddings(text_chunks)

        # 5. Store embeddings
        embeddings_to_store = []
        for chunk_content, embedding_vector in embedding_data:
            if len(embedding_vector) != EMBEDDING_DIM:
                 print(f"Warning: Embedding dimension mismatch for chunk in {file_path.name}. Expected {EMBEDDING_DIM}, got {len(embedding_vector)}. Skipping chunk.")
                 continue
            embeddings_to_store.append(
                Embedding(
                    resource_id=resource.id,
                    content=chunk_content,
                    embedding=embedding_vector,
                )
            )
        
        if embeddings_to_store:
            session.add_all(embeddings_to_store)
            await session.commit()
            print(f"  Stored {len(embeddings_to_store)} embeddings for {file_path.name}.")
            return resource.id, len(embeddings_to_store)
        else:
            print(f"  No valid embeddings generated for {file_path.name}.")
            await session.commit() # Commit resource even if no embeddings stored
            return resource.id, 0

    except Exception as e:
        await session.rollback()
        print(f"Error processing {file_path.name}: {e}")
        raise # Re-raise the exception to be caught by the main loop

# --- Directory Processing ---

async def process_directory(dir_path: Path, session: AsyncSession) -> List[dict]:
    """Processes all PDF files in a given directory."""
    results = []
    pdf_files = list(dir_path.glob("*.pdf")) + list(dir_path.glob("*.PDF"))
    
    if not pdf_files:
        print(f"No PDF files found in directory: {dir_path}")
        return []

    print(f"Found {len(pdf_files)} PDF files in {dir_path}.")

    for pdf_file in pdf_files:
        try:
            resource_id, num_chunks = await process_and_store_pdf(pdf_file, session)
            if resource_id: # Only add result if processing started
                 results.append({
                    "file": pdf_file.name,
                    "resource_id": resource_id,
                    "chunks_stored": num_chunks,
                 })
        except Exception as e:
            # Error is already logged in process_and_store_pdf
            print(f"Skipping {pdf_file.name} due to error.")
            continue # Continue to the next file

    return results

# --- Semantic Search (Example) ---

async def find_relevant_content(user_query: str, session: AsyncSession, top_k: int = 5):
    """Finds relevant content chunks based on semantic similarity using pgvector."""
    if not user_query:
        return []

    print(f"Finding relevant content for query: '{user_query[:50]}...'")
    query_embedding = await generate_single_embedding(user_query)
    
    # Use the l2_distance operator (<->) from pgvector for similarity search
    # Note: Cosine distance can also be used: 1 - (embedding <=> query_embedding)
    stmt = (
        text(
            """
        SELECT
            embedding.content,
            resource.source,
            embedding.embedding <-> :query_vector AS distance
        FROM embedding
        JOIN resource ON embedding.resource_id = resource.id
        ORDER BY distance ASC
        LIMIT :limit
        """
        )
        .bindparams(query_vector=np.array(query_embedding), limit=top_k)
    )
    
    result = await session.execute(stmt)
    similar_chunks = result.mappings().all() # Use mappings() for dict-like results

    print(f"Found {len(similar_chunks)} relevant chunks.")
    return similar_chunks


# --- Main Execution ---

async def main():
    parser = argparse.ArgumentParser(description="Process PDF files and store embeddings.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", type=str, help="Path to a single PDF file.")
    group.add_argument("--dir", type=str, help="Path to a directory containing PDF files.")
    parser.add_argument("--query", type=str, help="Optional: Perform a semantic search query after processing.")
    
    args = parser.parse_args()

    # Initialize database
    print("Initializing database...")
    await init_db()
    print("Database initialized.")

    async with AsyncSessionLocal() as session:
        if args.file:
            file_path = Path(args.file)
            if not file_path.is_file():
                print(f"Error: File not found at {file_path}")
                return
            try:
                resource_id, num_chunks = await process_and_store_pdf(file_path, session)
                if resource_id:
                   print(f"\nSuccessfully processed: {file_path.name}")
                   print(f"  Resource ID: {resource_id}")
                   print(f"  Chunks stored: {num_chunks}")
            except Exception:
                print(f"\nFailed to process file: {file_path.name}")

        elif args.dir:
            dir_path = Path(args.dir)
            if not dir_path.is_dir():
                print(f"Error: Directory not found at {dir_path}")
                return
            try:
                results = await process_directory(dir_path, session)
                print(f"\nFinished processing directory: {dir_path}")
                print(f"Processed {len(results)} files:")
                for res in results:
                    print(f"  - {res['file']}: {res['chunks_stored']} chunks (resourceId: {res['resource_id']})")
            except Exception as e:
                print(f"\nAn error occurred during directory processing: {e}")

        # Optional: Perform search after processing
        if args.query:
            print("\nPerforming semantic search...")
            search_results = await find_relevant_content(args.query, session)
            if search_results:
                print("Search Results:")
                for i, hit in enumerate(search_results):
                    print(f"  {i+1}. Score (distance): {hit['distance']:.4f} (Source: {hit['source']})")
                    print(f"     Content: {hit['content'][:150]}...") # Show snippet
            else:
                print("No relevant content found for the query.")

if __name__ == "__main__":
    asyncio.run(main()) 