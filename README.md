# Nature Finance RAG Database and API for working with IMF climate development reports

## Getting Started

1. Clone the repository with `git clone https://github.com/Teal-Insights/nature-finance-rag-api && cd nature-finance-rag-api`
2. Run `npm install` to install the dependencies
3. Run `docker compose up` to start the Postgres database
4. Run `npm run db:migrate` to migrate the database
5. Run `npm run ingest:pdfs` to ingest the PDFs

## Implementation

The text is chunked by line breaks, with a max chunk length of 2500 characters. This is very naive and should be improved.

## ETL architecture

```mermaid
graph TB
    subgraph "Data Collection"
        C["Cursor Agent<br>(Claude 3.7 Sonnet)"] <-- finds --> B["Data Sources<br>(Web/APIs)"]
        C -- writes --> D["Extraction Scripts<br>(Node.js)"]
        D <-- scrapes --> B
        D --> F["Raw Data"]
    end

    subgraph "Data Processing"
        F --> I["Chunking"]
        I --> J["Annotation"]
        J --> K["Embedding<br>(Google AI)"]
        K --> L["Database Upsert<br>(Drizzle ORM)"]
    end

    subgraph "Database & Storage"
        L --> M["PostgreSQL with pgvector"]
        M -- Stores --> N["Documents<br>(document_id, type, description)"]
        M -- Stores --> O["Content Nodes<br>(sections, paragraphs, tables, images)"]
        M -- Stores --> P["Embeddings<br>(vector data)"]
        M -- Stores --> Q["Footnote References"]
        
        subgraph "Storage Buckets"
            S["Document Storage<br>(PDFs, etc)"]
            T["Content Node Storage<br>(images)"]
        end
        
        N -- storage_url --> S
        O -- storage_url --> T
    end
```

## Database Schema

```mermaid
erDiagram
    %% Relationship lines
    PUBLICATION ||--o{ DOCUMENT : has
    DOCUMENT ||--|{ CONTENT_NODE : contains
    CONTENT_NODE }|--o{ CONTENT_NODE : is_child_of
    CONTENT_NODE ||--o{ EMBEDDING : has
    CONTENT_NODE ||--o{ FOOTNOTE_REFERENCE : references
    
    %% Entity: PUBLICATION
    PUBLICATION {
        string id PK "Unique publication identifier (pub_XXX)"
        string title "Title of the publication"
        text abstract "Optional publication abstract"
        string citation "Formal citation"
        string authors "Author(s) of the publication (comma separated)"
        date publication_date "Date of publication"
        string source "Explicit source repository"
        string source_url "Publication landing page URL"
        string uri "Persistent handle.net URI that redirects to source_url"
    }

    %% ENTITY: DOCUMENT
    DOCUMENT {
        string id PK "Unique document identifier (dl_XXX)"
        string type "Type of document (MAIN, SUPPLEMENTAL, OTHER)"
        string download_url "URL to the source document download endpoint"
        string description "Description of the document"
        string mime_type "MIME type of the document"
        string charset "Character set of the document"
        string storage_url "URL to the document storage bucket (s3://...)"
        bigint file_size "Size of the document in bytes"
    }

    %% ENTITY: CONTENT_NODE
    CONTENT_NODE {
        string id PK "Unique node identifier (cn_XXX)"
        string document_id FK
        string parent_node_id FK
        string node_type "Type (HEADING, PARAGRAPH, TABLE, IMAGE)"
        text raw_content "Optional original text content of the node"
        text content "Optional cleaned text content of the node"
        string storage_url "Optional URL to the node storage bucket (s3://...)"
        string caption "Optional original caption for the node (image, table, etc.)"
        string description "Optional VLM description of the node (image, table, etc.)"
        int sequence_in_parent "Sequence number in the parent node"
        int sequence_in_document "Sequence number in the document"
        int start_page_pdf "Start page of the node in the PDF"
        int end_page_pdf "End page of the node in the PDF"
        string start_page_logical "Numbered start page of the node"
        string end_page_logical "Numbered end page of the node"
        json bounding_box "Bounding box of the node in the document"
    }
    
    %% ENTITY: EMBEDDING
    EMBEDDING {
        string id PK "Unique embedding identifier (em_XXX)"
        string node_id FK
        vector embedding_vector "Embedding vector"
        string model_name "Name of the embedding model"
        timestamp created_at "Timestamp of when the embedding was created"
    }
    
    %% ENTITY: FOOTNOTE_REFERENCE
    FOOTNOTE_REFERENCE {
        string id PK "Unique footnote reference identifier (fr_XXX)"
        string referencing_node_id FK
        string definition_node_id FK
        string marker_text "Text that marks the footnote reference (usually a number, letter, or symbol)"
        int sequence_in_node "Sequence number in the node"
    }
```