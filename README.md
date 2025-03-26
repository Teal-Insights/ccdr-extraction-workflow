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
        C["Cursor Agent<br>(Claude 3.7 Sonnet)"] -- finds --> B["Data Sources<br>(Web/APIs)"]
        C -- writes --> D["Extraction Scripts<br>(Node.js)"]
        D -- scrapes --> B
        B --> D
        D --> F["Raw Data"]
    end

    subgraph "Data Processing"
        F --> I["Chunking"]
        I --> J["Annotation"]
        J --> K["Embedding<br>(Google AI)"]
        K --> L["Database Upsert<br>(Drizzle ORM)"]
    end

    subgraph "Database & Vector Storage"
        L --> M["PostgreSQL with pgvector"]
        M -- Stores --> N["Documents bucket"]
        M -- Stores --> O["Chunks table"]
        M -- Stores --> P["Sections table"]
        M -- Stores --> Q["Footnotes table"]
        M -- Stores --> R["Media bucket"]
    end
```

## Database Schema

```mermaid
erDiagram
    %% Relationship lines
    DOCUMENT ||--|{ CONTENT_NODE : contains
    DOCUMENT ||--o{ SUPPLEMENTARY_FILE : has_supplementary
    SUPPLEMENTARY_FILE ||--|{ CONTENT_NODE : contains
    CONTENT_NODE }|--o{ CONTENT_NODE : is_child_of
    CONTENT_NODE ||--o{ EMBEDDING : has
    CONTENT_NODE ||--o{ FOOTNOTE_REFERENCE : references
    
    %% ENTITY: DOCUMENT
    DOCUMENT {
        string document_id PK
        string uri "Unique handle.net URI"
        string title
        text abstract "Optional publication abstract"
        string citation "Formal citation"
        string author
        date publication_date
        string source_file_path
        string source "Explicit source repository"
        string source_url "URL to source page"
        json metadata
        json file_info "Main download file info"
    }
    
    %% ENTITY: CONTENT_NODE
    %% node_type can be SECTION_HEADING, PARAGRAPH, TABLE, IMAGE, etc.
    CONTENT_NODE {
        string node_id PK
        string document_id FK
        string parent_node_id FK
        string node_type
        text content
        json raw_content
        int sequence_in_parent
        int sequence_in_document
        int start_page_pdf
        int end_page_pdf
        string start_page_logical
        string end_page_logical
        json bounding_box
        string supplementary_file_id FK
    }
    
    %% ENTITY: SUPPLEMENTARY_FILE
    SUPPLEMENTARY_FILE {
        string file_id PK "Mapped from downloadLinks entry (dl_XXX pattern)"
        string document_id FK
        string file_path
        string url "Download URL from supplementary downloadLink"
        json file_info "Supplementary file info"
        string file_type "MIME type from file_info"
        text description
        boolean processed
    }
    
    %% ENTITY: EMBEDDING
    EMBEDDING {
        string embedding_id PK
        string node_id FK
        vector embedding_vector
        string model_name
        timestamp created_at
    }
    
    %% ENTITY: FOOTNOTE_REFERENCE
    FOOTNOTE_REFERENCE {
        string footnote_ref_id PK
        string referencing_node_id FK
        string definition_node_id FK
        string marker_text
        int sequence_in_node
    }
```