# Database Schema

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
        string publication_id PK "Unique publication identifier (pub_XXX)"
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
        string document_id PK "Unique document identifier (dl_XXX)"
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
        string node_id PK "Unique node identifier (cn_XXX)"
        string document_id FK
        string parent_node_id FK
        string node_type "Type (SECTION_HEADING, PARAGRAPH, TABLE, IMAGE)"
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
        string embedding_id PK "Unique embedding identifier (em_XXX)"
        string node_id FK
        vector embedding_vector "Embedding vector"
        string model_name "Name of the embedding model"
        timestamp created_at "Timestamp of when the embedding was created"
    }

    %% ENTITY: FOOTNOTE_REFERENCE
    FOOTNOTE_REFERENCE {
        string footnote_ref_id PK "Unique footnote reference identifier (fr_XXX)"
        string referencing_node_id FK
        string definition_node_id FK
        string marker_text "Text that marks the footnote reference (usually a number, letter, or symbol)"
        int sequence_in_node "Sequence number in the node"
    }
```

To store PDF and image files, we will use AWS's "S3" storage service, which costs about $0.023 per GB per month, with no flat fee.

## JSON to Database Schema Mapping

### PUBLICATION Table
| JSON Path                     | DB Field            | Notes                              
|-------------------------------|---------------------|------------------------------------|
| `id`                          | `publication_id`    | Direct mapping                     |
| `title`                       | `title`             | Direct mapping                     |
| `abstract`                    | `abstract`          | Direct mapping                     |
| `citation`                    | `citation`          | Direct mapping                     |
| `metadata.authors`            | `authors`           | Direct mapping                     |
| `metadata.date`               | `publication_date`  | Direct mapping                     |
| `source`                      | `source`            | Direct mapping                     |
| `source_url`                  | `source_url`        | Direct mapping                     |
| `uri`                         | `uri`               | Direct mapping                     |

### DOCUMENT Table
| JSON Path                     | DB Field            | Notes                              |
|-------------------------------|---------------------|------------------------------------|
| `downloadLinks[*].id`         | `document_id`       | Direct mapping                     |
| `downloadLinks[*].url`        | `download_url`      | Direct mapping                     |
| `downloadLinks[*].file_info.mime_type` | `mime_type`| Direct mapping                     |
| `downloadLinks[*].file_info.charset`   | `charset`  | Direct mapping                     |
| `downloadLinks[*].type`       | `type`              | Direct mapping                     |
| `downloadLinks[*].text`       | `description`       | Direct mapping                     |
| -                             | `storage_url`       | To be populated during processing  |
| -                             | `file_size`         | To be populated during processing  |
