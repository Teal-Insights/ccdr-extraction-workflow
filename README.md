
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

    style M fill:#f0c0f0,stroke:#333,stroke-width:2px
```
