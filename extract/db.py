import os
from sqlmodel import SQLModel, create_engine


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