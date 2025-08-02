import os
from sqlmodel import create_engine
import requests
import difflib
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(os.getenv("ENVIRONMENT", ".env"))


# Database connection setup
def get_database_url():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "ccdr-explorer-db")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


# Create database engine
engine = create_engine(get_database_url())


def check_schema_sync():
    """Check if local schema is in sync with master."""
    print(f"\n{'=' * 60}")
    print("CHECKING SCHEMA SYNCHRONIZATION")
    print(f"{'=' * 60}")

    try:
        # Fetch master schema from GitHub
        master_url = "https://raw.githubusercontent.com/Teal-Insights/ccdr-explorer-api/refs/heads/main/db/schema.py"
        print(f"Fetching master schema from: {master_url}")

        response = requests.get(master_url, timeout=10)
        response.raise_for_status()
        master_schema = response.text

        # Read local schema
        local_schema_path = Path("load/schema.py")
        if not local_schema_path.exists():
            raise FileNotFoundError(f"Local schema file not found: {local_schema_path}")

        with open(local_schema_path, "r", encoding="utf-8") as f:
            local_schema = f.read()

        # Compare schemas
        if master_schema.strip() == local_schema.strip():
            print("✅ Schema is in sync with master")
            return True
        else:
            print("❌ Schema differs from master")
            print("\nDifferences found:")
            print("-" * 40)

            # Generate diff
            master_lines = master_schema.splitlines(keepends=True)
            local_lines = local_schema.splitlines(keepends=True)

            diff = difflib.unified_diff(
                master_lines,
                local_lines,
                fromfile="master/db/schema.py",
                tofile="local/extract/schema.py",
                lineterm="",
            )

            # Print first 50 lines of diff to avoid overwhelming output
            diff_lines = list(diff)
            for i, line in enumerate(diff_lines[:50]):
                print(line.rstrip())
                if i == 49 and len(diff_lines) > 50:
                    print(f"... ({len(diff_lines) - 50} more lines)")
                    break

            print("-" * 40)
            print("\nTo sync your schema, run:")
            print(
                "curl -s https://raw.githubusercontent.com/Teal-Insights/ccdr-explorer-api/refs/heads/main/db/schema.py > load/schema.py"
            )
            return False

    except requests.RequestException as e:
        print(f"❌ Failed to fetch master schema: {str(e)}")
        print("Continuing with workflow (network issue or repository unavailable)")
        return True  # Don't block workflow for network issues

    except Exception as e:
        print(f"❌ Schema sync check failed: {str(e)}")
        return False


if __name__ == "__main__":
    check_schema_sync()