import json
import os
from pathlib import Path
import requests
from tqdm import tqdm
import mimetypes
import time
import random
from typing import Optional
from load.schema import Document


def ensure_directory(path):
    """Create directory if it doesn't exist"""
    Path(path).mkdir(parents=True, exist_ok=True)


def get_extension_from_headers(headers):
    """Extract file extension from Content-Type header"""
    content_type = headers.get("content-type", "").split(";")[0]
    ext = mimetypes.guess_extension(content_type) or ""
    return ext


def download_document_file(doc: Document, base_data_dir: str = "extract/data") -> str:
    """
    Downloads a single file for a given Document object.

    Args:
        doc: The Document SQLModel object.
        base_data_dir: The base directory to save data.

    Returns:
        The full path to the downloaded file.
    """
    # Create a stable, local path
    pub_dir = Path(base_data_dir) / f"pub_{doc.publication_id}"
    pub_dir.mkdir(parents=True, exist_ok=True)

    # Use a generic initial filename; we don't know the extension yet.
    # The .bin is a placeholder extension.
    temp_filename = f"doc_{doc.id}.bin"
    local_filepath = pub_dir / temp_filename

    # Use the existing download logic with retries and progress bar
    session = requests.Session()
    max_retries = 4

    for attempt in range(1, max_retries + 1):
        try:
            # Check if a file with the same base name already exists
            for file in os.listdir(pub_dir):
                if file.startswith(f"doc_{doc.id}"):
                    print(f"  -> File {file} already exists, skipping download")
                    return str(pub_dir / file)

            # Make the request with streaming enabled
            response = session.get(doc.download_url, allow_redirects=True, stream=True)
            response.raise_for_status()

            # Check for rate limiting
            if response.status_code == 429:
                if attempt == max_retries:
                    raise Exception("Maximum retries reached - rate limit persists")

                # Exponential backoff with random component
                base_wait = min(300, 15 * (2 ** (attempt - 1)))  # Cap at 5 minutes
                wait_time = random.uniform(base_wait, base_wait * 1.5)
                print(
                    f"  -> Rate limited. Waiting {wait_time:.1f} seconds before retry (attempt {attempt}/{max_retries})..."
                )
                time.sleep(wait_time)
                continue

            # Get extension from headers
            ext = get_extension_from_headers(response.headers)
            if not ext and "pdf" in response.headers.get("content-type", "").lower():
                ext = ".pdf"  # Force .pdf extension for PDF files

            # Update filename with proper extension if we got one
            if ext:
                final_filename = f"doc_{doc.id}{ext}"
                final_filepath = pub_dir / final_filename
            else:
                final_filepath = local_filepath

            # Get total file size for progress bar
            total_size = int(response.headers.get("content-length", 0))

            # Download with progress bar
            with (
                open(final_filepath, "wb") as f,
                tqdm(
                    desc=f"doc_{doc.id}",
                    total=total_size,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar,
            ):
                for data in response.iter_content(chunk_size=8192):
                    size = f.write(data)
                    pbar.update(size)

            print(f"  -> Downloaded to: {final_filepath}")
            return str(final_filepath)

        except Exception as e:
            if attempt == max_retries:
                raise Exception(
                    f"Failed to download after {max_retries} attempts: {str(e)}"
                )

            # Exponential backoff with random component for other errors
            base_wait = min(120, 10 * (2 ** (attempt - 1)))  # Cap at 2 minutes
            wait_time = random.uniform(base_wait, base_wait * 1.5)
            print(
                f"  -> Error encountered. Waiting {wait_time:.1f} seconds before retry (attempt {attempt}/{max_retries})..."
            )
            time.sleep(wait_time)

    raise Exception("Download failed after all retries")


def download_file(url, output_path, file_id, max_retries=4) -> Optional[str]:
    """Download a file with progress bar using file_id as the base filename"""
    session = requests.Session()

    for attempt in range(1, max_retries + 1):
        try:
            # Check if a file with the same name already exists
            for file in os.listdir(output_path):
                if file.startswith(file_id):
                    print(f"File {file} already exists, skipping download")
                    return None

            # Make the request with streaming enabled
            response = session.get(url, allow_redirects=True, stream=True)
            response.raise_for_status()

            # Check for rate limiting
            if response.status_code == 429:
                if attempt == max_retries:
                    raise Exception("Maximum retries reached - rate limit persists")

                # Exponential backoff with random component
                base_wait = min(300, 15 * (2 ** (attempt - 1)))  # Cap at 5 minutes
                wait_time = random.uniform(base_wait, base_wait * 1.5)
                print(
                    f"Rate limited. Waiting {wait_time:.1f} seconds before retry (attempt {attempt}/{max_retries})..."
                )
                time.sleep(wait_time)
                continue

            # Get extension from headers
            ext = get_extension_from_headers(response.headers)
            if not ext and "pdf" in response.headers.get("content-type", "").lower():
                ext = ".pdf"  # Force .pdf extension for PDF files
            filename = f"{file_id}{ext}"
            full_path = os.path.join(output_path, filename)

            # Get total file size for progress bar
            total_size = int(response.headers.get("content-length", 0))

            # Download with progress bar
            with (
                open(full_path, "wb") as f,
                tqdm(
                    desc=filename,
                    total=total_size,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar,
            ):
                for data in response.iter_content(chunk_size=1024):
                    size = f.write(data)
                    pbar.update(size)

            print(f"Successfully downloaded to {full_path}")
            return full_path

        except Exception as e:
            if attempt == max_retries:
                raise Exception(
                    f"Failed to download after {max_retries} attempts: {str(e)}"
                )

            # Exponential backoff with random component for other errors
            base_wait = min(120, 10 * (2 ** (attempt - 1)))  # Cap at 2 minutes
            wait_time = random.uniform(base_wait, base_wait * 1.5)
            print(
                f"Error encountered. Waiting {wait_time:.1f} seconds before retry (attempt {attempt}/{max_retries})..."
            )
            time.sleep(wait_time)
    return None


def main():
    # Read publication details
    with open("extract/data/publication_details.json", "r") as f:
        publications = json.load(f)

    # Process each publication
    for idx, pub in enumerate(publications):
        pub_id = pub["id"]
        pub_dir = f"extract/data/{pub_id}"
        ensure_directory(pub_dir)

        # Download files marked for download
        for link in pub["downloadLinks"]:
            if link.get("to_download", False):
                try:
                    print(f"\nDownloading {link['text']} for publication {pub_id}")
                    path = download_file(link["url"], pub_dir, link["id"])
                    if path:
                        # Add delay between publications with small random component
                        if idx > 0:
                            base_wait = 20
                            wait_time = random.uniform(base_wait, base_wait * 1.5)
                            print(
                                f"\nWaiting {wait_time:.1f} seconds before next publication..."
                            )
                            time.sleep(wait_time)
                except Exception as e:
                    print(f"Error downloading {link['url']}: {str(e)}")


if __name__ == "__main__":
    main()
