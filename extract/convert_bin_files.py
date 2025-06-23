#!/usr/bin/env python3
"""
Convert .bin files to .pdf if they are actually PDF documents.

Files detected as gzip actually turn out to be PDFs!
This script finds all .bin files recursively in extract/data and 
renames them to .pdf if they are PDF documents.
"""

import os
import subprocess
from pathlib import Path


def is_pdf_file(file_path: Path) -> bool:
    """Check if a file is a PDF document using the file command."""
    try:
        result = subprocess.run(
            ["file", str(file_path)], 
            capture_output=True, 
            text=True, 
            check=True
        )
        return "PDF document" in result.stdout
    except subprocess.CalledProcessError:
        return False


def main():
    """Find and convert .bin files to .pdf if they are PDF documents."""
    data_dir = Path("extract/data")
    
    if not data_dir.exists():
        print(f"Directory {data_dir} does not exist")
        return
    
    # Find all .bin files recursively
    bin_files = list(data_dir.rglob("*.bin"))
    
    if not bin_files:
        print("No .bin files found")
        return
    
    print(f"Found {len(bin_files)} .bin files")
    
    for bin_file in bin_files:
        if is_pdf_file(bin_file):
            # Create new filename with .pdf extension
            new_name = bin_file.with_suffix(".pdf")
            print(f"Renaming PDF: {bin_file} -> {new_name}")
            bin_file.rename(new_name)
        else:
            print(f"Not a PDF: {bin_file}")


if __name__ == "__main__":
    main() 