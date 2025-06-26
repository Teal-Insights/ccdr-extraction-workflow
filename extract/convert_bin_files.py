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
            ["file", str(file_path)], capture_output=True, text=True, check=True
        )
        return "PDF document" in result.stdout
    except subprocess.CalledProcessError:
        return False


def analyze_and_prepare_file(filepath_str: str) -> tuple[str, int]:
    """
    Analyzes a file, converts it if necessary, measures its size,
    and returns the final path and size.

    Args:
        filepath_str: The path to the downloaded file.

    Returns:
        A tuple of (final_file_path, file_size_in_bytes).
    """
    file_path = Path(filepath_str)

    # Check if it's a PDF using the 'file' command
    try:
        result = subprocess.run(
            ["file", str(file_path)], capture_output=True, text=True, check=True
        )
        is_pdf = "PDF document" in result.stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        is_pdf = False  # 'file' command might not be available or fails
        print("  -> Warning: Could not run 'file' command to verify file type.")

    # If it's a PDF and has a .bin extension, rename it
    if is_pdf and file_path.suffix == ".bin":
        new_path = file_path.with_suffix(".pdf")
        print(f"  -> Converting '{file_path.name}' to '{new_path.name}'")
        file_path.rename(new_path)
        final_path = new_path
    else:
        # If it's not a PDF or already has the right extension, keep it as is
        final_path = file_path

    # Measure the size of the final file
    file_size = os.path.getsize(final_path)
    print(f"  -> Final file is '{final_path.name}' with size {file_size} bytes.")

    return str(final_path), file_size


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
