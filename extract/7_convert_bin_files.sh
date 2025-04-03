#!/bin/bash

# Files detected as gzip actually turn out to be PDFs!
# Navigate to the project root directory if needed
# cd /path/to/project/root

# Find all .bin files recursively in extract/data and rename to .pdf if they are PDFs
find extract/data -type f -name "*.bin" | while read file; do
    # Check if it's a PDF
    if file "$file" | grep -q "PDF document"; then
        echo "Renaming PDF: $file"
        new_name="${file%.bin}.pdf"
        mv "$file" "$new_name"
    else
        echo "Not a PDF: $file"
    fi
done
