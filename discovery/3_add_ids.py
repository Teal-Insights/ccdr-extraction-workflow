#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

import json

# Read the JSON file
with open('discovery/data/publication_details.json', 'r', encoding='utf-8') as f:
    publications = json.load(f)

# Keep track of how many new IDs we add
new_pub_ids_added = 0
new_dl_ids_added = 0

# Add unique IDs to each publication that doesn't have one
next_pub_id = 1
next_dl_id = 1

# First pass: add publication IDs
for pub in publications:
    if 'id' not in pub:
        pub['id'] = f'pub_{next_pub_id:03d}'  # Creates IDs like pub_001, pub_002, etc.
        new_pub_ids_added += 1
    next_pub_id += 1

# Second pass: add download link IDs
for pub in publications:
    if 'downloadLinks' in pub:
        for dl in pub['downloadLinks']:
            if 'id' not in dl:
                dl['id'] = f'dl_{next_dl_id:03d}'  # Creates IDs like dl_001, dl_002, etc.
                new_dl_ids_added += 1
                next_dl_id += 1

# Write the updated JSON back to the file
with open('discovery/data/publication_details.json', 'w', encoding='utf-8') as f:
    json.dump(publications, f, indent=2, ensure_ascii=False)

print(f"Added {new_pub_ids_added} new publication IDs and {new_dl_ids_added} new download link IDs.")
print(f"Total publications: {len(publications)}") 