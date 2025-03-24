#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

import json

# Read the JSON file
with open('discovery/data/publication_links.json', 'r', encoding='utf-8') as f:
    publications = json.load(f)

# Add unique IDs to each publication
for i, pub in enumerate(publications):
    pub['id'] = f'pub_{i+1:03d}'  # Creates IDs like pub_001, pub_002, etc.

# Write the updated JSON back to the file
with open('discovery/data/publication_links.json', 'w', encoding='utf-8') as f:
    json.dump(publications, f, indent=2, ensure_ascii=False)

print(f"Added IDs to {len(publications)} publications.") 