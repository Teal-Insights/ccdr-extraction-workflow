import json
import glob

def combine_document_regions():
    # Get all document_regions.json files in dl_* directories
    json_files = sorted(glob.glob('transform/images/dl_*/document_regions.json'))
    
    # Combined array to store all JSON data
    combined_data = []
    
    # Read each file and extend the combined array
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    combined_data.extend(data)
                else:
                    combined_data.append(data)
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
    
    # Write the combined data to a new file
    output_path = 'transform/images/document_regions.json'
    with open(output_path, 'w') as f:
        json.dump(combined_data, f, indent=2)
    
    print(f"Combined {len(json_files)} files into {output_path}")
    print(f"Total number of items: {len(combined_data)}")

if __name__ == "__main__":
    combine_document_regions() 