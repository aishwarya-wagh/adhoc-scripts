import os
import re

# Configuration
SOURCE_DIR = 'path/to/your/bulk_files'  # Directory containing bulk_*.sql files
TARGET_DIR = 'path/to/your/new_project'  # Where to save renamed files
VERSION = 'V2025.04.01'                  # Version prefix
START_NUMBER = 0                         # Starting number for sequence

def rename_bulk_files():
    # Get all bulk_*.sql files
    bulk_files = [f for f in os.listdir(SOURCE_DIR) if f.startswith('bulk_') and f.endswith('.sql')]
    bulk_files.sort()  # Sort alphabetically for consistent numbering
    
    # Create target directory if it doesn't exist
    os.makedirs(TARGET_DIR, exist_ok=True)
    
    # Process each file
    for i, filename in enumerate(bulk_files, start=START_NUMBER):
        # Extract the "somename" part (between bulk_ and .sql)
        match = re.match(r'bulk_(.*?)\.sql', filename)
        if not match:
            continue
            
        somename = match.group(1)
        
        # Format new filename with zero-padded 4-digit number
        new_filename = f"{VERSION}.{i:04d}__{somename}.sql"
        
        # Build full paths
        old_path = os.path.join(SOURCE_DIR, filename)
        new_path = os.path.join(TARGET_DIR, new_filename)
        
        # Copy (or rename) the file
        with open(old_path, 'r') as old_file:
            content = old_file.read()
        with open(new_path, 'w') as new_file:
            new_file.write(content)
        
        print(f"Renamed: {filename} -> {new_filename}")

if __name__ == "__main__":
    print("Starting bulk file renaming...")
    rename_bulk_files()
    print("Renaming complete!")
