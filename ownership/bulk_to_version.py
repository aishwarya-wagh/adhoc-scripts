import os
import re
from pathlib import Path
import shutil

# Configuration - set both to the same directory
SOURCE_DIR = 'path/to/your/files'
TARGET_DIR = SOURCE_DIR  # Same as source
VERSION = 'V2025.04.01'  # Version prefix

def get_highest_existing_number():
    """Find the highest existing sequence number, ignoring bulk_*.sql files"""
    max_num = -1
    pattern = re.compile(rf"{re.escape(VERSION)}\.(\d{{4}})__.*\.sql")
    
    for filename in os.listdir(TARGET_DIR):
        # Skip the bulk_*.sql files when looking for existing numbers
        if filename.startswith('bulk_') and filename.endswith('.sql'):
            continue
        match = pattern.match(filename)
        if match:
            current_num = int(match.group(1))
            if current_num > max_num:
                max_num = current_num
    return max_num + 1  # Return next available number

def rename_in_place():
    """Rename files in the same directory with proper sequencing"""
    start_number = get_highest_existing_number()
    print(f"Starting numbering from: {start_number:04d}")
    
    # Get all bulk_*.sql files and sort them
    bulk_files = sorted(
        [f for f in os.listdir(SOURCE_DIR) 
         if f.startswith('bulk_') and f.endswith('.sql')],
        key=lambda x: x.lower()
    )
    
    # First collect all rename operations to perform
    rename_operations = []
    for i, filename in enumerate(bulk_files, start=start_number):
        match = re.match(r'bulk_(.*?)\.sql', filename)
        if not match:
            continue
            
        somename = match.group(1)
        new_filename = f"{VERSION}.{i:04d}__{somename}.sql"
        rename_operations.append((filename, new_filename))
    
    # Then execute the renames (safer than renaming while iterating)
    for old_name, new_name in rename_operations:
        old_path = Path(SOURCE_DIR) / old_name
        new_path = Path(TARGET_DIR) / new_name
        
        # Use copy + delete instead of rename to be extra safe
        shutil.copy(old_path, new_path)
        os.remove(old_path)
        print(f"Renamed: {old_name} -> {new_name}")

if __name__ == "__main__":
    print("Starting in-place file renaming...")
    
    # Safety check
    if SOURCE_DIR != TARGET_DIR:
        print("Warning: This script is optimized for same source/target directories")
    
    rename_in_place()
    print(f"Process completed. Renamed {len(os.listdir(SOURCE_DIR))} files.")
