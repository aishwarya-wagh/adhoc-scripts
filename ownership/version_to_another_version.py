import os
import re
from pathlib import Path
import shutil

# Configuration
WORK_DIR = 'path/to/your/files'  # Directory containing files to rename
VERSION_PREFIX = 'V2025.04.01'   # Version prefix
DRY_RUN = False                  # Set to True to preview changes without renaming

def get_next_available_number():
    """Find the next available sequence number, skipping existing files"""
    max_num = -1
    pattern = re.compile(rf"{re.escape(VERSION_PREFIX)}\.(\d{{4}})__.*\.sql")
    
    for filename in os.listdir(WORK_DIR):
        match = pattern.match(filename)
        if match:
            current_num = int(match.group(1))
            if current_num > max_num:
                max_num = current_num
    return max_num + 1

def rename_files_safely():
    """Handle all rename scenarios including existing files"""
    # Get all bulk_*.sql files sorted alphabetically
    bulk_files = sorted(
        [f for f in os.listdir(WORK_DIR) 
         if f.startswith('bulk_') and f.endswith('.sql')],
        key=lambda x: x.lower()
    )
    
    if not bulk_files:
        print("No bulk_*.sql files found to rename.")
        return
    
    # Get starting number
    start_num = get_next_available_number()
    print(f"Starting version numbering from: {start_num:04d}")
    
    # Process files
    for i, filename in enumerate(bulk_files, start=start_num):
        # Extract the base name
        match = re.match(r'bulk_(.*?)\.sql', filename)
        if not match:
            continue
            
        base_name = match.group(1)
        new_filename = f"{VERSION_PREFIX}.{i:04d}__{base_name}.sql"
        old_path = Path(WORK_DIR) / filename
        new_path = Path(WORK_DIR) / new_filename
        
        # Check for existing file
        if new_path.exists():
            print(f"Conflict: {new_filename} already exists. Finding next available number...")
            # Find next available number by incrementing
            while new_path.exists():
                i += 1
                new_filename = f"{VERSION_PREFIX}.{i:04d}__{base_name}.sql"
                new_path = Path(WORK_DIR) / new_filename
            print(f"Resolved: Will use {new_filename} instead")
        
        # Execute or preview the rename
        if DRY_RUN:
            print(f"[DRY RUN] Would rename: {filename} -> {new_filename}")
        else:
            try:
                # Use copy + delete for safer file operations
                shutil.copy(old_path, new_path)
                os.remove(old_path)
                print(f"Renamed: {filename} -> {new_filename}")
            except Exception as e:
                print(f"Error renaming {filename}: {str(e)}")
                continue

if __name__ == "__main__":
    print("Starting safe file renaming...")
    print(f"Working directory: {WORK_DIR}")
    print(f"Version prefix: {VERSION_PREFIX}")
    
    rename_files_safely()
    
    if DRY_RUN:
        print("\nDry run complete. No files were actually renamed.")
        print("Set DRY_RUN = False to execute the renaming.")
    else:
        print("\nRenaming completed successfully!")
