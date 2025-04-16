import csv
import os

# Configuration
CSV_FILE_PATH = 'grant_ownership_statements.csv'  # Path to your CSV file
OUTPUT_DIR = 'ownership_transfer_scripts'         # Output directory
BATCH_SIZE = 100                                  # Number of statements per file
FILE_PREFIX = 'V2025.04.16.0000.'                 # Fixed prefix for filenames

def extract_table_name(grant_statement):
    """Extract table name from grant statement"""
    try:
        # Find the pattern after "ON TABLE" or "ON SEQUENCE"
        if "ON TABLE" in grant_statement:
            parts = grant_statement.split("ON TABLE")[1].split("TO ROLE")[0].strip()
        elif "ON SEQUENCE" in grant_statement:
            parts = grant_statement.split("ON SEQUENCE")[1].split("TO ROLE")[0].strip()
        else:
            return "unknown"
        
        # Remove schema if present and get just the table name
        if '.' in parts:
            return parts.split('.')[1].strip('"\'')
        return parts.strip('"\'')
    except:
        return "unknown"

def create_output_directory():
    """Create output directory if it doesn't exist"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def process_csv():
    """Process CSV file and generate SQL files"""
    create_output_directory()
    
    with open(CSV_FILE_PATH, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        file_counter = 0
        statement_counter = 0
        current_file = None
        
        for row in csv_reader:
            if not row:  # Skip empty rows
                continue
                
            grant_statement = row[0].strip()
            if not grant_statement:  # Skip empty statements
                continue
                
            # Start a new file when needed
            if statement_counter % BATCH_SIZE == 0:
                if current_file is not None:
                    current_file.close()
                
                # Format the file number with leading zeros (0000, 0001, etc.)
                file_number = f"{file_counter:04d}"
                table_name = extract_table_name(grant_statement)
                filename = f"{FILE_PREFIX}{file_number}___{table_name}_ownership_transfer.sql"
                filepath = os.path.join(OUTPUT_DIR, filename)
                
                current_file = open(filepath, 'w')
                current_file.write("-- Ownership transfer batch script\n")
                current_file.write("-- Generated from CSV source\n\n")
                file_counter += 1
                
            # Write the statement to the current file
            current_file.write(grant_statement + ";\n")
            statement_counter += 1
        
        # Close the last file
        if current_file is not None:
            current_file.close()
            
    print(f"Processed {statement_counter} statements into {file_counter} files in '{OUTPUT_DIR}'")

if __name__ == "__main__":
    process_csv()
