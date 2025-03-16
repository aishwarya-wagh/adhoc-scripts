# Define the input file and output directory
input_file = 'create_tables_qa.sql'
output_dir = 'split_files'  # Directory to store the split files
queries_per_file = 50  # Number of queries per file

# Create the output directory if it doesn't exist
import os
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Read the input file
with open(input_file, 'r') as file:
    queries = file.read().split(';\n')  # Split queries by semicolon and newline

# Remove any empty strings from the list
queries = [query.strip() for query in queries if query.strip()]

# Split queries into chunks of `queries_per_file`
for i in range(0, len(queries), queries_per_file):
    chunk = queries[i:i + queries_per_file]
    file_number = (i // queries_per_file) + 1  # File numbering starts from 1
    output_file = os.path.join(output_dir, f'file{file_number}.sql')
    
    # Write the chunk to the output file
    with open(output_file, 'w') as outfile:
        outfile.write(';\n'.join(chunk) + ';\n')  # Add semicolon and newline after each query

print(f"Split {len(queries)} queries into {file_number} files in '{output_dir}'.")
