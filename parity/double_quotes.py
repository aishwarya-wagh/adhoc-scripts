import re

def quote_column_names(sql_statement):
    """
    Convert all column names in a CREATE TABLE statement to double-quoted names.
    """
    # Extract the text between parentheses (column definitions)
    column_section_match = re.search(r'\(([\s\S]*?)\)', sql_statement)
    if not column_section_match:
        return sql_statement  # No column definitions found

    column_section = column_section_match.group(1)

    # Regex to match column names (including single-quoted names)
    column_pattern = re.compile(r"('[^']+'|\b[^,\n]+\b)(?=\s+\w+|,)")

    # Replace column names with double-quoted names
    def quote_match(match):
        column_name = match.group(1).strip()
        # Remove single quotes if present
        if column_name.startswith("'") and column_name.endswith("'"):
            column_name = column_name[1:-1]
        return f'"{column_name}"'

    # Apply the replacement only to column names in the column section
    quoted_column_section = column_pattern.sub(quote_match, column_section)

    # Replace the original column section with the quoted version
    quoted_sql = sql_statement.replace(column_section, quoted_column_section)
    return quoted_sql

def process_csv(input_file, output_file):
    """
    Process a CSV file containing CREATE TABLE statements and quote column names.
    """
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            # Check if the line contains a CREATE TABLE statement
            if "CREATE TABLE IF NOT EXISTS" in line.upper():
                # Quote column names in the CREATE TABLE statement
                modified_line = quote_column_names(line)
                outfile.write(modified_line)
            else:
                # Write the line as-is if it's not a CREATE TABLE statement
                outfile.write(line)

# Input and output file paths
input_csv = "input_tables.csv"  # Replace with your input CSV file path
output_csv = "output_tables.csv"  # Replace with your output CSV file path

# Process the CSV file
process_csv(input_csv, output_csv)

print(f"Processed CSV saved to {output_csv}")
