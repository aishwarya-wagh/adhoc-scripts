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

    # Regex to match column names (including single-quoted names, but excluding already double-quoted names)
    column_pattern = re.compile(r'("[^"]+"|\'[^\']+\'|\b[^,\n]+\b)(?=\s+\w+|,)')

    # Replace column names with double-quoted names
    def quote_match(match):
        column_name = match.group(1).strip()
        # Remove single quotes if present
        if column_name.startswith("'") and column_name.endswith("'"):
            column_name = column_name[1:-1]
        # Skip if already double-quoted
        if column_name.startswith('"') and column_name.endswith('"'):
            return column_name
        return f'"{column_name}"'

    # Apply the replacement only to column names in the column section
    quoted_column_section = column_pattern.sub(quote_match, column_section)

    # Replace the original column section with the quoted version
    quoted_sql = sql_statement.replace(column_section, quoted_column_section)
    return quoted_sql

def process_sql_file(input_file, output_file):
    """
    Process an SQL file containing CREATE TABLE statements and quote column names.
    """
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        sql_content = infile.read()  # Read the entire SQL file

        # Split the SQL content into individual statements
        sql_statements = re.split(r';\s*\n', sql_content)

        for statement in sql_statements:
            # Check if the statement contains a CREATE TABLE statement
            if "CREATE TABLE IF NOT EXISTS" in statement.upper():
                # Quote column names in the CREATE TABLE statement
                modified_statement = quote_column_names(statement)
                outfile.write(modified_statement + ";\n")
            else:
                # Write the statement as-is if it's not a CREATE TABLE statement
                outfile.write(statement + ";\n")

# Input and output file paths
input_file = "input.sql"  # Replace with your input SQL file path
output_file = "output.sql"  # Replace with your output SQL file path

# Process the SQL file
process_sql_file(input_file, output_file)

print(f"Processed SQL file saved to {output_file}")
