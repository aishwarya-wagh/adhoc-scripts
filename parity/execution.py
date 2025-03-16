import os
import snowflake.connector
from snowflake.connector import ProgrammingError

# Snowflake connection details
SNOWFLAKE_ACCOUNT = 'your_account'
SNOWFLAKE_USER = 'your_username'
SNOWFLAKE_PASSWORD = 'your_password'
SNOWFLAKE_WAREHOUSE = 'your_warehouse'
SNOWFLAKE_DATABASE = 'your_database'  # Default database (can be overridden)
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'  # Specify the role here

# Directory containing split SQL files
input_dir = 'split_files'

# File to store failed queries
failed_queries_file = 'failed_queries.sql'

# Initialize Snowflake connection
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,  # Default database
    role=SNOWFLAKE_ROLE  # Use the specified role
)
cursor = conn.cursor()

# Open the failed queries file for writing
with open(failed_queries_file, 'w') as failed_file:
    # Iterate over all split files in the directory
    for filename in sorted(os.listdir(input_dir)):
        if filename.endswith('.sql'):
            file_path = os.path.join(input_dir, filename)
            print(f"Executing queries from {filename}...")

            # Set the database dynamically (e.g., 'abcd')
            target_database = 'abcd'  # Replace with the desired database name
            cursor.execute(f"USE DATABASE {target_database};")
            print(f"Set database to {target_database}.")

            # Read the file and split into individual queries
            with open(file_path, 'r') as sql_file:
                queries = sql_file.read().split(';\n')

            # Execute each query
            for query in queries:
                query = query.strip()
                if not query:
                    continue  # Skip empty queries

                try:
                    # Extract schema from the query (if applicable)
                    # Example: If the query is "USE SCHEMA schema_name;"
                    if query.upper().startswith('USE SCHEMA'):
                        schema_name = query.split()[-1].strip(';')
                        cursor.execute(f"USE SCHEMA {schema_name};")
                        print(f"Set schema to {schema_name}.")
                        continue  # Skip executing the USE SCHEMA query again

                    # Execute the query
                    cursor.execute(query)
                    print(f"Success: {query}")
                except ProgrammingError as e:
                    # Log the failed query to the failed_queries file
                    failed_file.write(query + ';\n')
                    print(f"Failed: {query}\nError: {e}")
                except Exception as e:
                    # Handle other exceptions (e.g., network errors)
                    failed_file.write(query + ';\n')
                    print(f"Failed: {query}\nError: {e}")

# Close the Snowflake connection
cursor.close()
conn.close()

print(f"Execution complete. Failed queries are logged in '{failed_queries_file}'.")
